{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.Mock.Forging.Tx.Shelley
  ( mkPaymentTx
  , mkDCertTxPools
  , mkSimpleTx
  , mkDCertTx
  , mkSimpleDCertTx
  , consPaymentTxBody
  , consCertTxBody
  , consTxBody
  ) where


import           Cardano.Prelude

import qualified Data.Maybe.Strict as Strict
import           Data.Sequence.Strict (StrictSeq)
import qualified Data.Sequence.Strict as StrictSeq
import qualified Data.Set as Set

import           Cardano.Ledger.Address
import           Cardano.Ledger.BaseTypes
import           Cardano.Ledger.Coin
import           Cardano.Ledger.Credential
import           Cardano.Ledger.Era (Crypto)
import           Cardano.Ledger.Shelley.Tx
import           Cardano.Ledger.Shelley.TxBody

import           Ouroboros.Consensus.Cardano.Block (LedgerState)
import           Ouroboros.Consensus.Shelley.Eras (ShelleyEra, StandardCrypto)
import           Ouroboros.Consensus.Shelley.Ledger (ShelleyBlock)

import           Cardano.Slotting.Slot (SlotNo (..))

import           Cardano.Mock.Forging.Tx.Generic
import           Cardano.Mock.Forging.Types

type ShelleyUTxOIndex = UTxOIndex (ShelleyEra StandardCrypto)
type ShelleyLedgerState = LedgerState (ShelleyBlock TPraosStandard (ShelleyEra StandardCrypto))
type ShelleyTx = Tx (ShelleyEra StandardCrypto)

instance HasField "address" (TxOut (ShelleyEra StandardCrypto)) (Addr StandardCrypto) where
    getField (TxOut addr _) = addr

mkPaymentTx
    :: ShelleyUTxOIndex -> ShelleyUTxOIndex -> Integer -> Integer -> ShelleyLedgerState
    -> Either ForgingError ShelleyTx
mkPaymentTx inputIndex outputIndex amount fees st = do
    (inputPair, _) <- resolveUTxOIndex inputIndex st
    (outputPair, _ ) <- resolveUTxOIndex outputIndex st
    let input = Set.singleton $ fst inputPair
        TxOut addr _ = snd outputPair
        output = TxOut addr (Coin amount)
        TxOut addr' (Coin inputValue) = snd inputPair
        change = TxOut addr' $ Coin (inputValue - amount - fees)

    Right $ mkSimpleTx $ consPaymentTxBody input (StrictSeq.fromList [output, change]) (Coin fees)

mkDCertTxPools :: ShelleyLedgerState -> Either ForgingError ShelleyTx
mkDCertTxPools sta = Right $ mkSimpleTx $ consCertTxBody (allPoolStakeCert sta) (Wdrl mempty)

mkSimpleTx :: TxBody (ShelleyEra StandardCrypto) -> ShelleyTx
mkSimpleTx txBody = Tx
    txBody
    mempty
    (maybeToStrictMaybe Nothing)

mkDCertTx :: [DCert StandardCrypto] -> Wdrl StandardCrypto -> Either ForgingError ShelleyTx
mkDCertTx certs wdrl = Right $ mkSimpleTx $ consCertTxBody certs wdrl

mkSimpleDCertTx
    :: [(StakeIndex, StakeCredential StandardCrypto -> DCert StandardCrypto)] -> ShelleyLedgerState
    -> Either ForgingError ShelleyTx
mkSimpleDCertTx consDert st = do
    dcerts <- forM consDert $ \(stakeIndex, mkDCert) -> do
      cred <- resolveStakeCreds stakeIndex st
      pure $ mkDCert cred
    mkDCertTx dcerts (Wdrl mempty)

consPaymentTxBody
    :: Set (TxIn (Crypto (ShelleyEra StandardCrypto))) -> StrictSeq (TxOut (ShelleyEra StandardCrypto))
    -> Coin -> TxBody (ShelleyEra StandardCrypto)
consPaymentTxBody ins outs fees = consTxBody ins outs fees mempty (Wdrl mempty)

consCertTxBody :: [DCert StandardCrypto] -> Wdrl StandardCrypto -> TxBody (ShelleyEra StandardCrypto)
consCertTxBody = consTxBody mempty mempty (Coin 0)

consTxBody
    :: Set (TxIn (Crypto (ShelleyEra StandardCrypto))) -> StrictSeq (TxOut (ShelleyEra StandardCrypto))
    -> Coin -> [DCert StandardCrypto] -> Wdrl StandardCrypto -> TxBody (ShelleyEra StandardCrypto)
consTxBody ins outs fees certs wdrl =
    TxBody
      ins
      outs
      (StrictSeq.fromList certs)
      wdrl
      fees
      (SlotNo 1000000000) -- TODO ttl
      Strict.SNothing
      Strict.SNothing
