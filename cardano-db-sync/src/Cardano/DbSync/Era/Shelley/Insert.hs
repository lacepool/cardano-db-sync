{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Cardano.DbSync.Era.Shelley.Insert
  ( insertShelleyBlock

  -- These are exported for data in Shelley Genesis
  , insertPoolRegister
  , insertStakeRegistration
  , insertDelegation
  , insertStakeAddressRefIfMissing
  ) where

import           Cardano.Prelude

import           Cardano.Api (SerialiseAsCBOR (..))
import           Cardano.Api.Shelley (TxMetadataValue (..), makeTransactionMetadata,
                   metadataValueToJsonNoSchema)

import           Cardano.BM.Trace (Trace, logDebug, logInfo, logWarning)

import           Cardano.Crypto.Hash (hashToBytes)
import qualified Cardano.Crypto.Hashing as Crypto

import           Cardano.Db (DbLovelace (..), DbWord64 (..))
import qualified Cardano.Db as DB

import qualified Cardano.DbSync.Era.Shelley.Generic as Generic
import           Cardano.DbSync.Era.Shelley.Generic.ParamProposal
import           Cardano.DbSync.Era.Shelley.Insert.Epoch
import           Cardano.DbSync.Era.Shelley.Insert.Grouped
import           Cardano.DbSync.Era.Shelley.Offline
import           Cardano.DbSync.Era.Shelley.Query
import           Cardano.DbSync.Era.Util (liftLookupFail, safeDecodeToJson)

import qualified Cardano.Ledger.Address as Ledger
import           Cardano.Ledger.Alonzo.Language (Language)
import qualified Cardano.Ledger.Alonzo.Scripts as Ledger
import qualified Cardano.Ledger.BaseTypes as Ledger
import           Cardano.Ledger.Coin (Coin (..))
import qualified Cardano.Ledger.Coin as Ledger
import qualified Cardano.Ledger.Credential as Ledger
import           Cardano.Ledger.Keys
import qualified Cardano.Ledger.Keys as Ledger
import           Cardano.Ledger.Mary.Value (AssetName (..), PolicyID (..), Value (..))
import qualified Cardano.Ledger.Shelley.API.Wallet as Shelley
import qualified Cardano.Ledger.Shelley.TxBody as Shelley

import           Cardano.DbSync.Api
import           Cardano.DbSync.Cache
import           Cardano.DbSync.Error
import           Cardano.DbSync.Types
import           Cardano.DbSync.Util

import           Control.Monad.Trans.Control (MonadBaseControl)

import           Cardano.Slotting.Block (BlockNo (..))
import           Cardano.Slotting.Slot (EpochNo (..), EpochSize (..), SlotNo (..))

import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy.Char8 as LBS
import           Data.Either.Extra (eitherToMaybe)
import           Data.Group (invert)
import qualified Data.Map.Strict as Map
import           Data.Maybe.Strict (strictMaybeToMaybe)
import qualified Data.Strict.Maybe as Strict
import qualified Data.Text.Encoding as Text

import           Database.Persist.Sql (SqlBackend)

import           Ouroboros.Consensus.Cardano.Block (StandardCrypto)

{- HLINT ignore "Reduce duplication" -}

type IsPoolMember = PoolKeyHash -> Bool

insertShelleyBlock
    :: (MonadBaseControl IO m, MonadIO m)
    => SyncEnv -> Bool -> Generic.Block -> SlotDetails
    -> IsPoolMember -> Strict.Maybe Generic.NewEpoch -> Generic.StakeSliceRes
    -> ReaderT SqlBackend m (Either SyncNodeError ())
insertShelleyBlock env shouldLog blk details isMember mNewEpoch stakeSlice = do
  runExceptT $ do
    mPhid <- lift $ queryPoolKeyWithCache cache CacheNew $ coerceKeyRole $ Generic.blkSlotLeader blk
    slid <- lift . DB.insertSlotLeader $ Generic.mkSlotLeader (Generic.unKeyHashRaw $ Generic.blkSlotLeader blk) (eitherToMaybe mPhid)
    void . lift . DB.insertBlock $
                  DB.Block
                    { DB.blockHash = Generic.blkHash blk
                    , DB.blockEpochNo = Just $ unEpochNo (sdEpochNo details)
                    , DB.blockSlotNo = Just $ unSlotNo (Generic.blkSlotNo blk)
                    , DB.blockEpochSlotNo = Just $ unEpochSlot (sdEpochSlot details)
                    , DB.blockBlockNo = fromIntegral $ unBlockNo (Generic.blkBlockNo blk)
                    , DB.blockSlotLeaderId = slid
                    , DB.blockSize = Generic.blkSize blk
                    , DB.blockTime = sdSlotTime details
                    , DB.blockTxCount = fromIntegral $ length (Generic.blkTxs blk)
                    , DB.blockProtoMajor = fromIntegral $ Ledger.pvMajor (Generic.blkProto blk)
                    , DB.blockProtoMinor = fromIntegral $ Ledger.pvMinor (Generic.blkProto blk)

                    -- Shelley specific
                    , DB.blockVrfKey = Just $ Generic.blkVrfKey blk
                    , DB.blockOpCert = Just $ Generic.blkOpCert blk
                    , DB.blockOpCertCounter = Just $ Generic.blkOpCertCounter blk
                    }

    let zippedTx = zip [0 .. ] (Generic.blkTxs blk)
    let txInserter = insertTx tracer cache (leNetwork lenv) isMember (Generic.blkBlockNo blk) (sdEpochNo details) (Generic.blkSlotNo blk)
    grouped <- foldM (\grouped (idx, tx) -> txInserter idx tx grouped) mempty zippedTx
    insertBlockGroupedData tracer grouped

    liftIO $ do
      let epoch = unEpochNo (sdEpochNo details)
          slotWithinEpoch = unEpochSlot (sdEpochSlot details)
          followingClosely = getSyncStatus details == SyncFollowing

      when (followingClosely && slotWithinEpoch /= 0 && unBlockNo (Generic.blkBlockNo blk) `mod` 20 == 0) $ do
        logInfo tracer $
          mconcat
            [ renderInsertName (Generic.blkEra blk), ": continuing epoch ", textShow epoch
            , " (slot ", textShow slotWithinEpoch , "/"
            , textShow (unEpochSize $ sdEpochSize details), ")"
            ]
      logger followingClosely tracer $ mconcat
        [ renderInsertName (Generic.blkEra blk), ": epoch "
        , textShow (unEpochNo $ sdEpochNo details)
        , ", slot ", textShow (unSlotNo $ Generic.blkSlotNo blk)
        , ", block ", textShow (unBlockNo $ Generic.blkBlockNo blk)
        , ", hash ", renderByteArray (Generic.blkHash blk)
        ]

    whenStrictJust mNewEpoch $ \ newEpoch ->
      insertOnNewEpoch tracer (Generic.blkBlockNo blk) (Generic.blkSlotNo blk) (sdEpochNo details) newEpoch

    insertStakeSlice env stakeSlice

    when (unBlockNo (Generic.blkBlockNo blk) `mod` offlineModBase == 0) .
      lift $ do
        insertOfflineResults tracer (envOfflineResultQueue env)
        loadOfflineWorkQueue tracer (envOfflineWorkQueue env)

    when (getSyncStatus details == SyncFollowing) $
      -- Serializiing things during syncing can drastically slow down full sync
      -- times (ie 10x or more).
      lift DB.transactionCommit
  where
    logger :: Bool -> Trace IO a -> a -> IO ()
    logger followingClosely
      | shouldLog = logInfo
      | followingClosely = logInfo
      | unBlockNo (Generic.blkBlockNo blk) `mod` 5000 == 0 = logInfo
      | otherwise = logDebug

    renderInsertName :: Generic.BlockEra -> Text
    renderInsertName eraName =
      mconcat ["Insert ",  textShow eraName, " Block"]

    offlineModBase :: Word64
    offlineModBase =
      case getSyncStatus details of
        SyncFollowing -> 10
        SyncLagging -> 2000

    lenv :: LedgerEnv
    lenv = envLedger env

    tracer :: Trace IO Text
    tracer = getTrace env

    cache :: Cache
    cache = envCache env

-- -----------------------------------------------------------------------------

insertOnNewEpoch
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> BlockNo -> SlotNo -> EpochNo -> Generic.NewEpoch
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertOnNewEpoch tracer blkNo slotNo epochNo newEpoch = do
    whenStrictJust (Generic.euProtoParams epochUpdate) $ \ params ->
      insertEpochParam tracer blkNo epochNo params (Generic.euNonce epochUpdate)
    whenStrictJust (Generic.neAdaPots newEpoch) $ \pots ->
      insertPots blkNo slotNo epochNo pots
  where
    epochUpdate :: Generic.EpochUpdate
    epochUpdate = Generic.neEpochUpdate newEpoch

-- -----------------------------------------------------------------------------

insertTx
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> Ledger.Network -> IsPoolMember -> BlockNo -> EpochNo
    -> SlotNo -> Word64 -> Generic.Tx -> BlockGroupedData
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) BlockGroupedData
insertTx tracer cache network isMember blkNo epochNo slotNo blockIndex tx grouped = do
    let !outSum = fromIntegral $ unCoin $ Generic.txOutSum tx
        !withdrawalSum = fromIntegral $ unCoin $ Generic.txWithdrawalSum tx
    !resolvedInputs <- mapM (resolveTxInputs (fst <$> groupedTxOut grouped)) (Generic.txInputs tx)
    let !inSum = sum $ map (unDbLovelace . thrd3) resolvedInputs
    let diffSum = if inSum >= outSum then inSum - outSum else 0
    let !fees = maybe diffSum (fromIntegral . unCoin) (Generic.txFees tx)
    let !txHash = Generic.txHash tx
    -- Insert transaction and get txId from the DB.
    !txId <- lift . DB.insertTx $
              DB.Tx
                { DB.txHash = txHash
                , DB.txBlockNo = fromIntegral $ unBlockNo blkNo
                , DB.txBlockIndex = blockIndex
                , DB.txOutSum = DB.DbLovelace outSum
                , DB.txFee = DB.DbLovelace $ fromIntegral fees
                , DB.txDeposit =
                    if not (Generic.txValidContract tx)
                      then 0
                      else fromIntegral (inSum + withdrawalSum) - fromIntegral (outSum + fees)
                , DB.txSize = Generic.txSize tx
                , DB.txInvalidBefore = DbWord64 . unSlotNo <$> Generic.txInvalidBefore tx
                , DB.txInvalidHereafter = DbWord64 . unSlotNo <$> Generic.txInvalidHereafter tx
                , DB.txValidContract = Generic.txValidContract tx
                , DB.txScriptSize = sum $ Generic.txScriptSizes tx
                }

    !txOutsGrouped <- mapM (prepareTxOut tracer cache blkNo (txId, txHash)) (Generic.txOutputs tx)

    if not (Generic.txValidContract tx)
      then do
        let txIns = map (prepareTxIn blkNo txId mempty) resolvedInputs
        pure $ grouped <> BlockGroupedData txIns txOutsGrouped

      else do
        -- The following operations only happen if the script passes stage 2 validation (or the tx
        -- has no script).
        !redeemers <- Map.fromList <$> mapM (insertRedeemer tracer blkNo (fst <$> groupedTxOut grouped) txId) (Generic.txRedeemer tx)

        mapM_ (insertDatum tracer txId) (Generic.txData tx)
        -- Insert the transaction inputs and collateral inputs (Alonzo).
        let txIns = map (prepareTxIn blkNo txId redeemers) resolvedInputs
        mapM_ (insertCollateralTxIn tracer blkNo txId) (Generic.txCollateralInputs tx)

        whenStrictJust (maybeToStrict $ Generic.txMetadata tx) $ \ md ->
          insertTxMetadata tracer blkNo txId md

        mapM_ (insertCertificate tracer cache isMember network blkNo txId epochNo slotNo redeemers) $ Generic.txCertificates tx
        mapM_ (insertWithdrawals tracer cache blkNo txId redeemers) $ Generic.txWithdrawals tx
        mapM_ (insertReferenceTxIn tracer blkNo txId) (Generic.txReferenceInputs tx)
        mapM_ (inertCollateralTxOut tracer cache blkNo (txId, txHash)) (Generic.txCollateralOutputs tx)
        mapM_ (insertParamProposal tracer blkNo) $ Generic.txParamProposal tx

        insertMaTxMint tracer cache txId $ Generic.txMint tx

        mapM_ (insertScript tracer txId) $ Generic.txScripts tx
        mapM_ (insertExtraKeyWitness tracer txId) $ Generic.txExtraKeyWitnesses tx

        pure $ grouped <> BlockGroupedData txIns txOutsGrouped

prepareTxOut
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> BlockNo -> (DB.TxId, ByteString) -> Generic.TxOut
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) (ExtendedTxOut, [MissingMaTxOut])
prepareTxOut tracer cache blkNo (txId, txHash) (Generic.TxOut index addr addrRaw value maMap mScript dt) = do
    mSaId <- lift $ insertStakeAddressRefIfMissing tracer cache blkNo addr
    mDatumId <- Generic.whenInlineDatum dt $ insertDatum tracer txId
    mScriptId <- whenMaybe mScript $ insertScript tracer txId
    let !txOut = DB.TxOut
                  { DB.txOutTxId = txId
                  , DB.txOutIndex = fromIntegral index
                  , DB.txOutAddress = Generic.renderAddress addr
                  , DB.txOutAddressRaw = addrRaw
                  , DB.txOutAddressHasScript = hasScript
                  , DB.txOutPaymentCred = Generic.maybePaymentCred addr
                  , DB.txOutStakeAddressId = mSaId
                  , DB.txOutValue = Generic.coinToDbLovelace value
                  , DB.txOutDataHash = Generic.getTxOutDatumHash dt
                  , DB.txOutInlineDatumId = mDatumId
                  , DB.txOutReferenceScriptId = mScriptId
                  , DB.txOutBlockNo = fromIntegral $ unBlockNo blkNo
                  }
    let !eutxo = ExtendedTxOut txHash txOut
    !maTxOuts <- prepareMaTxOuts tracer cache maMap
    pure (eutxo, maTxOuts)
  where
    hasScript :: Bool
    hasScript = maybe False Generic.hasCredScript (Generic.getPaymentCred addr)

inertCollateralTxOut
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> BlockNo -> (DB.TxId, ByteString) -> Generic.TxOut
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
inertCollateralTxOut tracer cache blkNo (txId, _txHash) (Generic.TxOut index addr addrRaw value maMap mScript dt) = do
    mSaId <- lift $ insertStakeAddressRefIfMissing tracer cache blkNo addr
    mDatumId <- Generic.whenInlineDatum dt $ insertDatum tracer txId
    mScriptId <- whenMaybe mScript $ insertScript tracer txId
    void . lift . DB.insertCollateralTxOut $
                DB.CollateralTxOut
                  { DB.collateralTxOutTxId = txId
                  , DB.collateralTxOutIndex = index
                  , DB.collateralTxOutAddress = Generic.renderAddress addr
                  , DB.collateralTxOutAddressRaw = addrRaw
                  , DB.collateralTxOutAddressHasScript = hasScript
                  , DB.collateralTxOutPaymentCred = Generic.maybePaymentCred addr
                  , DB.collateralTxOutStakeAddressId = mSaId
                  , DB.collateralTxOutValue = Generic.coinToDbLovelace value
                  , DB.collateralTxOutDataHash = Generic.getTxOutDatumHash dt
                  , DB.collateralTxOutMultiAssetsDescr = textShow maMap
                  , DB.collateralTxOutInlineDatumId = mDatumId
                  , DB.collateralTxOutReferenceScriptId = mScriptId
                  , DB.collateralTxOutBlockNo = fromIntegral $ unBlockNo blkNo
                  }
    -- TODO: Is there any reason to add new tables for collateral multi-assets/multi-asset-outputs
  where
    hasScript :: Bool
    hasScript = maybe False Generic.hasCredScript (Generic.getPaymentCred addr)

prepareTxIn
    :: BlockNo -> DB.TxId -> Map Word64 DB.RedeemerId
    -> (Generic.TxIn, DB.TxId, DbLovelace)
    -> DB.TxIn
prepareTxIn blkNo txInId redeemers (txIn, txOutId, _lovelace) =
    DB.TxIn
      { DB.txInTxInId = txInId
      , DB.txInTxOutId = txOutId
      , DB.txInTxOutIndex = fromIntegral $ Generic.txInIndex txIn
      , DB.txInRedeemerId = mlookup (Generic.txInRedeemerIndex txIn) redeemers
      , DB.txInBlockNo = fromIntegral $ unBlockNo blkNo
      }

insertCollateralTxIn
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> BlockNo -> DB.TxId -> Generic.TxIn
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertCollateralTxIn _tracer blkNo txInId (Generic.TxIn txId index _) = do
  txOutId <- liftLookupFail "insertCollateralTxIn" $ DB.queryTxId txId
  void . lift . DB.insertCollateralTxIn $
            DB.CollateralTxIn
              { DB.collateralTxInTxInId = txInId
              , DB.collateralTxInTxOutId = txOutId
              , DB.collateralTxInTxOutIndex = fromIntegral index
              , DB.collateralTxInBlockNo = fromIntegral $ unBlockNo blkNo
              }

insertReferenceTxIn
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> BlockNo -> DB.TxId -> Generic.TxIn
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertReferenceTxIn _tracer blkNo txInId (Generic.TxIn txId index _) = do
  txOutId <- liftLookupFail "insertReferenceTxIn" $ DB.queryTxId txId
  void . lift . DB.insertReferenceTxIn $
            DB.ReferenceTxIn
              { DB.referenceTxInTxInId = txInId
              , DB.referenceTxInTxOutId = txOutId
              , DB.referenceTxInTxOutIndex = fromIntegral index
              , DB.referenceTxInBlockNo = fromIntegral $ unBlockNo blkNo
              }

insertCertificate
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> IsPoolMember -> Ledger.Network -> BlockNo -> DB.TxId -> EpochNo
    -> SlotNo -> Map Word64 DB.RedeemerId -> Generic.TxCertificate
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertCertificate tracer cache isMember network blkNo txId epochNo slotNo redeemers (Generic.TxCertificate ridx idx cert) =
  case cert of
    Shelley.DCertDeleg deleg -> insertDelegCert tracer cache network blkNo txId idx mRedeemerId epochNo slotNo deleg
    Shelley.DCertPool pool -> insertPoolCert tracer cache isMember network epochNo blkNo txId idx pool
    Shelley.DCertMir mir -> insertMirCert tracer cache network blkNo txId idx mir
    Shelley.DCertGenesis _gen -> do
        -- TODO : Low priority
        liftIO $ logWarning tracer "insertCertificate: Unhandled DCertGenesis certificate"
        pure ()
  where
    mRedeemerId :: Maybe DB.RedeemerId
    mRedeemerId = mlookup ridx redeemers

insertPoolCert
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> IsPoolMember -> Ledger.Network -> EpochNo -> BlockNo
    -> DB.TxId -> Word16 -> Shelley.PoolCert StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertPoolCert tracer cache isMember network epoch blkNo txId idx pCert =
  case pCert of
    Shelley.RegPool pParams -> insertPoolRegister tracer cache isMember network epoch blkNo txId idx pParams
    Shelley.RetirePool keyHash epochNum -> insertPoolRetire txId cache epochNum idx keyHash

insertDelegCert
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> Ledger.Network -> BlockNo -> DB.TxId -> Word16
    -> Maybe DB.RedeemerId -> EpochNo -> SlotNo -> Shelley.DelegCert StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertDelegCert _tracer cache network blkNo txId idx mRedeemerId epochNo slotNo dCert =
  case dCert of
    Shelley.RegKey cred -> insertStakeRegistration epochNo blkNo txId idx $ Generic.annotateStakingCred network cred
    Shelley.DeRegKey cred -> insertStakeDeregistration cache network epochNo blkNo txId idx mRedeemerId cred
    Shelley.Delegate (Shelley.Delegation cred poolkh) -> insertDelegation cache network epochNo slotNo blkNo idx mRedeemerId cred poolkh

insertPoolRegister
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> IsPoolMember -> Ledger.Network -> EpochNo -> BlockNo -> DB.TxId
    -> Word16 -> Shelley.PoolParams StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertPoolRegister _tracer cache isMember network epochNo blkNo txId idx params = do
    poolHashId <- lift $ insertPoolKeyWithCache cache CacheNew (Shelley._poolId params)
    mdId <- case strictMaybeToMaybe $ Shelley._poolMD params of
              Just md -> Just <$> insertMetaDataRef poolHashId txId md
              Nothing -> pure Nothing

    epochActivationDelay <- mkEpochActivationDelay poolHashId

    saId <- lift $ insertStakeAddressWithCache cache CacheNew blkNo (adjustNetworkTag $ Shelley._poolRAcnt params)
    poolUpdateId <- lift . DB.insertPoolUpdate $
                      DB.PoolUpdate
                        { DB.poolUpdateHashId = poolHashId
                        , DB.poolUpdateCertIndex = idx
                        , DB.poolUpdateVrfKeyHash = hashToBytes (Shelley._poolVrf params)
                        , DB.poolUpdatePledge = Generic.coinToDbLovelace (Shelley._poolPledge params)
                        , DB.poolUpdateRewardAddrId = saId
                        , DB.poolUpdateActiveEpochNo = unEpochNo epochNo + epochActivationDelay
                        , DB.poolUpdateMetaId = mdId
                        , DB.poolUpdateMargin = realToFrac $ Ledger.unboundRational (Shelley._poolMargin params)
                        , DB.poolUpdateFixedCost = Generic.coinToDbLovelace (Shelley._poolCost params)
                        , DB.poolUpdateRegisteredTxId = txId
                        }

    mapM_ (insertPoolOwner cache network poolUpdateId blkNo) $ toList (Shelley._poolOwners params)
    mapM_ (insertPoolRelay poolUpdateId) $ toList (Shelley._poolRelays params)

  where
    mkEpochActivationDelay :: MonadIO m => DB.PoolHashId -> ExceptT SyncNodeError (ReaderT SqlBackend m) Word64
    mkEpochActivationDelay poolHashId =
      if isMember (Shelley._poolId params)
      then pure 3
      else do
            -- if the pool is not registered at the end of the previous block, check for
            -- other registrations at the current block. If this is the first registration
            -- then it's +2, else it's +3.
            otherUpdates <- lift $ queryPoolUpdateByBlock blkNo poolHashId
            pure $ if otherUpdates then 3 else 2

    -- Ignore the network in the `RewardAcnt` and use the provided one instead.
    -- This is a workaround for https://github.com/input-output-hk/cardano-db-sync/issues/546
    adjustNetworkTag :: Ledger.RewardAcnt StandardCrypto -> Ledger.RewardAcnt StandardCrypto
    adjustNetworkTag (Shelley.RewardAcnt _ cred) = Shelley.RewardAcnt network cred

insertPoolRetire
    :: (MonadBaseControl IO m, MonadIO m)
    => DB.TxId -> Cache -> EpochNo -> Word16 -> Ledger.KeyHash 'Ledger.StakePool StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertPoolRetire txId cache epochNum idx keyHash = do
  poolId <- liftLookupFail "insertPoolRetire" $ queryPoolKeyWithCache cache CacheNew keyHash
  void . lift . DB.insertPoolRetire $
    DB.PoolRetire
      { DB.poolRetireHashId = poolId
      , DB.poolRetireCertIndex = idx
      , DB.poolRetireAnnouncedTxId = txId
      , DB.poolRetireRetiringEpoch = unEpochNo epochNum
      }


insertMetaDataRef
    :: (MonadBaseControl IO m, MonadIO m)
    => DB.PoolHashId -> DB.TxId -> Shelley.PoolMetadata
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) DB.PoolMetadataRefId
insertMetaDataRef poolId txId md =
  lift . DB.insertPoolMetadataRef $
    DB.PoolMetadataRef
      { DB.poolMetadataRefPoolId = poolId
      , DB.poolMetadataRefUrl = Ledger.urlToText (Shelley._poolMDUrl md)
      , DB.poolMetadataRefHash = Shelley._poolMDHash md
      , DB.poolMetadataRefRegisteredTxId = txId
      }

insertStakeAddressWithCache
    :: (MonadBaseControl IO m, MonadIO m)
    => Cache -> CacheNew -> BlockNo -> Shelley.RewardAcnt StandardCrypto
    -> ReaderT SqlBackend m DB.StakeAddressId
insertStakeAddressWithCache cache cacheNew txId rewardAddr = do
    eiAddrId <- queryRewardAccountWithCacheRetBs cache cacheNew rewardAddr
    case eiAddrId of
      Left (_err, bs) -> insertStakeAddress txId rewardAddr (Just bs)
      Right addrId -> pure addrId

-- If the address already esists in the table, it will not be inserted again (due to
-- the uniqueness constraint) but the function will return the 'StakeAddressId'.
insertStakeAddress
    :: (MonadBaseControl IO m, MonadIO m)
    => BlockNo -> Shelley.RewardAcnt StandardCrypto -> Maybe ByteString
    -> ReaderT SqlBackend m DB.StakeAddressId
insertStakeAddress blkNo rewardAddr mStakeCred =
    DB.insertStakeAddress $
      DB.StakeAddress
        { DB.stakeAddressHashRaw = addrBs
        , DB.stakeAddressView = Generic.renderRewardAcnt rewardAddr
        , DB.stakeAddressScriptHash = Generic.getCredentialScriptHash $ Ledger.getRwdCred rewardAddr
        , DB.stakeAddressBlockNo = fromIntegral $ unBlockNo blkNo
        }
  where
    addrBs :: ByteString
    addrBs = fromMaybe (Ledger.serialiseRewardAcnt rewardAddr) mStakeCred

-- | Insert a stake address if it is not already in the `stake_address` table. Regardless of
-- whether it is newly inserted or it is already there, we retrun the `StakeAddressId`.
insertStakeAddressRefIfMissing
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> BlockNo -> Ledger.Addr StandardCrypto
    -> ReaderT SqlBackend m (Maybe DB.StakeAddressId)
insertStakeAddressRefIfMissing _trce cache txId addr =
    case addr of
      Ledger.AddrBootstrap {} -> pure Nothing
      Ledger.Addr nw _pcred sref ->
        case sref of
          Ledger.StakeRefBase cred -> do
            Just <$> insertStakeAddressWithCache cache DontCacheNew txId (Shelley.RewardAcnt nw cred)
          Ledger.StakeRefPtr ptr -> do
            queryStakeRefPtr ptr
          Ledger.StakeRefNull -> pure Nothing

insertPoolOwner
    :: (MonadBaseControl IO m, MonadIO m)
    => Cache -> Ledger.Network -> DB.PoolUpdateId -> BlockNo -> Ledger.KeyHash 'Ledger.Staking StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertPoolOwner cache network poolUpdateId blkNo skh = do
  saId <- lift $ insertStakeAddressWithCache cache CacheNew blkNo (Shelley.RewardAcnt network (Ledger.KeyHashObj skh))
  void . lift . DB.insertPoolOwner $
    DB.PoolOwner
      { DB.poolOwnerAddrId = saId
      , DB.poolOwnerPoolUpdateId = poolUpdateId
      }

insertStakeRegistration
    :: (MonadBaseControl IO m, MonadIO m)
    => EpochNo -> BlockNo -> DB.TxId -> Word16 -> Shelley.RewardAcnt StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertStakeRegistration epochNo blkNo txId idx rewardAccount = do
  -- We by-pass the cache here It's likely it won't hit.
  -- We don't store to the cache yet, since there are many addrresses
  -- which are registered and never used.
  saId <- lift $ insertStakeAddress blkNo rewardAccount Nothing
  void . lift . DB.insertStakeRegistration $
    DB.StakeRegistration
      { DB.stakeRegistrationAddrId = saId
      , DB.stakeRegistrationCertIndex = idx
      , DB.stakeRegistrationEpochNo = unEpochNo epochNo
      , DB.stakeRegistrationBlockNo = fromIntegral $ unBlockNo blkNo
      , DB.stakeRegistrationTxId = txId
      }

insertStakeDeregistration
    :: (MonadBaseControl IO m, MonadIO m)
    => Cache -> Ledger.Network -> EpochNo -> BlockNo -> DB.TxId -> Word16 -> Maybe DB.RedeemerId
    -> StakeCred
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertStakeDeregistration cache network epochNo blkNo txId idx mRedeemerId cred = do
    scId <- liftLookupFail "insertStakeDeregistration" $ queryStakeAddrWithCache cache EvictAndReturn network cred
    void . lift . DB.insertStakeDeregistration $
      DB.StakeDeregistration
        { DB.stakeDeregistrationAddrId = scId
        , DB.stakeDeregistrationCertIndex = idx
        , DB.stakeDeregistrationEpochNo = unEpochNo epochNo
        , DB.stakeDeregistrationTxId = txId
        , DB.stakeDeregistrationBlockNo = fromIntegral $ unBlockNo blkNo
        , DB.stakeDeregistrationRedeemerId = mRedeemerId
        }

insertDelegation
    :: (MonadBaseControl IO m, MonadIO m)
    => Cache -> Ledger.Network -> EpochNo -> SlotNo -> BlockNo -> Word16 -> Maybe DB.RedeemerId
    -> StakeCred -> Ledger.KeyHash 'Ledger.StakePool StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertDelegation cache network epochNo slotNo blkNo idx mRedeemerId cred poolkh = do
    addrId <- liftLookupFail "insertDelegation" $ queryStakeAddrWithCache cache CacheNew network cred
    poolHashId <-liftLookupFail "insertDelegation" $ queryPoolKeyWithCache cache CacheNew poolkh
    void . lift . DB.insertDelegation $
      DB.Delegation
        { DB.delegationAddrId = addrId
        , DB.delegationCertIndex = idx
        , DB.delegationPoolHashId = poolHashId
        , DB.delegationActiveEpochNo = unEpochNo epochNo + 2 -- The first epoch where this delegation is valid.
        , DB.delegationBlockNo = fromIntegral $ unBlockNo blkNo
        , DB.delegationSlotNo = unSlotNo slotNo
        , DB.delegationRedeemerId = mRedeemerId
        }

insertMirCert
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> Ledger.Network -> BlockNo -> DB.TxId -> Word16 -> Shelley.MIRCert StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertMirCert _tracer cache network blkNo txId idx mcert = do
    case Shelley.mirPot mcert of
      Shelley.ReservesMIR ->
        case Shelley.mirRewards mcert of
          Shelley.StakeAddressesMIR rwds -> mapM_ insertMirReserves $ Map.toList rwds
          Shelley.SendToOppositePotMIR xfrs -> insertPotTransfer (Ledger.toDeltaCoin xfrs)

      Shelley.TreasuryMIR -> do
        case Shelley.mirRewards mcert of
          Shelley.StakeAddressesMIR rwds -> mapM_ insertMirTreasury $ Map.toList rwds
          Shelley.SendToOppositePotMIR xfrs -> insertPotTransfer (invert $ Ledger.toDeltaCoin xfrs)

  where
    insertMirReserves
        :: (MonadBaseControl IO m, MonadIO m)
        => (StakeCred, Ledger.DeltaCoin)
        -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
    insertMirReserves (cred, dcoin) = do
      addrId <- lift . insertStakeAddressWithCache cache CacheNew blkNo $ Generic.annotateStakingCred network cred
      void . lift . DB.insertReserve $
        DB.Reserve
          { DB.reserveAddrId = addrId
          , DB.reserveTxId = txId
          , DB.reserveCertIndex = idx
          , DB.reserveBlockNo = fromIntegral $ unBlockNo blkNo
          , DB.reserveAmount = DB.deltaCoinToDbInt65 dcoin
          }

    insertMirTreasury
        :: (MonadBaseControl IO m, MonadIO m)
        => (StakeCred, Ledger.DeltaCoin)
        -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
    insertMirTreasury (cred, dcoin) = do
      addrId <- lift . insertStakeAddressWithCache cache CacheNew blkNo $ Generic.annotateStakingCred network cred
      void . lift . DB.insertTreasury $
        DB.Treasury
          { DB.treasuryAddrId = addrId
          , DB.treasuryTxId = txId
          , DB.treasuryCertIndex = idx
          , DB.treasuryBlockNo = fromIntegral $ unBlockNo blkNo
          , DB.treasuryAmount = DB.deltaCoinToDbInt65 dcoin
          }

    insertPotTransfer
        :: (MonadBaseControl IO m, MonadIO m)
        => Ledger.DeltaCoin -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
    insertPotTransfer dcoinTreasury = do
      void . lift . DB.insertPotTransfer $
        DB.PotTransfer
          { DB.potTransferCertIndex = idx
          , DB.potTransferTreasury = DB.deltaCoinToDbInt65 dcoinTreasury
          , DB.potTransferReserves = DB.deltaCoinToDbInt65 (invert dcoinTreasury)
          , DB.potTransferBlockNo = fromIntegral $ unBlockNo blkNo
          }

insertWithdrawals
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> BlockNo -> DB.TxId -> Map Word64 DB.RedeemerId
    -> Generic.TxWithdrawal
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertWithdrawals _tracer cache blkNo txId redeemers txWdrl = do
    addrId <- liftLookupFail "insertWithdrawals" $
        queryRewardAccountWithCache cache CacheNew $ Generic.txwRewardAccount txWdrl
    void . lift . DB.insertWithdrawal $
      DB.Withdrawal
        { DB.withdrawalAddrId = addrId
        , DB.withdrawalTxId = txId
        , DB.withdrawalAmount = Generic.coinToDbLovelace $ Generic.txwAmount txWdrl
        , DB.withdrawalRedeemerId = mlookup (Generic.txwRedeemerIndex txWdrl) redeemers
        , DB.withdrawalBlockNo = fromIntegral $ unBlockNo blkNo
        }

insertPoolRelay
    :: (MonadBaseControl IO m, MonadIO m)
    => DB.PoolUpdateId -> Shelley.StakePoolRelay
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertPoolRelay updateId relay =
  void . lift . DB.insertPoolRelay $
    case relay of
      Shelley.SingleHostAddr mPort mIpv4 mIpv6 ->
        DB.PoolRelay -- An IPv4 and/or IPv6 address
          { DB.poolRelayUpdateId = updateId
          , DB.poolRelayIpv4 = textShow <$> strictMaybeToMaybe mIpv4
          , DB.poolRelayIpv6 = textShow <$> strictMaybeToMaybe mIpv6
          , DB.poolRelayDnsName = Nothing
          , DB.poolRelayDnsSrvName = Nothing
          , DB.poolRelayPort = Ledger.portToWord16 <$> strictMaybeToMaybe mPort
          }
      Shelley.SingleHostName mPort name ->
        DB.PoolRelay -- An A or AAAA DNS record
          { DB.poolRelayUpdateId = updateId
          , DB.poolRelayIpv4 = Nothing
          , DB.poolRelayIpv6 = Nothing
          , DB.poolRelayDnsName = Just (Ledger.dnsToText name)
          , DB.poolRelayDnsSrvName = Nothing
          , DB.poolRelayPort = Ledger.portToWord16 <$> strictMaybeToMaybe mPort
          }
      Shelley.MultiHostName name ->
        DB.PoolRelay -- An SRV DNS record
          { DB.poolRelayUpdateId = updateId
          , DB.poolRelayIpv4 = Nothing
          , DB.poolRelayIpv6 = Nothing
          , DB.poolRelayDnsName = Nothing
          , DB.poolRelayDnsSrvName = Just (Ledger.dnsToText name)
          , DB.poolRelayPort = Nothing
          }

insertParamProposal
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> BlockNo -> ParamProposal
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertParamProposal _tracer blkNo pp = do
  cmId <- maybe (pure Nothing) (fmap Just . insertCostModel blkNo) (pppCostmdls pp)
  void . lift . DB.insertParamProposal $
    DB.ParamProposal
      { DB.paramProposalBlockNo = fromIntegral $ unBlockNo blkNo

      , DB.paramProposalEpochNo = unEpochNo $ pppEpochNo pp
      , DB.paramProposalKey = pppKey pp
      , DB.paramProposalMinFeeA = fromIntegral <$> pppMinFeeA pp
      , DB.paramProposalMinFeeB = fromIntegral <$> pppMinFeeB pp
      , DB.paramProposalMaxBlockSize = fromIntegral <$> pppMaxBlockSize pp
      , DB.paramProposalMaxTxSize = fromIntegral <$> pppMaxTxSize pp
      , DB.paramProposalMaxBhSize = fromIntegral <$> pppMaxBhSize pp
      , DB.paramProposalKeyDeposit = Generic.coinToDbLovelace <$> pppKeyDeposit pp
      , DB.paramProposalPoolDeposit = Generic.coinToDbLovelace <$> pppPoolDeposit pp
      , DB.paramProposalMaxEpoch = unEpochNo <$> pppMaxEpoch pp
      , DB.paramProposalOptimalPoolCount = fromIntegral <$> pppOptimalPoolCount pp
      , DB.paramProposalInfluence = fromRational <$> pppInfluence pp
      , DB.paramProposalMonetaryExpandRate = Generic.unitIntervalToDouble <$> pppMonetaryExpandRate pp
      , DB.paramProposalTreasuryGrowthRate = Generic.unitIntervalToDouble <$> pppTreasuryGrowthRate pp
      , DB.paramProposalDecentralisation = Generic.unitIntervalToDouble <$> pppDecentralisation pp
      , DB.paramProposalEntropy = Generic.nonceToBytes =<< pppEntropy pp
      , DB.paramProposalProtocolMajor = fromIntegral . Ledger.pvMajor <$> pppProtocolVersion pp
      , DB.paramProposalProtocolMinor = fromIntegral . Ledger.pvMinor <$> pppProtocolVersion pp
      , DB.paramProposalMinUtxoValue = Generic.coinToDbLovelace <$> pppMinUtxoValue pp
      , DB.paramProposalMinPoolCost = Generic.coinToDbLovelace <$> pppMinPoolCost pp

      -- New for Alonzo

      , DB.paramProposalCoinsPerUtxoSize = Generic.coinToDbLovelace <$> pppCoinsPerUtxo pp
      , DB.paramProposalCostModelId = cmId
      , DB.paramProposalPriceMem = realToFrac <$> pppPriceMem pp
      , DB.paramProposalPriceStep = realToFrac <$> pppPriceStep pp
      , DB.paramProposalMaxTxExMem = DbWord64 <$> pppMaxTxExMem pp
      , DB.paramProposalMaxTxExSteps = DbWord64 <$> pppMaxTxExSteps pp
      , DB.paramProposalMaxBlockExMem = DbWord64 <$> pppMaxBlockExMem pp
      , DB.paramProposalMaxBlockExSteps = DbWord64 <$> pppMaxBlockExSteps pp
      , DB.paramProposalMaxValSize = DbWord64 . fromIntegral <$> pppMaxValSize pp
      , DB.paramProposalCollateralPercent = fromIntegral <$> pppCollateralPercentage pp
      , DB.paramProposalMaxCollateralInputs = fromIntegral <$> pppMaxCollateralInputs pp
      }

insertRedeemer
  :: (MonadBaseControl IO m, MonadIO m)
  => Trace IO Text -> BlockNo -> [ExtendedTxOut] -> DB.TxId -> (Word64, Generic.TxRedeemer)
  -> ExceptT SyncNodeError (ReaderT SqlBackend m) (Word64, DB.RedeemerId)
insertRedeemer tracer blkNo groupedOutputs txId (rix, redeemer) = do
    tdId <- insertRedeemerData tracer blkNo txId $ Generic.txRedeemerData redeemer
    scriptHash <- findScriptHash
    rid <- lift . DB.insertRedeemer $
              DB.Redeemer
                { DB.redeemerTxId = txId
                , DB.redeemerUnitMem = Generic.txRedeemerMem redeemer
                , DB.redeemerUnitSteps = Generic.txRedeemerSteps redeemer
                , DB.redeemerFee = DB.DbLovelace . fromIntegral . unCoin <$> Generic.txRedeemerFee redeemer
                , DB.redeemerPurpose = mkPurpose $ Generic.txRedeemerPurpose redeemer
                , DB.redeemerIndex = Generic.txRedeemerIndex redeemer
                , DB.redeemerScriptHash = scriptHash
                , DB.redeemerRedeemerDataId = tdId
                }
    pure (rix, rid)
  where
    mkPurpose :: Ledger.Tag -> DB.ScriptPurpose
    mkPurpose tag =
      case tag of
        Ledger.Spend -> DB.Spend
        Ledger.Mint -> DB.Mint
        Ledger.Cert -> DB.Cert
        Ledger.Rewrd -> DB.Rewrd

    findScriptHash
      :: (MonadBaseControl IO m, MonadIO m)
      => ExceptT SyncNodeError (ReaderT SqlBackend m) (Maybe ByteString)
    findScriptHash =
      case Generic.txRedeemerScriptHash redeemer of
        Nothing -> pure Nothing
        Just (Right bs) -> pure $ Just bs
        Just (Left txIn) -> resolveScriptHash groupedOutputs txIn

insertDatum
  :: (MonadBaseControl IO m, MonadIO m)
  => Trace IO Text -> DB.TxId -> Generic.PlutusData
  -> ExceptT SyncNodeError (ReaderT SqlBackend m) DB.DatumId
insertDatum tracer txId txd = do
    mDatumId <- lift $ DB.queryDatum $ Generic.txDataHash txd
    case mDatumId of
      Just datumId -> pure datumId
      Nothing -> do
        value <- safeDecodeToJson tracer "insertDatum" $ Generic.txDataValue txd
        lift . DB.insertDatum $ DB.Datum
          { DB.datumHash = Generic.txDataHash txd
          , DB.datumTxId = txId
          , DB.datumValue = value
          , DB.datumBytes = Generic.txDataBytes txd
          }

insertRedeemerData
  :: (MonadBaseControl IO m, MonadIO m)
  => Trace IO Text -> BlockNo -> DB.TxId -> Generic.PlutusData
  -> ExceptT SyncNodeError (ReaderT SqlBackend m) DB.RedeemerDataId
insertRedeemerData tracer blkNo txId txd = do
    mRedeemerDataId <- lift $ DB.queryRedeemerData $ Generic.txDataHash txd
    case mRedeemerDataId of
      Just redeemerDataId -> pure redeemerDataId
      Nothing -> do
        value <- safeDecodeToJson tracer "insertRedeemerData" $ Generic.txDataValue txd
        lift . DB.insertRedeemerData $ DB.RedeemerData
          { DB.redeemerDataHash = Generic.txDataHash txd
          , DB.redeemerDataTxId = txId
          , DB.redeemerDataValue = value
          , DB.redeemerDataBytes = Generic.txDataBytes txd
          , DB.redeemerDataBlockNo = fromIntegral $ unBlockNo blkNo
          }

insertTxMetadata
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> BlockNo -> DB.TxId -> Map Word64 TxMetadataValue
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertTxMetadata tracer (BlockNo blkNo) txId metadata =
    mapM_ insert $ Map.toList metadata
  where
    insert
        :: (MonadBaseControl IO m, MonadIO m)
        => (Word64, TxMetadataValue)
        -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
    insert (key, md) = do
      let jsonbs = LBS.toStrict $ Aeson.encode (metadataValueToJsonNoSchema md)
          singleKeyCBORMetadata = serialiseToCBOR $ makeTransactionMetadata (Map.singleton key md)
      mjson <- safeDecodeToJson tracer "insertTxMetadata" jsonbs

      void . lift . DB.insertTxMetadata $
        DB.TxMetadata
          { DB.txMetadataKey = DbWord64 key
          , DB.txMetadataJson = mjson
          , DB.txMetadataBytes = singleKeyCBORMetadata
          , DB.txMetadataTxId = txId
          , DB.txMetadataBlockNo = fromIntegral blkNo
          }

insertCostModel
    :: (MonadBaseControl IO m, MonadIO m)
    => BlockNo -> Map Language Ledger.CostModel
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) DB.CostModelId
insertCostModel blkNo cms =
  lift . DB.insertCostModel $
    DB.CostModel
      { DB.costModelHash = Crypto.abstractHashToBytes $ Crypto.serializeCborHash cms
      , DB.costModelCosts = Text.decodeUtf8 $ LBS.toStrict (Aeson.encode cms)
      , DB.costModelBlockNo = fromIntegral $ unBlockNo blkNo
      }

insertEpochParam
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> BlockNo -> EpochNo -> Generic.ProtoParams -> Ledger.Nonce
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertEpochParam _tracer blkNo epoch params nonce = do
  cmId <- maybe (pure Nothing) (fmap Just . insertCostModel blkNo) (Generic.ppCostmdls params)
  void . lift . DB.insertEpochParam $
    DB.EpochParam
      { DB.epochParamEpochNo = unEpochNo epoch
      , DB.epochParamMinFeeA = fromIntegral (Generic.ppMinfeeA params)
      , DB.epochParamMinFeeB = fromIntegral (Generic.ppMinfeeB params)
      , DB.epochParamMaxBlockSize = fromIntegral (Generic.ppMaxBBSize params)
      , DB.epochParamMaxTxSize = fromIntegral (Generic.ppMaxTxSize params)
      , DB.epochParamMaxBhSize = fromIntegral (Generic.ppMaxBHSize params)
      , DB.epochParamKeyDeposit = Generic.coinToDbLovelace (Generic.ppKeyDeposit params)
      , DB.epochParamPoolDeposit = Generic.coinToDbLovelace (Generic.ppPoolDeposit params)
      , DB.epochParamMaxEpoch = unEpochNo (Generic.ppMaxEpoch params)
      , DB.epochParamOptimalPoolCount = fromIntegral (Generic.ppOptialPoolCount params)
      , DB.epochParamInfluence = fromRational (Generic.ppInfluence params)
      , DB.epochParamMonetaryExpandRate = Generic.unitIntervalToDouble (Generic.ppMonetaryExpandRate params)
      , DB.epochParamTreasuryGrowthRate = Generic.unitIntervalToDouble (Generic.ppTreasuryGrowthRate params)
      , DB.epochParamDecentralisation = Generic.unitIntervalToDouble (Generic.ppDecentralisation params)
      , DB.epochParamExtraEntropy = Generic.nonceToBytes $ Generic.ppExtraEntropy params
      , DB.epochParamProtocolMajor = fromIntegral $ Ledger.pvMajor (Generic.ppProtocolVersion params)
      , DB.epochParamProtocolMinor = fromIntegral $ Ledger.pvMinor (Generic.ppProtocolVersion params)
      , DB.epochParamMinUtxoValue = Generic.coinToDbLovelace (Generic.ppMinUTxOValue params)
      , DB.epochParamMinPoolCost = Generic.coinToDbLovelace (Generic.ppMinPoolCost params)
      , DB.epochParamNonce = Generic.nonceToBytes nonce
      , DB.epochParamCoinsPerUtxoWord = Generic.coinToDbLovelace <$> Generic.ppCoinsPerUtxo params
      , DB.epochParamCostModelId = cmId
      , DB.epochParamPriceMem = realToFrac <$> Generic.ppPriceMem params
      , DB.epochParamPriceStep = realToFrac <$> Generic.ppPriceStep params
      , DB.epochParamMaxTxExMem = DbWord64 <$> Generic.ppMaxTxExMem params
      , DB.epochParamMaxTxExSteps = DbWord64 <$> Generic.ppMaxTxExSteps params
      , DB.epochParamMaxBlockExMem = DbWord64 <$> Generic.ppMaxBlockExMem params
      , DB.epochParamMaxBlockExSteps = DbWord64 <$> Generic.ppMaxBlockExSteps params
      , DB.epochParamMaxValSize = DbWord64 . fromIntegral <$> Generic.ppMaxValSize params
      , DB.epochParamCollateralPercent = fromIntegral <$> Generic.ppCollateralPercentage params
      , DB.epochParamMaxCollateralInputs = fromIntegral <$> Generic.ppMaxCollateralInputs params
      }

insertMaTxMint
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> DB.TxId -> Value StandardCrypto
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertMaTxMint _tracer cache txId (Value _adaShouldAlwaysBeZeroButWeDoNotCheck mintMap) =
    mapM_ (lift . insertOuter) $ Map.toList mintMap
  where
    insertOuter
        :: (MonadBaseControl IO m, MonadIO m)
        => (PolicyID StandardCrypto, Map AssetName Integer)
        -> ReaderT SqlBackend m ()
    insertOuter (policy, aMap) =
      mapM_ (insertInner policy) $ Map.toList aMap

    insertInner
        :: (MonadBaseControl IO m, MonadIO m)
        => PolicyID StandardCrypto -> (AssetName, Integer)
        -> ReaderT SqlBackend m ()
    insertInner policy (aname, amount) = do
      maId <- insertMultiAsset cache policy aname
      void . DB.insertMaTxMint $
        DB.MaTxMint
          { DB.maTxMintIdent = maId
          , DB.maTxMintQuantity = DB.integerToDbInt65 amount
          , DB.maTxMintTxId = txId
          }

prepareMaTxOuts
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> Cache -> Map (PolicyID StandardCrypto) (Map AssetName Integer)
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) [MissingMaTxOut]
prepareMaTxOuts _tracer cache maMap =
    concatMapM (lift . prepareOuter) $ Map.toList maMap
  where
    prepareOuter
        :: (MonadBaseControl IO m, MonadIO m)
        => (PolicyID StandardCrypto, Map AssetName Integer)
        -> ReaderT SqlBackend m [MissingMaTxOut]
    prepareOuter (policy, aMap) =
      mapM (prepareInner policy) $ Map.toList aMap

    prepareInner
        :: (MonadBaseControl IO m, MonadIO m)
        => PolicyID StandardCrypto -> (AssetName, Integer)
        -> ReaderT SqlBackend m MissingMaTxOut
    prepareInner policy (aname, amount) = do
      maId <- insertMultiAsset cache policy aname
      pure $
        MissingMaTxOut
          { mmtoIdent = maId
          , mmtoQuantity = DbWord64 (fromIntegral amount)
          }

insertMultiAsset
    :: (MonadBaseControl IO m, MonadIO m)
    => Cache -> PolicyID StandardCrypto -> AssetName
    -> ReaderT SqlBackend m DB.MultiAssetId
insertMultiAsset cache policy aName = do
    mId <- queryMAWithCache cache policy aName
    case mId of
      Right maId -> pure maId
      Left (policyBs, assetNameBs) -> DB.insertMultiAssetUnchecked $
                  DB.MultiAsset
                    { DB.multiAssetPolicy = policyBs
                    , DB.multiAssetName = assetNameBs
                    , DB.multiAssetFingerprint = DB.unAssetFingerprint (DB.mkAssetFingerprint policyBs assetNameBs)
                    }

insertScript
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> DB.TxId -> Generic.TxScript
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) DB.ScriptId
insertScript tracer txId script = do
    mScriptId <- lift $ DB.queryScript $ Generic.txScriptHash script
    case mScriptId of
      Just scriptId -> pure scriptId
      Nothing -> do
        json <- scriptConvert script
        lift . DB.insertScript $
          DB.Script
            { DB.scriptTxId = txId
            , DB.scriptHash = Generic.txScriptHash script
            , DB.scriptType = Generic.txScriptType script
            , DB.scriptSerialisedSize = Generic.txScriptPlutusSize script
            , DB.scriptJson = json
            , DB.scriptBytes = Generic.txScriptCBOR script
            }
  where
    scriptConvert :: MonadIO m => Generic.TxScript -> m (Maybe Text)
    scriptConvert s =
      maybe (pure Nothing) (safeDecodeToJson tracer "insertScript") (Generic.txScriptJson s)

insertExtraKeyWitness
    :: (MonadBaseControl IO m, MonadIO m)
    => Trace IO Text -> DB.TxId -> ByteString
    -> ExceptT SyncNodeError (ReaderT SqlBackend m) ()
insertExtraKeyWitness _tracer txId keyHash = do
  void . lift . DB.insertExtraKeyWitness $
          DB.ExtraKeyWitness
          { DB.extraKeyWitnessHash = keyHash
          , DB.extraKeyWitnessTxId = txId
          }

insertPots
    :: (MonadBaseControl IO m, MonadIO m)
    => BlockNo
    -> SlotNo -> EpochNo
    -> Shelley.AdaPots
    -> ExceptT e (ReaderT SqlBackend m) ()
insertPots (BlockNo blkNo) slotNo epochNo pots =
    void . lift $ DB.insertAdaPots $
      DB.AdaPots
        { DB.adaPotsSlotNo = unSlotNo slotNo
        , DB.adaPotsEpochNo = unEpochNo epochNo
        , DB.adaPotsTreasury = Generic.coinToDbLovelace $ Shelley.treasuryAdaPot pots
        , DB.adaPotsReserves = Generic.coinToDbLovelace $ Shelley.reservesAdaPot pots
        , DB.adaPotsRewards = Generic.coinToDbLovelace $ Shelley.rewardsAdaPot pots
        , DB.adaPotsUtxo = Generic.coinToDbLovelace $ Shelley.utxoAdaPot pots
        , DB.adaPotsDeposits = Generic.coinToDbLovelace $ Shelley.depositsAdaPot pots
        , DB.adaPotsFees = Generic.coinToDbLovelace $ Shelley.feesAdaPot pots
        , DB.adaPotsBlockNo = fromIntegral blkNo
        }
