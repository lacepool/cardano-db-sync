{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.Db.Migration.Haskell.MultiAsset
  ( migrateMultiAssetOne
  ) where

import qualified Cardano.Crypto.Hash as Crypto

import qualified Cardano.Db.Insert as Db
import qualified Cardano.Db.Schema as Db
import           Cardano.Db.Types

import qualified Cardano.Ledger.Hashes as Ledger
import           Cardano.Ledger.Mary.Value (AssetName (..), PolicyID (..))
import qualified Cardano.Ledger.Shelley.Scripts as Shelley

import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Control.Monad.Trans.Reader (ReaderT)

import           Data.ByteString.Char8 (ByteString)
import           Data.Maybe (fromMaybe)

import           Database.Esqueleto.Legacy (Value (..), distinct, from, select, set, update, val,
                   where_, (&&.), (=.), (==.), (^.))
import           Database.Persist.Sql (SqlBackend)

import           Ouroboros.Consensus.Shelley.Protocol (StandardCrypto)


migrateMultiAssetOne :: (MonadBaseControl IO m, MonadIO m) => ReaderT SqlBackend m ()
migrateMultiAssetOne = do
  xs <- queryDisinctAssets
  ys <- mapM insertMultiAsset xs
  mapM_ updateMultiAssetRefs ys

-- -------------------------------------------------------------------------------------------------

data MultiAssetTmp = MultiAssetTmp
  { matPolicyId :: !(PolicyID StandardCrypto)
  , matAssetName :: !AssetName
  , matFingerprint :: !AssetFingerprint
  , matDbIndex :: !(Maybe Db.MultiAssetId)
  }

insertMultiAsset :: (MonadBaseControl IO m, MonadIO m) => MultiAssetTmp -> ReaderT SqlBackend m MultiAssetTmp
insertMultiAsset ma = do
    index <- Db.insertMultiAsset $
                  Db.MultiAsset
                    { Db.multiAssetPolicy = unScriptHash (policyID $ matPolicyId ma)
                    , Db.multiAssetName = assetName (matAssetName ma)
                    , Db.multiAssetFingerprint = unAssetFingerprint (matFingerprint ma)
                    }
    pure $ ma { matDbIndex = Just index }

queryDisinctAssets :: MonadIO m => ReaderT SqlBackend m [MultiAssetTmp]
queryDisinctAssets = do
    res <- select . distinct . from $ \ mint ->
            pure (mint ^. Db.MaTxMintPolicy, mint ^. Db.MaTxMintName)
    pure $ map convert res
  where
    convert :: (Value ByteString, Value ByteString) -> MultiAssetTmp
    convert (Value pol, Value name) =
      let policyId = mkPolicyId pol in
      MultiAssetTmp
        { matPolicyId = policyId
        , matAssetName = AssetName name
        , matFingerprint = mkAssetFingerprint policyId (AssetName name)
        , matDbIndex = Nothing
        }

    mkPolicyId :: ByteString -> PolicyID StandardCrypto
    mkPolicyId = PolicyID . Ledger.ScriptHash . fromMaybe (error "mkPolicyId") . Crypto.hashFromBytes

updateMultiAssetRefs :: (MonadBaseControl IO m, MonadIO m) => MultiAssetTmp -> ReaderT SqlBackend m ()
updateMultiAssetRefs ma = do
    update $ \ maMint -> do
      set maMint [ Db.MaTxMintIdent =. val (matDbIndex ma) ]
      where_ (maMint ^. Db.MaTxMintPolicy ==. val policyId &&. maMint ^. Db.MaTxMintName ==. val aName)
    update $ \ maTxOut -> do
      set maTxOut [ Db.MaTxOutIdent =. val (matDbIndex ma) ]
      where_ (maTxOut ^. Db.MaTxOutPolicy ==. val policyId &&. maTxOut ^. Db.MaTxOutName ==. val aName)
  where
    policyId :: ByteString
    policyId = unScriptHash (policyID $ matPolicyId ma)

    aName :: ByteString
    aName = assetName (matAssetName ma)

unScriptHash :: Shelley.ScriptHash StandardCrypto -> ByteString
unScriptHash (Shelley.ScriptHash h) = Crypto.hashToBytes h
