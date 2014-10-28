{-# LANGUAGE
    MultiParamTypeClasses
  , RecordWildCards
  #-}

module Vaultaire.Collector.Common.Process where

import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import qualified Data.ByteString as BS
import           Options.Applicative
import           System.IO

import           Marquise.Client
import           Vaultaire.Types

import           Vaultaire.Collector.Common.Classes
import           Vaultaire.Collector.Common.Types

runCollector :: (MonadIO m, CollectorMonad o s m) => m ()
runCollector = do
    (parseExtraOpts', desc, hdr) <- parseExtraOpts
    (cOpts, eOpts) <- liftIO $ execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts') (fullDesc <> progDesc desc <> header hdr))
    let opts = (cOpts, eOpts)
    eState <- initialiseExtraState opts
    evalStateT (runReaderT (unCollector (setup >> act >> cleanup)) opts) (undefined, eState)
  where
    setup = setSpoolFiles >> setInitialCache
    cleanup = writeCache
    act :: (MonadIO m, CollectorMonad o s m) => Collector o s m ()
    act = forever $ do
        (addr, sd, ts, payload) <- collectFourTuple
        (cS@CommonState{..}, eS) <- get
        let hash = hashSource sd
        let cache = collectorCache
        unless (memberSourceCache hash cache) $ do
            let newCache = insertSourceCache hash collectorCache
            sdResult <- liftIO $ unMarquise $ queueSourceDictUpdate collectorSpoolFiles addr sd
            either (\e -> logWarnStr $ "Marquise error when queuing sd update: " ++ show e) return sdResult
            put (cS{collectorCache = newCache}, eS)
        pointResult <- liftIO $ unMarquise $ queueSimple collectorSpoolFiles addr ts payload
        either (\e -> logWarnStr $ "Marquise error when queuing simple point: " ++ show e) return pointResult
        return ()

parseCommonOpts :: Parser CommonOpts
parseCommonOpts = CommonOpts
    <$> (read <$> strOption
        (long "log-verbosity"
         <> short 'v'
         <> help "Verbosity level at which to write log output"))
    <*> strOption
        (long "marquise-namespace"
         <> short 'n'
         <> value "perfdata"
         <> metavar "MARQUISE-NAMESPACE"
         <> help "Marquise namespace to write to. Must be unique on a host basis.")
    <*> strOption
        (long "cache-file"
         <> short 'c'
         <> value "/var/cache/marquise-cache/"
         <> metavar "CACHE-FILE"
         <> help "Filepath to cache file to use. Created if non-existant.")

--- | Writes out the final state of the cache to the hash file
writeCache :: (MonadIO m, CollectorMonad o s m) => Collector o s m ()
writeCache = do
    (CommonOpts{..}, _)  <- ask
    (CommonState{..}, _) <- get
    liftIO $ withFile optCacheFile WriteMode (`BS.hPut` toWire collectorCache)

setSpoolFiles :: (MonadIO m, CollectorMonad o s m) => Collector o s m ()
setSpoolFiles = do
    (CommonOpts{..}, _) <- ask
    (cState@CommonState{..}, eState) <- get
    files <- liftIO $ unMarquise $ createSpoolFiles optNamespace
    case files of
        Left e -> do
            logErrorStr $ "Error creating spool files: " ++ show e
            error ""
        Right files' -> put (cState{collectorSpoolFiles = files'}, eState)

--- | Attempts to generate an initial cache of SourceDicts from the given file path
--- If the file does not exist, or is improperly formatted returns an empty cache
setInitialCache :: (MonadIO m, CollectorMonad o s m) => Collector o s m ()
setInitialCache = do
    (CommonOpts{..}, _) <- ask
    (cState, eState) <- get
    h <- liftIO $ openFile optCacheFile ReadWriteMode
    logDebugStr "Reading cache file"
    contents <- liftIO $ BS.hGetContents h
    let result = fromWire contents
    logDebugStr "Decoding cache file"
    cache <- case result of
        Left e -> do
            logWarnStr $ concat ["Error decoding hash file: ", show e, " Continuing with empty initial cache"]
            return emptySourceCache
        Right cache' ->
            return cache'
    liftIO $ hClose h
    put (cState{collectorCache = cache}, eState)
