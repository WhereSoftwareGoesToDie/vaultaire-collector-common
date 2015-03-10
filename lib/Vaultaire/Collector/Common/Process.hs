{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}

module Vaultaire.Collector.Common.Process
    ( runBaseCollector
    , runCollector
    , runCollector'
    , runCollectorN
    , runNullCollector
    , getInitialCommonState
    , getNullCommonState
    , collectSource
    , collectSimple
    , collectExtended
    ) where

import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import qualified Data.ByteString                  as BS
import           Options.Applicative
import           System.Log.Logger

import           Marquise.Client
import           Marquise.Types
import           Vaultaire.Types

import           Vaultaire.Collector.Common.Types

-- | Run a Vaultaire Collector with no extra state or options.
runBaseCollector :: MonadIO m
                 => Collector () () m a
                 -> m a
runBaseCollector = runCollector
                   (pure ())
                   (\_ -> return ())
                   (return ())

-- | Run a Vaultaire Collector given an extra options parser, state setup
--   function, cleanup function and collector action.
runCollector :: MonadIO m
             => Parser o
             -> (CollectorOpts o -> m s)
             -> Collector o s m ()
             -> Collector o s m a
             -> m a
runCollector parseExtraOpts initialiseExtraState cleanup collect = do
    (opts, st) <- setup parseExtraOpts initialiseExtraState getInitialCommonState
    liftM fst $ runCollector' opts st cleanup collect

-- | Run a Vaultaire Collector which outputs to /dev/null
--   Does no options parsing and requires a fully evalutated set of options
--   Suitable for testing.
runNullCollector :: MonadIO m
                 => CollectorOpts o
                 -> (CollectorOpts o -> m s)
                 -> Collector o s m ()
                 -> Collector o s m a
                 -> m a
runNullCollector opts@(cOpts, _) initialiseExtraState cleanup collect = do
    cState <- getNullCommonState cOpts
    eState <- initialiseExtraState opts
    liftM fst $ runCollector' opts (cState, eState) cleanup collect

-- | Run several concurrent Vaultaire Collector with the same options.
runCollectorN :: Parser o
              -> (CollectorOpts o -> IO s)
              -> Collector o s IO ()
              -> Collector o s IO a
              -> IO a
runCollectorN parseExtraOpts initialiseExtraState cleanup collect = do
    (cOpts@CommonOpts{..}, eOpts) <- liftIO $
        execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts) fullDesc)
    liftIO $ setupLogger optLogLevel optContinueOnError
    let opts = (cOpts, eOpts)
    result <- waitAny =<< replicateM optNumThreads ( do
        cState <- getInitialCommonState cOpts
        eState <- initialiseExtraState opts
        act <- async $ runCollector' opts (cState, eState) cleanup collect
        link act
        return act)
    return $ fst $ snd result

-- | Runs a collector directly from options and state rather than through
--   an options parser and state initialisation function. Also returns
--   the final state of the collector
runCollector' :: Monad m
              => CollectorOpts o
              -> CollectorState s
              -> Collector o s m ()
              -> Collector o s m a
              -> m (a, CollectorState s)
runCollector' opts st cleanup collect =
    let collect' = unCollector $ do
            result <- collect
            cleanup
            return result
    in runStateT (runReaderT collect' opts) st

-- | Helper function to setup initial state of the collector.
setup :: MonadIO m
      => Parser o
      -> (CollectorOpts o -> m s)
      -> (CommonOpts -> m CommonState)
      -> m (CollectorOpts o, CollectorState s)
setup parseExtraOpts initialiseExtraState initialiseCommonState = do
    opts@(cOpts, _) <- liftIO $
        execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts) fullDesc)
    liftIO $ setupLogger (optLogLevel cOpts) (optContinueOnError cOpts)
    cState <- initialiseCommonState cOpts
    eState <- initialiseExtraState opts
    return (opts, (cState, eState))

maybeRotatePointsFile :: MonadIO m => Collector o s m ()
maybeRotatePointsFile = do
    (cS@CommonState{..}, eS) <- get
    (CommonOpts{..}, _) <- ask
    when (pointsBytesWritten > optRotateThreshold) $ do
        liftIO $ infoM "Process.maybeRotatePointsFile"
            "Rotating points spool file"
        newFile <- liftIO $ newRandomPointsSpoolFile collectorSpoolName
        let (SpoolFiles _ cFile) = collectorSpoolFiles
        let newSpools = SpoolFiles newFile cFile
        put (cS{ collectorSpoolFiles = newSpools
               , pointsBytesWritten  = 0}, eS)

maybeRotateContentsFile :: MonadIO m => Collector o s m ()
maybeRotateContentsFile = do
    (cS@CommonState{..}, eS) <- get
    (CommonOpts{..}, _) <- ask
    when (contentsBytesWritten > optRotateThreshold) $ do
        liftIO $ infoM "Process.maybeRotateContentsFile"
            "Rotating contents spool file"
        newFile <- liftIO $ newRandomContentsSpoolFile collectorSpoolName
        let (SpoolFiles pFile _) = collectorSpoolFiles
        let newSpools = SpoolFiles pFile newFile
        put (cS{ collectorSpoolFiles  = newSpools
               , contentsBytesWritten = 0}, eS)

-- | Sets the global logger to the given priority, and specify whether
--   we want to halt and catch fire on any error.
setupLogger :: Priority -> Bool -> IO ()
setupLogger level continueOnError = do
    rLogger <- getRootLogger
    let rLogger' = maybeAddCrashHandler $ setLevel level rLogger
    saveGlobalLogger rLogger'
  where
    maybeAddCrashHandler logger =
        if continueOnError then
            logger
        else
            addHandler CrashLogHandler logger

-- | Generates a new set of spool files and an empty SourceDictCache.
getInitialCommonState :: MonadIO m
                      => CommonOpts
                      -> m CommonState
getInitialCommonState CommonOpts{..} = do
    files <- liftIO $ handle
        (\(MarquiseException e) -> error $ "Error creating spool files: " ++ e) $
        createSpoolFiles optNamespace
    let name = SpoolName optNamespace
    return $ CommonState name files emptySourceCache 0 0

-- | Generates a dummy set of spool files and an empty SourceDictCache.
getNullCommonState :: MonadIO m
                    => CommonOpts
                    -> m CommonState
getNullCommonState CommonOpts{..} =
    return $ CommonState (SpoolName "") (SpoolFiles "/dev/null" "/dev/null") emptySourceCache 0 0

-- | Wrapped Marquise.Client.queueSourceUpdate with logging and caching.
collectSource :: MonadIO m => Address -> SourceDict -> Collector o s m ()
collectSource addr sd = do
    (cS@CommonState{..}, eS) <- get
    let hash = hashSource sd
    let cache = collectorCache
    unless (memberSourceCache hash cache) $ do
        res <- liftIO $ try $
            queueSourceDictUpdate collectorSpoolFiles addr sd
        case res of
            Left (MarquiseException e) -> liftIO . errorM "Process.collectSource" $
                "Marquise error when queuing sd update: " ++ e
            Right _ -> do
                liftIO $ debugM "Process.collectSource" $
                    concat ["Queued sd ", show sd, " to addr ", show addr]
                let newCache = insertSourceCache hash collectorCache
                let newLen = contentsBytesWritten + 16
                           + fromIntegral (BS.length (toWire sd))
                put (cS{ collectorCache = newCache
                       , contentsBytesWritten = newLen}, eS)
                maybeRotateContentsFile

-- | Wrapped Marquise.Client.queueSimple with logging.
collectSimple :: MonadIO m => SimplePoint -> Collector o s m ()
collectSimple (SimplePoint addr ts payload) = do
    (cS@CommonState{..}, eS) <- get
    res <- liftIO $ try $
        queueSimple collectorSpoolFiles addr ts payload
    case res of
        Left (MarquiseException e) -> liftIO . errorM "Process.collectSimple" $
            "Marquise error when queuing simple point: " ++ e
        Right _ -> do
            liftIO $ debugM "Process.collectSimple" $
                concat ["Queued simple point "
                       , show addr, ", "
                       , show ts, ", "
                       , show payload]
            let newLen = pointsBytesWritten + 24
            put (cS{ pointsBytesWritten = newLen}, eS)
            maybeRotatePointsFile

-- | Wrapped Marquise.Client.queueExtended with logging.
collectExtended :: MonadIO m => ExtendedPoint -> Collector o s m ()
collectExtended (ExtendedPoint addr ts payload) = do
    (cS@CommonState{..}, eS) <- get
    res <- liftIO $ try $
        queueExtended collectorSpoolFiles addr ts payload
    case res of
        Left (MarquiseException e) -> liftIO . errorM "Process.collectExtended" $
            "Marquise error when queuing extended point: " ++ e
        Right _ -> do
            liftIO . debugM "Process.collectExtended" $
                concat ["Queued extended point "
                       , show addr, ", "
                       , show ts, ", "
                       , show payload]
            let payloadLen = fromIntegral $ BS.length payload
            -- An ExtendedPoint has 4 components:
            -- Address:   8 bytes
            -- Timestamp: 8 bytes
            -- PayloadLength: 8 bytes
            -- Payload: PayloadLength bytes
            -- For a total length of 24 + PayloadLength bytes
            let newLen = pointsBytesWritten + 24 + payloadLen
            put (cS{ pointsBytesWritten = newLen}, eS)
            maybeRotatePointsFile

-- | Parses the common options for Vaultaire collectors.
parseCommonOpts :: Parser CommonOpts
parseCommonOpts = CommonOpts
    <$> flag WARNING DEBUG
        (long "verbose"
         <> short 'v'
         <> help "Run in verbose mode")
    <*> strOption
        (long "marquise-namespace"
         <> short 'm'
         <> metavar "MARQUISE-NAMESPACE"
         <> help "Marquise namespace to write to. Must be unique on a per-host basis.")
    <*> option auto
        (long "num-threads"
         <> short 'j'
         <> value 1
         <> metavar "NUM-THREADS"
         <> help "The number of collectors to run concurrently")
    <*> option auto
        (long "max-spool-size"
         <> value (1024*1024)
         <> metavar "MAX-SPOOL-SIZE"
         <> help "Maximum spool file size before rotation")
    <*> switch
        (long "continue-on-error"
         <> help "Continue execution when logging an error or more severe message")
