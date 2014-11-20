{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}

module Vaultaire.Collector.Common.Process
    ( runBaseCollector
    , runCollector
    , runCollectorN
    , runNullCollector
    , collectSource
    , collectSimple
    ) where

import           Control.Concurrent.Async
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Options.Applicative
import           System.Log.Logger

import           Marquise.Client
import           Marquise.Types
import           Vaultaire.Types

import           Vaultaire.Collector.Common.Types

-- | Run a Vaultaire Collector with no extra state or options
runBaseCollector :: MonadIO m
                 => Collector () () m a
                 -> m a
runBaseCollector = runCollector
                   (pure ())
                   (\_ -> return ())
                   (return ())

-- | Run a Vaultaire Collector given an extra options parser, state setup
--   function, cleanup function and collector action
runCollector :: MonadIO m
             => Parser o
             -> (CollectorOpts o -> m s)
             -> Collector o s m ()
             -> Collector o s m a
             -> m a
runCollector parseExtraOpts initialiseExtraState cleanup collect = do
    (opts, st) <- setup parseExtraOpts initialiseExtraState getInitialCommonState
    runCollector' opts st cleanup collect

-- | Run a Vaultaire Collector which outputs to /dev/null
--   Suitable for testing
runNullCollector :: MonadIO m
                 => Parser o
                 -> (CollectorOpts o -> m s)
                 -> Collector o s m ()
                 -> Collector o s m a
                 -> m a
runNullCollector parseExtraOpts initialiseExtraState cleanup collect = do
    (opts, st) <- setup parseExtraOpts initialiseExtraState getNullCommonState
    runCollector' opts st cleanup collect

-- | Run several concurrent Vaultaire Collector with the same options
runCollectorN :: Parser o
              -> (CollectorOpts o -> IO s)
              -> Collector o s IO ()
              -> Collector o s IO a
              -> IO a
runCollectorN parseExtraOpts initialiseExtraState cleanup collect = do
    (cOpts@CommonOpts{..}, eOpts) <- liftIO $ execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts) fullDesc)
    liftIO $ setupLogger optLogLevel optContinueOnError
    let opts = (cOpts, eOpts)
    result <- waitAny =<< replicateM optNumThreads ( do
        cState <- getInitialCommonState cOpts
        eState <- initialiseExtraState opts
        async $ runCollector' opts (cState, eState) cleanup collect)
    return $ snd result

-- | Helper run function
runCollector' :: Monad m
              => CollectorOpts o
              -> CollectorState s
              -> Collector o s m ()
              -> Collector o s m a
              -> m a
runCollector' opts st cleanup collect =
    let collect' = unCollector $ do
            result <- collect
            cleanup
            return result
    in evalStateT (runReaderT collect' opts) st

-- | Helper function to setup initial state of the collector
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

-- | Sets the global logger to the given priority
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

-- | Generates a new set of spool files and an empty SourceDictCache
getInitialCommonState :: MonadIO m
                      => CommonOpts
                      -> m CommonState
getInitialCommonState CommonOpts{..} = do
    files <- liftIO $ withMarquiseHandler
        (\e -> error $ "Error creating spool files: " ++ show e)
        (createSpoolFiles optNamespace)
    return $ CommonState files emptySourceCache

-- | Generates a dummy set of spool files and an empty SourceDictCache
getNullCommonState :: MonadIO m
                    => CommonOpts
                    -> m CommonState
getNullCommonState CommonOpts{..} =
    return $ CommonState (SpoolFiles "/dev/null" "/dev/null") emptySourceCache

-- | Wrapped Marquise.Client.queueSourceUpdate with logging and caching
collectSource :: MonadIO m => Address -> SourceDict -> Collector o s m ()
collectSource addr sd = do
    (cS@CommonState{..}, eS) <- get
    let hash = hashSource sd
    let cache = collectorCache
    unless (memberSourceCache hash cache) $ do
        let newCache = insertSourceCache hash collectorCache
        liftIO $ withMarquiseHandler handler $ do
            queueSourceDictUpdate collectorSpoolFiles addr sd
            lift $ debugM "Process.handleSource" $
                   concat ["Queued sd ", show sd, " to addr ", show addr]
        put (cS{collectorCache = newCache}, eS)
  where
    handler e = warningM
                "Process.handleSource" $
                "Marquise error when queuing sd update: " ++ show e
-- | Wrapped Marquise.Client.queueSimple with logging
collectSimple :: MonadIO m => SimplePoint -> Collector o s m ()
collectSimple (SimplePoint addr ts payload) = do
    (CommonState{..}, _) <- get
    liftIO $ withMarquiseHandler handler $ do
        queueSimple collectorSpoolFiles addr ts payload
        lift $ debugM "Process.handleSimple" $
               concat [ "Queued simple point "
                      , show addr, ", "
                      , show ts,   ", "
                      , show payload]
  where
    handler e = warningM
                "Process.handleSimple" $
                "Marquise error when queuing simple point: " ++ show e

-- | Parses the common options for Vaultaire collectors
parseCommonOpts :: Parser CommonOpts
parseCommonOpts = CommonOpts
    <$> flag WARNING DEBUG
        (long "verbose"
         <> short 'v'
         <> help "Run in verbose mode")
    <*> strOption
        (long "marquise-namespace"
         <> short 'm'
         <> value "perfdata"
         <> metavar "MARQUISE-NAMESPACE"
         <> help "Marquise namespace to write to. Must be unique on a per-host basis.")
    <*> option auto
        (long "num-threads"
         <> short 'j'
         <> value 1
         <> metavar "NUM-THREADS"
         <> help "The number of collectors to run concurrently")
    <*> switch
        (long "continue-on-error"
         <> help "Continue execution when logging an error or more severe message")
