{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}

module Vaultaire.Collector.Common.Process
    ( runBaseCollector
    , runCollector
    , collectSource
    , collectSimple
    ) where

import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Options.Applicative
import           System.Log.Logger

import           Marquise.Client
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
    (cOpts, eOpts) <- liftIO $
        execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts) fullDesc)
    liftIO $ setupLogger (optLogLevel cOpts)
    let opts = (cOpts, eOpts)
    cState <- getInitialCommonState cOpts
    eState <- initialiseExtraState opts
    evalStateT (runReaderT (unCollector collect') opts) (cState, eState)
  where
    collect' = do
        result <- collect
        cleanup
        return result

-- | Sets the global logger to the given priority
setupLogger :: Priority -> IO ()
setupLogger level = do
    rLogger <- getRootLogger
    let rLogger' = setLevel level rLogger
    saveGlobalLogger rLogger'

-- | Generates a new set of spool files and an empty SourceDictCache
getInitialCommonState :: MonadIO m
                      => CommonOpts
                      -> m CommonState
getInitialCommonState CommonOpts{..} = do
    files <- liftIO $ withMarquiseHandler
        (\e -> error $ "Error creating spool files: " ++ show e)
        (createSpoolFiles optNamespace)
    return $ CommonState files emptySourceCache

-- | Wrapped Marquise.Client.queueSourceUpdate with logging and caching
collectSource :: MonadIO m => Address -> SourceDict -> Collector o s m ()
collectSource addr sd = do
    (cS@CommonState{..}, eS) <- get
    let hash = hashSource sd
    let cache = collectorCache
    unless (memberSourceCache hash cache) $ do
        let newCache = insertSourceCache hash collectorCache
        liftIO $ withMarquiseHandler handler
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
