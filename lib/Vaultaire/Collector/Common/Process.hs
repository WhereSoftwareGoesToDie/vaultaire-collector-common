{-# LANGUAGE
    MultiParamTypeClasses
  , RecordWildCards
  #-}

module Vaultaire.Collector.Common.Process where

import           Control.Concurrent.Async
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Options.Applicative
import           System.Log.Logger

import           Marquise.Client
import           Vaultaire.Types

import           Vaultaire.Collector.Common.Types

runBaseCollector :: MonadIO m
                 => Collector () () m a
                 -> m a
runBaseCollector = runCollector (pure ()) (\_ -> return ()) (return ())

runCollectorN :: Parser o
              -> (CollectorOpts o -> IO s)
              -> Collector o s IO ()
              -> Collector o s IO a
              -> IO a
runCollectorN parseExtraOpts initialiseExtraState cleanup collect = do
    (cOpts@CommonOpts{..}, eOpts) <- liftIO $ execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts) fullDesc)
    liftIO $ setupLogger optLogLevel
    let opts = (cOpts, eOpts)
    result <- waitAny =<< (replicateM optNumThreads $ do
        cState <- getInitialCommonState cOpts
        eState <- initialiseExtraState opts
        (async $ evalStateT (runReaderT (unCollector collect') opts) (cState, eState)))
    return $ snd result
  where
    collect' = do
        result <- collect
        cleanup
        return result
    setupLogger level = do
        rLogger <- getRootLogger
        let rLogger' = setLevel level rLogger
        saveGlobalLogger rLogger'
    getInitialCommonState CommonOpts{..} = do
        files <- liftIO $ withMarquiseHandler (\e -> error $ "Error creating spool files: " ++ show e) $
            createSpoolFiles optNamespace
        return $ CommonState files emptySourceCache

runCollector :: MonadIO m
             => Parser o
             -> (CollectorOpts o -> m s)
             -> Collector o s m ()
             -> Collector o s m a
             -> m a
runCollector parseExtraOpts initialiseExtraState cleanup collect = do
    (cOpts, eOpts) <- liftIO $ execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts) fullDesc)
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
    setupLogger level = do
        rLogger <- getRootLogger
        let rLogger' = setLevel level rLogger
        saveGlobalLogger rLogger'
    getInitialCommonState CommonOpts{..} = do
        files <- liftIO $ withMarquiseHandler (\e -> error $ "Error creating spool files: " ++ show e) $
            createSpoolFiles optNamespace
        return $ CommonState files emptySourceCache

collectSource :: MonadIO m => Address -> SourceDict -> Collector o s m ()
collectSource addr sd = do
    (cS@CommonState{..}, eS) <- get
    let hash = hashSource sd
    let cache = collectorCache
    unless (memberSourceCache hash cache) $ do
        let newCache = insertSourceCache hash collectorCache
        liftIO $ withMarquiseHandler (\e -> warningM "Process.handleSource" $  "Marquise error when queuing sd update: " ++ show e) $ do
            queueSourceDictUpdate collectorSpoolFiles addr sd
            lift $ debugM "Process.handleSource" $ concat ["Queued sd ", show sd, " to addr ", show addr]
        put (cS{collectorCache = newCache}, eS)

collectSimple :: MonadIO m => SimplePoint -> Collector o s m ()
collectSimple (SimplePoint addr ts payload) = do
    (CommonState{..}, _) <- get
    liftIO $ withMarquiseHandler (\e -> warningM "Process.handleSimple" $ "Marquise error when queuing simple point: " ++ show e) $ do
        queueSimple collectorSpoolFiles addr ts payload
        lift $ debugM "Process.handleSimple" $ concat ["Queued simple point ", show addr, ", ", show ts, ", ", show payload]

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
