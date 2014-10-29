{-# LANGUAGE
    MultiParamTypeClasses
  , RecordWildCards
  #-}

module Vaultaire.Collector.Common.Process where

import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Options.Applicative
import           Pipes
import           System.Log.Logger

import           Marquise.Client
import           Vaultaire.Types

import           Vaultaire.Collector.Common.Types

runBaseCollector :: MonadIO m
                 => Producer (Address, Either SourceDict SimplePoint) (Collector () () m) ()
                 -> m ()
runBaseCollector = runCollector () (\_ -> return ())

runCollector :: MonadIO m
             => o
             -> (CollectorOpts o -> m s)
             -> Producer (Address, Either SourceDict SimplePoint) (Collector o s m) ()
             -> m ()
runCollector eOpts initialiseExtraState collect = do
    cOpts <- liftIO $ execParser (info parseCommonOpts fullDesc)
    let opts = (cOpts, eOpts)
    eState <- initialiseExtraState opts
    cState <- getInitialCommonState cOpts
    liftIO $ setupLogger (optLogLevel cOpts)
    evalStateT (runReaderT (unCollector $ act collect) opts) (cState, eState)
  where
    setupLogger level = do
        rLogger <- getRootLogger
        let rLogger' = setLevel level rLogger
        saveGlobalLogger rLogger'
    getInitialCommonState CommonOpts{..} = do
        files <- liftIO $ withMarquiseHandler (\e -> error $ "Error creating spool files: " ++ show e) $
            createSpoolFiles optNamespace
        return $ CommonState files emptySourceCache
    act :: MonadIO m => Producer (Address, Either SourceDict SimplePoint) (Collector o s m) () -> Collector o s m ()
    act prod = do
        result <- next prod
        case result of
            Left _ -> return ()
            Right ((addr, item), prod') -> do
                case item of
                    Left sd -> handleSource addr sd
                    Right p -> handleSimple addr p
                act prod'
    handleSource :: MonadIO m => Address -> SourceDict -> Collector o s m ()
    handleSource addr sd = do
        (cS@CommonState{..}, eS) <- get
        let hash = hashSource sd
        let cache = collectorCache
        unless (memberSourceCache hash cache) $ do
            let newCache = insertSourceCache hash collectorCache
            liftIO $ withMarquiseHandler (\e -> warningM "Process.handleSource" $  "Marquise error when queuing sd update: " ++ show e) $ do
                queueSourceDictUpdate collectorSpoolFiles addr sd
                lift $ debugM "Process.handleSource" $ concat ["Queued sd ", show sd, " to addr ", show addr]
            put (cS{collectorCache = newCache}, eS)
    handleSimple :: MonadIO m => Address -> SimplePoint -> Collector o s m ()
    handleSimple addr (SimplePoint _ ts payload) = do
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
         <> short 'n'
         <> value "perfdata"
         <> metavar "MARQUISE-NAMESPACE"
         <> help "Marquise namespace to write to. Must be unique on a per-host basis.")
