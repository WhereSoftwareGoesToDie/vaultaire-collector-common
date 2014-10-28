{-# LANGUAGE
    MultiParamTypeClasses
  , RecordWildCards
  #-}

module Vaultaire.Collector.Common.Process where

import           Control.Monad
import           Control.Monad.Logger
import           Control.Monad.Reader
import           Control.Monad.State
import           Options.Applicative
import           Pipes

import           Marquise.Client
import           Vaultaire.Types

import           Vaultaire.Collector.Common.Types

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
    evalStateT (runReaderT (unCollector $ act collect) opts) (cState, eState)
  where
    getInitialCommonState CommonOpts{..} = do
        files <- liftIO $ unMarquise $ createSpoolFiles optNamespace
        case files of
            Left e -> error $ "Error creating spool files: " ++ show e
            Right files' -> return $ CommonState files' emptySourceCache
    act prod = do
        result <- next prod
        case result of
            Left _ -> return ()
            Right ((addr, item), prod') -> do
                case item of
                    Left sd -> handleSource addr sd
                    Right p -> handleSimple addr p
                act prod'
    handleSource addr sd = do
        (cS@CommonState{..}, eS) <- get
        let hash = hashSource sd
        let cache = collectorCache
        unless (memberSourceCache hash cache) $ do
            let newCache = insertSourceCache hash collectorCache
            sdResult <- liftIO $ unMarquise $ queueSourceDictUpdate collectorSpoolFiles addr sd
            either (\e -> logWarnStr $ "Marquise error when queuing sd update: " ++ show e) return sdResult
            put (cS{collectorCache = newCache}, eS)
    handleSimple addr (SimplePoint _ ts payload) = do
        (CommonState{..}, _) <- get
        pointResult <- liftIO $ unMarquise $ queueSimple collectorSpoolFiles addr ts payload
        either (\e -> logWarnStr $ "Marquise error when queuing simple point: " ++ show e) return pointResult

parseCommonOpts :: Parser CommonOpts
parseCommonOpts = CommonOpts
    <$> flag LevelWarn LevelDebug
        (long "verbose"
         <> short 'v'
         <> help "Run in verbose mode")
    <*> strOption
        (long "marquise-namespace"
         <> short 'n'
         <> value "perfdata"
         <> metavar "MARQUISE-NAMESPACE"
         <> help "Marquise namespace to write to. Must be unique on a per-host basis.")
