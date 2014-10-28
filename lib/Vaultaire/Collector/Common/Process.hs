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

import           Marquise.Client
import           Vaultaire.Types

import           Vaultaire.Collector.Common.Types

runCollector :: MonadIO m
             => m (Parser o, String, String)
             -> (CollectorOpts o -> m s)
             -> Collector o s m FourTuple
             -> m ()
runCollector parseExtraOpts initialiseExtraState collectFourTuple = do
    (parseExtraOpts', desc, hdr) <- parseExtraOpts
    (cOpts, eOpts) <- liftIO $ execParser (info (liftA2 (,) parseCommonOpts parseExtraOpts') (fullDesc <> progDesc desc <> header hdr))
    let opts = (cOpts, eOpts)
    eState <- initialiseExtraState opts
    cState <- getInitialCommonState cOpts
    evalStateT (runReaderT (unCollector act) opts) (cState, eState)
  where
    getInitialCommonState CommonOpts{..} = do
        files <- liftIO $ unMarquise $ createSpoolFiles optNamespace
        case files of
            Left e -> error $ "Error creating spool files: " ++ show e
            Right files' -> return $ CommonState files' emptySourceCache
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
