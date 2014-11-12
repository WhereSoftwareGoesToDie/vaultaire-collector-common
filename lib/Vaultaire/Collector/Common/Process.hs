{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}

module Vaultaire.Collector.Common.Process where

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

runBaseCollector :: MonadIO m
                 => Collector () () m a
                 -> m a
runBaseCollector = runCollector (pure ()) (\_ -> return ()) (return ())

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
        let name = SpoolName optNamespace
        return $ CommonState name files emptySourceCache 0 0

maybeRotatePointsFile :: MonadIO m => Collector o s m ()
maybeRotatePointsFile = do
    (cS@CommonState{..}, eS) <- get
    (CommonOpts{..}, _) <- ask
    when (pointsBytesWritten > optRotateThreshold) $ do
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
        newFile <- liftIO $ newRandomContentsSpoolFile collectorSpoolName
        let (SpoolFiles pFile _) = collectorSpoolFiles
        let newSpools = SpoolFiles pFile newFile
        put (cS{ collectorSpoolFiles  = newSpools
               , contentsBytesWritten = 0}, eS)

collectSource :: MonadIO m => Address -> SourceDict -> Collector o s m ()
collectSource addr sd = do
    (cS@CommonState{..}, eS) <- get
    let hash = hashSource sd
    let cache = collectorCache
    unless (memberSourceCache hash cache) $ do
        res <- liftIO $ runMarquise $
            queueSourceDictUpdate collectorSpoolFiles addr sd
        case res of
            Left e  -> liftIO $ warningM "Process.collectSource" $
                "Marquise error when queuing sd update: " ++ show e
            Right _ -> do
                liftIO $ debugM "Process.collectSource" $
                    concat ["Queued sd ", show sd, " to addr ", show addr]
                let newCache = insertSourceCache hash collectorCache
                let newLen = contentsBytesWritten + 16 + fromIntegral (BS.length (toWire sd))
                put (cS{ collectorCache = newCache
                       , contentsBytesWritten = newLen}, eS)
                maybeRotateContentsFile

collectSimple :: MonadIO m => SimplePoint -> Collector o s m ()
collectSimple (SimplePoint addr ts payload) = do
    (cS@CommonState{..}, eS) <- get
    res <- liftIO $ runMarquise $
        queueSimple collectorSpoolFiles addr ts payload
    case res of
        Left e -> liftIO $ warningM "Process.collectSimple" $
            "Marquise error when queuing simple point: " ++ show e
        Right _ -> do
            liftIO $ debugM "Process.handleSimple" $
                concat ["Queued simple point ", show addr, ", ", show ts, ", ", show payload]
            let newLen = pointsBytesWritten + 24
            put (cS{ pointsBytesWritten = newLen}, eS)
            maybeRotatePointsFile

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
        (long "max-spool-size"
         <> value (1024*1024)
         <> metavar "MAX-SPOOL-SIZE"
         <> help "Maximum spool file size before rotation")
