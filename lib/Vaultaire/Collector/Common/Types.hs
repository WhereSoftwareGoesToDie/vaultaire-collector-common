{-# LANGUAGE
    GeneralizedNewtypeDeriving
  , RecordWildCards
  , StandaloneDeriving
  #-}

module Vaultaire.Collector.Common.Types where

import           Control.Applicative
import           Control.Monad.Logger
import           Control.Monad.Reader
import           Control.Monad.State
import qualified Data.ByteString.Char8 as S(hPutStrLn)
import           Data.Monoid
import qualified Data.Text as T
import           Data.Word
import           System.IO
import           System.Log.FastLogger

import           Marquise.Client
import           Vaultaire.Types

data CommonOpts = CommonOpts
  { optLogLevel  :: LogLevel
  , optNamespace :: String
  , optCacheFile :: String
  }

data CommonState = CommonState
  { collectorSpoolFiles   :: SpoolFiles
  , collectorCache        :: SourceDictCache
  }

type CollectorOpts o = (CommonOpts, o)

type CollectorState s = (CommonState, s)

type FourTuple = (Address, SourceDict, TimeStamp, Word64)

newtype Collector o s m a = Collector {
    unCollector :: ReaderT (CollectorOpts o) (StateT (CollectorState s) m) a
} deriving (Functor, Applicative, Monad, MonadReader (CollectorOpts o), MonadState (CollectorState s))

deriving instance MonadIO m => MonadIO (Collector o s m)

instance MonadTrans (Collector o s) where
    lift act = Collector $ lift $ lift act

instance MonadIO m => MonadLogger (Collector o s m) where
    monadLoggerLog _ _ level msg = do
        (CommonOpts{..}, _) <- ask
        when (level >= optLogLevel) $ liftIO $ do
            currTime <- getCurrentTimeNanoseconds
            let logPrefix = mconcat $ map toLogStr [showLevel level, " ",  show currTime, " "]
            let output = fromLogStr $ logPrefix <> toLogStr msg
            S.hPutStrLn stdout output
            when (level == LevelError) $ S.hPutStrLn stderr output
      where
        showLevel LevelDebug     = "[Debug]"
        showLevel LevelInfo      = "[Info]"
        showLevel LevelWarn      = "[Warning]"
        showLevel LevelError     = "[Error]"
        showLevel (LevelOther l) = concat ["[", show l, "]"]

logDebugStr   :: MonadLogger m => String -> m ()
logDebugStr   = logDebugN   . T.pack

logInfoStr    :: MonadLogger m => String -> m ()
logInfoStr    = logInfoN    . T.pack

logWarnStr    :: MonadLogger m => String -> m ()
logWarnStr    = logWarnN    . T.pack

logErrorStr   :: MonadLogger m => String -> m ()
logErrorStr   = logErrorN   . T.pack

logOtherStr   :: MonadLogger m => LogLevel -> String -> m ()
logOtherStr l = logOtherN l . T.pack
