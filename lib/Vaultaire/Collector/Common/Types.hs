{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Vaultaire.Collector.Common.Types where

import           Control.Applicative
import           Control.Monad.Catch
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Word
import           System.Log.Handler
import           System.Log.Logger

import           Marquise.Client
import           Vaultaire.Types

data CommonOpts = CommonOpts
  { optLogLevel   :: Priority
  , optNamespace  :: String
  , optNumThreads :: Int
  , optRotateThreshold :: Word64
  , optContinueOnError :: Bool
  }

data CommonState = CommonState
  { collectorSpoolName   :: SpoolName
  , collectorSpoolFiles  :: SpoolFiles
  , collectorCache       :: SourceDictCache
  , pointsBytesWritten   :: Word64
  , contentsBytesWritten :: Word64
  }

type CollectorOpts o = (CommonOpts, o)

type CollectorState s = (CommonState, s)

newtype Collector o s m a = Collector {
    unCollector :: ReaderT (CollectorOpts o) (StateT (CollectorState s) m) a
} deriving (Functor, Applicative, Monad, MonadReader (CollectorOpts o), MonadState (CollectorState s), MonadThrow, MonadCatch)

instance MonadTrans (Collector o s) where
    lift act = Collector $ lift $ lift act

deriving instance MonadIO m => MonadIO (Collector o s m)

-- |Stub handler. All it does is explode on an error or more severe message
data CrashLogHandler = CrashLogHandler

instance LogHandler CrashLogHandler where
    setLevel x _ = x
    getLevel _ = ERROR
    handle _ (p, m) _  = when (p >= ERROR) (error m)
    emit _ _ _ = return ()
    close _ = return ()
