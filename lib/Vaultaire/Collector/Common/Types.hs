{-# LANGUAGE
    GeneralizedNewtypeDeriving
  , StandaloneDeriving
  #-}

module Vaultaire.Collector.Common.Types where

import           Control.Applicative
import           Control.Monad.Reader
import           Control.Monad.State
import           System.Log.Logger

import           Marquise.Client
import           Vaultaire.Types

data CommonOpts = CommonOpts
  { optLogLevel  :: Priority
  , optNamespace :: String
  }

data CommonState = CommonState
  { collectorSpoolFiles   :: SpoolFiles
  , collectorCache        :: SourceDictCache
  }

type CollectorOpts o = (CommonOpts, o)

type CollectorState s = (CommonState, s)

newtype Collector o s m a = Collector {
    unCollector :: ReaderT (CollectorOpts o) (StateT (CollectorState s) m) a
} deriving (Functor, Applicative, Monad, MonadReader (CollectorOpts o), MonadState (CollectorState s))

deriving instance MonadIO m => MonadIO (Collector o s m)

instance MonadTrans (Collector o s) where
    lift act = Collector $ lift $ lift act
