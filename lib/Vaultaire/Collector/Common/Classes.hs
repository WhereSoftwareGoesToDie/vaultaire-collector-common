{-# LANGUAGE
    MultiParamTypeClasses
  , FunctionalDependencies
  #-}

module Vaultaire.Collector.Common.Classes where

import           Options.Applicative

import           Vaultaire.Collector.Common.Types

class Monad m => CollectorMonad o s m | m -> o, m -> s where
    --Give us a way to parse extra options, a program description and a header
    parseExtraOpts       :: m (Parser o, String, String)
    initialiseExtraState :: CollectorOpts o -> m s
    collectFourTuple     :: Collector o s m FourTuple
