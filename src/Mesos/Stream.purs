module Mesos.Stream where

import Prelude
import Control.Monad.Aff (Aff, makeAff)
import Control.Monad.Eff.Exception (EXCEPTION)
import Control.Monad.Eff.Unsafe (unsafeCoerceEff)
import Control.Monad.ST (modifySTRef, newSTRef, readSTRef)
import Node.Encoding (Encoding)
import Node.Stream (Readable, onDataString, onEnd, onError)

readFullString :: forall w eff. Readable w (err :: EXCEPTION | eff) -> Encoding -> Aff (err :: EXCEPTION | eff) String
readFullString stream encoding = makeAff \onE onS -> do
    onError stream onE
    buf <- unsafeCoerceEff $ newSTRef ""
    onDataString stream encoding \s -> do
        unsafeCoerceEff (modifySTRef buf $ flip (<>) s)
        pure unit
    onEnd stream $ unsafeCoerceEff (readSTRef buf) >>= onS
