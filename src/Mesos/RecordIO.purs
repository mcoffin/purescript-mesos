module Mesos.RecordIO 
    ( recordIO
    , onRecordIO
    ) where

import Prelude
import Control.Monad.Aff (Aff, makeAff, runAff)
import Control.Monad.Aff.AVar (AVAR, AffAVar, makeVar, putVar, takeVar)
import Control.Monad.Aff.Internal (AVar)
import Control.Monad.Aff.Unsafe (unsafeCoerceAff)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Class (liftEff)
import Control.Monad.Eff.Exception (EXCEPTION, Error, error)
import Control.Monad.Eff.Unsafe (unsafeCoerceEff)
import Control.Monad.Error.Class (throwError)
import Control.Monad.Rec.Class (Step(..), tailRecM, forever)
import Control.Monad.ST (newSTRef, readSTRef, writeSTRef)
import Control.Monad.State.Trans (StateT, runStateT, get, put)
import Control.Monad.Trans.Class (lift)
import Data.Array (unsafeIndex, index, head)
import Data.Int as I
import Data.Maybe (Maybe(..), maybe, fromJust, fromMaybe, fromMaybe')
import Data.String (Pattern(..), splitAt)
import Data.String as S
import Data.Tuple (Tuple(..))
import Node.Encoding as Encoding
import Node.Stream (Readable, onError, onDataString)
import Partial.Unsafe (unsafePartial)

-- | Data structure to hold the curent state of a `RecordIO` stream reader
data ReadState = ReadLength String
               | ReadData { length :: Int, buffer :: String }

-- | Initial state of a `RecordIO` reader
initialState :: ReadState
initialState = ReadLength ""

-- | Handle all `RecordIO` events that come across a readable stream
onRecordIO :: forall w eff. Readable w (avar :: AVAR, err :: EXCEPTION | eff)
           -> (Error -> Eff (avar :: AVAR, err :: EXCEPTION | eff) Unit)
           -> (String -> Eff (avar :: AVAR, err :: EXCEPTION | eff) Unit)
           -> Eff (avar :: AVAR, err :: EXCEPTION | eff) Unit
onRecordIO stream onE onS = do
    runAff onE (\_ -> pure unit) $ recordIO stream >>= handleVar
    pure unit
    where
        handleVar :: AVar String -> Aff (avar :: AVAR, err :: EXCEPTION | eff) Unit
        handleVar v = forever do
            message <- takeVar v
            liftEff $ onS message

-- | Returns a `RecordIO` formatted string as an AVar queue of the messages in the stream
recordIO :: forall w eff. Readable w (avar :: AVAR, err :: EXCEPTION | eff)
         -> AffAVar (err :: EXCEPTION | eff) (AVar String)
recordIO stream = makeVar >>= produceRecords where
    readRecord :: forall eff1. AVar String -> String -> StateT ReadState (Aff (avar :: AVAR | eff1)) Unit
    readRecord var = tailRecM \next -> do
        currentState <- get
        case Tuple currentState (S.indexOf (Pattern "\n") next) of
          Tuple (ReadLength s) Nothing -> do
              -- If we're reading a length and we have no newline, then we just append to the existing length buffer
              put <<< ReadLength $ s <> next 
              pure $ Done unit
          Tuple (ReadLength s) (Just idx) -> do
              -- If we're reading a length and we have a newline, parse the length buffer, Set state to readData, and set initial data buffer to remainder
              let nextA = fromMaybe' (\_ -> [next]) $ splitAt idx next
                  nextLen = unsafePartial fromJust <<< head $ nextA
                  nextNext = fromMaybe "" (S.drop 1 <$> index nextA 1)
                  lenBuf = s <> nextLen
              len <- lift (maybe (throwError <<< error $ "Length buffer couldn't be parsed as an Int: " <> lenBuf) pure $ I.fromString lenBuf)
              put <<< ReadData $ { length: len, buffer: "" }
              pure $ Loop nextNext
          Tuple (ReadData s) _ ->
              -- If we're reading data, proceed to the data reading stage
              readDataStage s next
        where
            byteLength :: String -> Int
            byteLength = flip Encoding.byteLength Encoding.UTF8
            readDataStage :: forall eff0. { length :: Int, buffer :: String } -> String -> StateT ReadState (Aff (avar :: AVAR | eff0)) (Step String Unit)
            readDataStage s next
              | byteLength s.buffer + byteLength next < s.length = do
                  -- If we don't have enough data to finish the message, buffer the data and terminate
                  put <<< ReadData $ s { buffer = s.buffer <> next }
                  pure $ Done unit
              | otherwise = do
                  -- If we have enough data to finish the message, read the finished data, push to the AVar
                  -- then reset to initial state and loop with any trailing data
                  let nextA = fromMaybe' (\_ -> [next]) $ splitAt (s.length - (byteLength s.buffer)) next
                      dat = s.buffer <> unsafePartial unsafeIndex nextA 0
                      nextNext = fromMaybe "" $ index nextA 1
                  lift $ putVar var dat
                  put initialState
                  pure $ Loop nextNext
    produceRecords :: AVar String -> AffAVar (err :: EXCEPTION | eff) (AVar String)
    produceRecords v = do
        makeAff \onE onS -> do
            stateRef <- unsafeCoerceEff $ newSTRef initialState
            onError stream onE
            onDataString stream Encoding.UTF8 \s -> do
                currentState <- unsafeCoerceEff $ readSTRef stateRef
                runAff onE onS $ do
                    Tuple r endState <- runStateT (readRecord v s) currentState
                    unsafeCoerceAff <<< liftEff $ writeSTRef stateRef endState
                    pure r
                pure unit
        pure v
