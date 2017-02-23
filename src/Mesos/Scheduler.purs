module Mesos.Scheduler where

import Prelude
import Control.Monad.Aff (Aff, makeAff)
import Control.Monad.Aff.AVar (AVAR)
import Control.Monad.Eff.Class (liftEff)
import Control.Monad.Eff.Console (CONSOLE, log)
import Control.Monad.Eff.Exception (EXCEPTION, Error, error, throwException)
import Control.Monad.Error.Class (class MonadError, throwError)
import Control.Monad.State (execState, modify)
import Data.Foreign (Foreign, writeObject, toForeign, ForeignError(..))
import Data.Foreign.Class (class AsForeign, class IsForeign, readProp, write, (.=))
import Data.List.NonEmpty as NEL
import Data.Maybe (maybe')
import Data.Options (Options, (:=))
import Data.StrMap (lookup)
import Data.StrMap as SM
import Node.Encoding as Encoding
import Node.HTTP as HTTP
import Node.HTTP.Client (RequestHeaders(..), RequestOptions, Response, request, method, path, requestAsStream, responseHeaders, responseAsStream, statusCode, headers)
import Node.Process (stdout)
import Mesos.Raw (FrameworkID, FrameworkInfo)
import Mesos.RecordIO (onRecordIO)
import Mesos.Util (jsonStringify)
import Node.Stream (end, writeString, pipe)

newtype Subscribed = Subscribed { frameworkId :: FrameworkID
                                , heartbeatIntervalSeconds :: Int
                                }

instance subscribedAsForeign :: AsForeign Subscribed where
    write (Subscribed obj) = writeObject props where
        props = [ "framework_id" .= write obj.frameworkId
                , "heartbeat_interval_seconds" .= write obj.heartbeatIntervalSeconds
                ]

instance subscribedIsForeign :: IsForeign Subscribed where
    read value = do
        frameworkId <- readProp "framework_id" value
        heartbeatIntervalSeconds <- readProp "heartbeat_interval_seconds" value
        pure $ Subscribed { frameworkId: frameworkId
                          , heartbeatIntervalSeconds: heartbeatIntervalSeconds
                          }

newtype Subscribe = Subscribe { frameworkInfo :: FrameworkInfo
                              }

instance subscribeAsForeign :: AsForeign Subscribe where
    write (Subscribe obj) = writeObject props where
        props = [ "framework_info" .= write obj.frameworkInfo
                ]
-- | Represents a single RecordIO message
data Message = SubscribeMessage Subscribe
             | SubscribedMessage Subscribed

writeMessage :: forall a. (AsForeign a) => String -> String -> a -> Foreign
writeMessage t k d = writeObject props where
    props = [ "type" .= toForeign t
            , k .= write d
            ]

instance messageAsForeign :: AsForeign Message where
    write (SubscribeMessage subscribeInfo) = writeMessage "SUBSCRIBE" "subscribe" subscribeInfo
    write (SubscribedMessage subscribedInfo) = writeMessage "SUBSCRIBED" "subscribed" subscribedInfo

instance messageIsForeign :: IsForeign Message where
    read value = readProp "type" value >>= readMessageType where
        readMessageType "SUBSCRIBE" = throwError $ NEL.singleton $ ForeignError "Unimplemented!" -- TODO: implement
        readMessageType "SUBSCRIBED" = SubscribedMessage <$> readProp "subscribed" value
        readMessageType _ = throwError $ NEL.singleton $ ErrorAtProperty "type" (ForeignError "Unknown message type")

subscribeHeaders :: RequestHeaders
subscribeHeaders =
    SM.insert "Content-Type" "application/json" >>>
    SM.insert "Accept" "application/json" >>>
    SM.insert "Connection" "close" >>>
    RequestHeaders $
    SM.empty

subscribe :: forall eff. Options RequestOptions
          -> Subscribe
          -> (String -> Aff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) Unit)
          -> Aff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) Unit
subscribe userReqOpts subscribeInfo listeners = do
    res <- makeAff \_ onS -> do
        req <- request reqOpts onS
        let reqStream = requestAsStream req
            reqData = jsonStringify <<< write $ SubscribeMessage subscribeInfo
        writeString reqStream Encoding.UTF8 reqData $ end reqStream (pure unit)
        pure unit
    handleRes res
    where
        handleRes :: Response -> Aff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) Unit
        handleRes res
          | statusCode res == 200 = do
              mesosStreamId <- requiredHeader "mesos-stream-id"
              liftEff <<< log $ "mesos-stream-id: " <> mesosStreamId
              liftEff $ onRecordIO resStream throwException log
              where
                  headers = responseHeaders res
                  resStream = responseAsStream res
                  requiredHeader :: forall m. (MonadError Error m) => String -> m String
                  requiredHeader key =
                      maybe' (throwError <<< error <<< e) pure $ lookup key headers where
                          e _ = "Header \"" <> key <> "\" not found"
          | otherwise = do
              liftEff do
                  log $ "Reponse status error (" <> (show <<< statusCode) res <> "):"
                  pipe (responseAsStream res) stdout
                  log ""
              pure unit
        reqOpts :: Options RequestOptions
        reqOpts = flip execState userReqOpts do
            overrideOption $ method := "POST"
            overrideOption $ path := "/api/v1/scheduler"
            overrideOption $ headers := subscribeHeaders
            where
                overrideOption o = modify $ flip (<>) o
