module Mesos.Scheduler where

import Prelude
import Control.Monad.Aff (Aff, makeAff)
import Control.Monad.Aff.AVar (AVAR)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Class (liftEff)
import Control.Monad.Eff.Console (CONSOLE, log)
import Control.Monad.Eff.Exception (EXCEPTION, Error, throw, throwException)
import Control.Monad.Except (runExcept)
import Control.Monad.Error.Class (class MonadError, throwError, catchError)
import Control.Monad.State (execState, modify)
import Data.Either (either)
import Data.Foreign (Foreign, writeObject, toForeign, ForeignError(..))
import Data.Foreign.Class (class AsForeign, class IsForeign, readProp, write, (.=))
import Data.Foreign.Index (prop)
import Data.Foreign.Undefined (Undefined(..))
import Data.List.NonEmpty as NEL
import Data.Maybe (Maybe, maybe')
import Data.Options (Options, (:=))
import Data.StrMap (lookup)
import Data.StrMap as SM
import Node.Encoding as Encoding
import Node.HTTP as HTTP
import Node.HTTP.Client (RequestHeaders(..), RequestOptions, Response, request, method, path, requestAsStream, responseHeaders, responseAsStream, statusCode, headers)
import Node.Process (stdout)
import Mesos.Raw (FrameworkID, FrameworkInfo, Offer, OfferID, TaskStatus, AgentID, ExecutorID, readPropNU)
import Mesos.RecordIO (onRecordIO)
import Mesos.Util (jsonStringify, fromJSON, throwErrorS)
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

newtype ExecutorMessage = ExecutorMessage
    { agentId :: AgentID
    , executorId :: ExecutorID
    , data :: String
    }

instance executorMessageAsForeign :: AsForeign ExecutorMessage where
    write (ExecutorMessage obj) = writeObject props where
        props = [ "agent_id" .= write obj.agentId
                , "executor_id" .= write obj.executorId
                , "data" .= write obj.data
                ]

instance executorMessageIsForeign :: IsForeign ExecutorMessage where
    read obj = do
        agentId <- readProp "agent_id" obj
        executorId <- readProp "executor_id" obj
        d <- readProp "data" obj
        pure <<< ExecutorMessage $
            { agentId: agentId
            , executorId: executorId
            , data: d
            }

newtype Failure = Failure
    { slaveId :: Maybe AgentID
    , executorId :: Maybe ExecutorID
    , status :: Maybe Int
    }

instance failureAsForeign :: AsForeign Failure where
    write (Failure obj) = writeObject props where
        props = [ "agent_id" .= (write $ Undefined obj.slaveId)
                , "executor_id" .= (write $ Undefined obj.executorId)
                , "status" .= (write $ Undefined obj.status)
                ]

instance failureIsForeign :: IsForeign Failure where
    read obj = do
        slaveId <- catchError (readPropNU "agent_id" obj) \_ -> readPropNU "slave_id" obj
        executorId <- readPropNU "executor_id" obj
        status <- readPropNU "status" obj
        pure <<< Failure $
            { slaveId: slaveId
            , executorId: executorId
            , status: status
            }

-- | Represents a single RecordIO message
data Message = SubscribeMessage Subscribe
             | SubscribedMessage Subscribed
             | OffersMessage (Array Offer)
             | RescindMessage OfferID
             | UpdateMessage TaskStatus
             | MessageMessage ExecutorMessage
             | FailureMessage Failure
             | ErrorMessage String
             | HeartbeatMessage
             | CustomMessage (forall o. { type :: String | o })

instance messageShow :: Show Message where
    show = jsonStringify <<< write

-- | Utility for Writing a mesos subscribe-style recordio message
writeMessage :: forall a. (AsForeign a) => String -> String -> a -> Foreign
writeMessage t k d = writeObject props where
    props = [ "type" .= toForeign t
            , k .= write d
            ]

instance messageAsForeign :: AsForeign Message where
    write (SubscribeMessage subscribeInfo) = writeMessage "SUBSCRIBE" "subscribe" subscribeInfo
    write (SubscribedMessage subscribedInfo) = writeMessage "SUBSCRIBED" "subscribed" subscribedInfo
    write (OffersMessage offers) = writeMessage "OFFERS" "offers" offers
    write (RescindMessage offerId) = toForeign $
        { type: "RESCIND"
        , rescind: { offer_id: write offerId }
        }
    write (UpdateMessage taskStatus) = toForeign $
        { type: "UPDATE"
        , update: { status: write taskStatus }
        }
    write (MessageMessage msg) = writeMessage "MESSAGE" "message" msg
    write (FailureMessage failure) = writeMessage "FAILURE" "failure" failure
    write (ErrorMessage msg) = toForeign $
        { type: "ERROR"
        , message: msg
        }
    write HeartbeatMessage = toForeign $ { type: "HEARTBEAT" }
    write (CustomMessage obj) = toForeign obj

instance messageIsForeign :: IsForeign Message where
    read value = readProp "type" value >>= readMessageType where
        readMessageType "SUBSCRIBE" = throwError $ NEL.singleton $ ForeignError "Unimplemented!" -- TODO: implement
        readMessageType "SUBSCRIBED" = SubscribedMessage <$> readProp "subscribed" value
        readMessageType "OFFERS" = OffersMessage <$> readProp "offers" value
        readMessageType "RESCIND" = RescindMessage <$> (prop "rescind" value >>= readProp "offer_id")
        readMessageType "UPDATE" = UpdateMessage <$> (prop "update" value >>= readProp "status")
        readMessageType "MESSAGE" = MessageMessage <$> readProp "message" value
        readMessageType "FAILURE" = FailureMessage <$> readProp "failure" value
        readMessageType "ERRROR" = ErrorMessage <$> readProp "message" value
        readMessageType "HEARTBEAT" = pure HeartbeatMessage
        readMessageType _ = throwError $ NEL.singleton $ ErrorAtProperty "type" (ForeignError "Unknown message type")

-- | List of common headers to include in a call to the mesos scheduler subscribe api
subscribeHeaders :: RequestHeaders
subscribeHeaders =
    SM.insert "Content-Type" "application/json" >>>
    SM.insert "Accept" "application/json" >>>
    SM.insert "Connection" "close" >>>
    RequestHeaders $
    SM.empty

messageHeaders :: SM.StrMap String
messageHeaders =
    SM.insert "Content-Type" "application/json" $
    SM.empty

scheduler :: forall eff. Options RequestOptions
          -> String
          -> Message
          -> Aff (http :: HTTP.HTTP | eff) Response
scheduler userReqOpts mesosStreamId message =
    makeAff \_ onS -> do
        req <- request reqOpts onS
        let reqStream = requestAsStream req
            reqData = jsonStringify <<< write $ message
        writeString reqStream Encoding.UTF8 reqData $ end reqStream (pure unit)
        pure unit
    where
    reqOpts :: Options RequestOptions
    reqOpts = flip execState userReqOpts do
        overrideOption $ method := "POST"
        overrideOption $ path := "/api/v1/scheduler"
        overrideOption $ headers := (RequestHeaders $ SM.insert "Mesos-Stream-Id" mesosStreamId messageHeaders)
        where
            overrideOption o = modify $ flip (<>) o

-- | Make a call to `/api/v1/subscribe`, taking an action for each message
subscribe :: forall eff. Options RequestOptions
          -> Subscribe
          -> (Message -> Eff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) Unit)
          -> Aff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) Unit
subscribe userReqOpts subscribeInfo callback = do
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
              liftEff $ onRecordIO resStream throwException handleRecord
              where
                  handleRecord =
                      either (throw <<< show) callback <<<
                      runExcept <<<
                      fromJSON
                  headers = responseHeaders res
                  resStream = responseAsStream res
                  requiredHeader :: forall m. (MonadError Error m) => String -> m String
                  requiredHeader key =
                      maybe' (throwErrorS <<< e) pure $ lookup key headers where
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
