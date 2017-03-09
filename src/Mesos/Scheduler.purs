module Mesos.Scheduler where

import Prelude
import Control.Alt ((<|>))
import Control.Monad.Aff (Aff, makeAff, runAff)
import Control.Monad.Aff.AVar (AVAR, AVar, makeVar, putVar)
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
import Data.Newtype (class Newtype)
import Data.Options (Options, (:=))
import Data.StrMap (lookup)
import Data.StrMap as SM
import Data.Tuple (Tuple(..))
import Node.Encoding as Encoding
import Node.HTTP as HTTP
import Node.HTTP.Client (RequestHeaders(..), RequestOptions, Response, request, method, path, requestAsStream, responseHeaders, responseAsStream, statusCode, headers)
import Node.Process (stdout)
import Mesos.Raw (FrameworkID, FrameworkInfo, KillPolicy, Offer, OfferID, TaskID, TaskStatus, AgentID, ExecutorID, Filters, readPropNU, readPropNUM)
import Mesos.Raw.Offer (Operation)
import Mesos.RecordIO (onRecordIO)
import Mesos.Stream (readFullString)
import Mesos.Util (jsonStringify, fromJSON, throwErrorS)
import Node.Stream (end, writeString, pipe)

newtype Kill = Kill
    { taskId :: TaskID
    , slaveId :: Maybe AgentID
    , killPolicy :: Maybe KillPolicy
    }

instance killAsForeign :: AsForeign Kill where
    write (Kill obj) = writeObject $
        [ "task_id" .= write obj.taskId
        , "slave_id" .= (write $ Undefined obj.slaveId)
        , "kill_policy" .= (write $ Undefined obj.killPolicy)
        ]

instance killIsForeign :: IsForeign Kill where
    read obj = do
        taskId <- readProp "task_id" obj
        slaveId <- readPropNU "slave_id" obj
        killPolicy <- readPropNU "kill_policy" obj
        pure <<< Kill $
            { taskId: taskId
            , slaveId: slaveId
            , killPolicy: killPolicy
            }

derive instance killNewtype :: Newtype Kill _

newtype ReconcileTask = ReconcileTask
    { taskId :: TaskID
    , slaveId :: Maybe AgentID
    }

instance reconcileTaskAsForeign :: AsForeign ReconcileTask where
    write (ReconcileTask obj) = writeObject $
        [ "task_id" .= write obj.taskId
        , "slave_id" .= (write $ Undefined obj.slaveId)
        ]

instance reconcileTaskIsForeign :: IsForeign ReconcileTask where
    read obj = do
        taskId <- readProp "task_id" obj
        slaveId <- readPropNU "slave_id" obj
        pure <<< ReconcileTask $
            { taskId: taskId
            , slaveId: slaveId
            }

derive instance reconcileTaskNewtype :: Newtype ReconcileTask _

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

newtype Decline = Decline
    { offerIds :: Array OfferID
    , filters :: Maybe Filters
    }

instance declineAsForeign :: AsForeign Decline where
    write (Decline obj) = writeObject props where
        props = [ "offer_ids" .= write obj.offerIds
                , "filters" .= (write $ Undefined obj.filters)
                ]

instance declineIsForeign :: IsForeign Decline where
    read obj = do
        offerIds <- readPropNUM "offer_ids" obj
        filters <- readPropNU "filters" obj
        pure <<< Decline $
            { offerIds: offerIds
            , filters: filters
            }

newtype Accept = Accept
    { offerIds :: Array OfferID
    , operations :: Array Operation
    , filters :: Maybe Filters
    }

instance acceptAsForeign :: AsForeign Accept where
    write (Accept obj) = writeObject props where
        props = [ "offer_ids" .= write obj.offerIds
                , "operations" .= write obj.operations
                , "filters" .= (write $ Undefined obj.filters)
                ]

instance acceptIsForeign :: IsForeign Accept where
    read obj = do
        offerIds <- readPropNUM "offer_ids" obj
        operations <- readPropNUM "operations" obj
        filters <- readPropNU "filters" obj
        pure <<< Accept $
            { offerIds: offerIds
            , operations: operations
            , filters: filters
            }

newtype Acknowlege = Acknowlege
    { slaveId :: AgentID
    , taskId :: TaskID
    , uuid :: String
    }

instance acknowlegeAsForeign :: AsForeign Acknowlege where
    write (Acknowlege obj) = writeObject props where
        props = [ "agent_id" .= write obj.slaveId
                , "task_id" .= write obj.taskId
                , "uuid" .= write obj.uuid
                ]

instance acknowlegeIsForeign :: IsForeign Acknowlege where
    read obj = do
        slaveId <- readProp "slave_id" obj <|> readProp "agent_id" obj
        taskId <- readProp "task_id" obj
        uuid <- readProp "uuid" obj
        pure <<< Acknowlege $
            { slaveId: slaveId
            , taskId: taskId
            , uuid: uuid
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
             | DeclineMessage FrameworkID Decline
             | AcceptMessage FrameworkID Accept
             | AcknowlegeMessage FrameworkID Acknowlege
             | ReconcileMessage FrameworkID (Array ReconcileTask)
             | KillMessage FrameworkID Kill

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
    write (DeclineMessage fwid decline) = toForeign $
        { type: "DECLINE"
        , framework_id: write fwid
        , decline: write decline
        }
    write (AcceptMessage fwid acceptInfo) = toForeign $
        { type: "ACCEPT"
        , framework_id: write fwid
        , accept: write acceptInfo
        }
    write (AcknowlegeMessage fwid acknowlegeInfo) = toForeign $
        { type: "ACKNOWLEDGE"
        , framework_id: write fwid
        , acknowledge: write acknowlegeInfo
        }
    write (ReconcileMessage fwid reconcileTasks) = toForeign $
        { type: "RECONCILE"
        , framework_id: write fwid
        , reconcile: { tasks: write reconcileTasks }
        }
    write (KillMessage fwid killInfo) = toForeign $
        { type: "KILL"
        , framework_id: write fwid
        , kill: write killInfo
        }

instance messageIsForeign :: IsForeign Message where
    read value = readProp "type" value >>= readMessageType where
        readMessageType "SUBSCRIBE" = throwError $ NEL.singleton $ ForeignError "Unimplemented!" -- TODO: implement
        readMessageType "SUBSCRIBED" = SubscribedMessage <$> readProp "subscribed" value
        readMessageType "OFFERS" = OffersMessage <$> (prop "offers" value >>= readProp "offers")
        readMessageType "RESCIND" = RescindMessage <$> (prop "rescind" value >>= readProp "offer_id")
        readMessageType "UPDATE" = UpdateMessage <$> (prop "update" value >>= readProp "status")
        readMessageType "MESSAGE" = MessageMessage <$> readProp "message" value
        readMessageType "FAILURE" = FailureMessage <$> readProp "failure" value
        readMessageType "ERRROR" = ErrorMessage <$> readProp "message" value
        readMessageType "HEARTBEAT" = pure HeartbeatMessage
        readMessageType "DECLINE" = DeclineMessage <$> readProp "framework_id" value <*> readProp "decline" value
        readMessageType "ACCEPT" = AcceptMessage <$> readProp "framework_id" value <*> readProp "accept" value
        readMessageType "ACKNOWLEDGE" = AcknowlegeMessage <$> readProp "framework_id" value <*> readProp "acknowlege" value
        readMessageType "RECONCILE" = ReconcileMessage <$> readProp "framework_id" value <*> (prop "reconcile" value >>= readProp "tasks")
        readMessageType "KILL" = KillMessage <$> readProp "framework_id" value <*> readProp "kill" value
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

checkResponseStatus :: forall eff. Int -> Response -> Aff (http :: HTTP.HTTP, err :: EXCEPTION | eff) Response
checkResponseStatus expectedStatusCode res
    | statusCode res == expectedStatusCode = pure res
    | otherwise = do
        let resStream = responseAsStream res
        message <- readFullString resStream Encoding.UTF8
        throwErrorS $ "Bad response status code (" <> (show <<< statusCode) res <> "): " <> message

-- | Convenience wrapper for `scheduler` that sends accept messages
accept :: forall eff. Options RequestOptions
       -> String
       -> Message
       -> Aff (http :: HTTP.HTTP, err :: EXCEPTION | eff) Response
accept userReqOpts mesosStreamId message =
    scheduler userReqOpts mesosStreamId message >>= checkResponseStatus 202

scheduler :: forall eff. Options RequestOptions
          -> String
          -> Message
          -> Aff (http :: HTTP.HTTP, err :: EXCEPTION | eff) Response
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

-- | `AVar` version of `subscribe`
subscribe' :: forall eff. Options RequestOptions
           -> Subscribe
           -> Aff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) (Tuple String (AVar Message))
subscribe' userReqOpts subscribeInfo = do
    v <- makeVar
    streamId <- subscribe userReqOpts subscribeInfo \str -> do
        runAff throwException (\_ -> pure unit) $ putVar v str
        pure unit
    pure $ Tuple streamId v

-- | Make a call to `/api/v1/subscribe`, taking an action for each message
subscribe :: forall eff. Options RequestOptions
          -> Subscribe
          -> (Message -> Eff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) Unit)
          -> Aff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) String
subscribe userReqOpts subscribeInfo callback = do
    res <- makeAff \_ onS -> do
        req <- request reqOpts onS
        let reqStream = requestAsStream req
            reqData = jsonStringify <<< write $ SubscribeMessage subscribeInfo
        writeString reqStream Encoding.UTF8 reqData $ end reqStream (pure unit)
        pure unit
    handleRes res
    where
        handleRes :: Response -> Aff (avar :: AVAR, err :: EXCEPTION, http :: HTTP.HTTP, console :: CONSOLE | eff) String
        handleRes res
          | statusCode res == 200 = do
              mesosStreamId <- requiredHeader "mesos-stream-id"
              liftEff <<< log $ "mesos-stream-id: " <> mesosStreamId
              liftEff $ onRecordIO resStream throwException handleRecord
              pure mesosStreamId
              where
                  handleRecord recordStr =
                      either (throw <<< flip (<>) (": " <> recordStr) <<< show) callback <<< runExcept <<< fromJSON $ recordStr
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
              throwErrorS $ "Response status error " <> (show <<< statusCode) res <> ")"
        reqOpts :: Options RequestOptions
        reqOpts = flip execState userReqOpts do
            overrideOption $ method := "POST"
            overrideOption $ path := "/api/v1/scheduler"
            overrideOption $ headers := subscribeHeaders
            where
                overrideOption o = modify $ flip (<>) o
