module Mesos.Raw where

import Prelude
import Control.Alt ((<|>))
import Control.Monad.Error.Class (catchError, throwError)
import Data.Either (Either(..))
import Data.Foreign (F, Foreign, ForeignError(..), toForeign, writeObject, isNull, isUndefined, unsafeFromForeign)
import Data.Foreign.Class (class AsForeign, class IsForeign, read, write, readProp, (.=))
import Data.Foreign.Index (prop)
import Data.Foreign.NullOrUndefined (readNullOrUndefined, unNullOrUndefined)
import Data.Foreign.Undefined (Undefined(..))
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Monoid (class Monoid, mempty)
import Data.Newtype (class Newtype)
import Data.List.NonEmpty as NEL

readPropNU :: forall a. (IsForeign a) => String -> Foreign -> F (Maybe a)
readPropNU p v = unNullOrUndefined <$> (prop p v >>= readNullOrUndefined read)

readPropNUM :: forall a. (IsForeign a, Monoid a) => String -> Foreign -> F a
readPropNUM p v = fromMaybe mempty <$> readPropNU p v

isNullOrUndefined :: Foreign -> Boolean
isNullOrUndefined = isNull || isUndefined

-- | TODO: actually implement this
foreign import data HealthCheck :: *

instance healthCheckAsForeign :: AsForeign HealthCheck where
    write = toForeign

instance healthCheckIsForeign :: IsForeign HealthCheck where
    read = pure <<< unsafeFromForeign

-- | TODO: actually implement this
foreign import data DiscoveryInfo :: *

instance discoveryInfoAsForeign :: AsForeign DiscoveryInfo where
    write = toForeign

instance discoveryInfoIsForeign :: IsForeign DiscoveryInfo where
    read = pure <<< unsafeFromForeign

-- | TODO: actually implement this
foreign import data LinuxInfo :: *

instance linuxInfoAsForeign :: AsForeign LinuxInfo where
    write = toForeign

instance linuxInfoIsForeign :: IsForeign LinuxInfo where
    read = pure <<< unsafeFromForeign

-- | TODO: actually implement this
foreign import data NetworkInfo :: *

instance networkInfoAsForeign :: AsForeign NetworkInfo where
    write = toForeign

instance networkInfoIsForeign :: IsForeign NetworkInfo where
    read = pure <<< unsafeFromForeign

-- | TODO: actually implement this
foreign import data TaskGroupInfo :: *

instance taskGroupInfoAsForeign :: AsForeign TaskGroupInfo where
    write = toForeign

instance taskGroupInfoIsForeign :: IsForeign TaskGroupInfo where
    read = pure <<< unsafeFromForeign

-- | TODO: actually implement this
foreign import data DockerInfo :: *

instance dockerInfoAsForeign :: AsForeign DockerInfo where
    write = toForeign

instance dockerInfoIsForeign :: IsForeign DockerInfo where
    read = pure <<< unsafeFromForeign

data VolumeMode = ReadWrite
                | ReadOnly

instance volumeModeAsForeign :: AsForeign VolumeMode where
    write ReadWrite = toForeign "RW"
    write ReadOnly = toForeign "RO"

instance volumeModeIsForeign :: IsForeign VolumeMode where
    read obj = do
        s <- read obj
        case s of
          "RW" -> pure ReadWrite
          "RO" -> pure ReadOnly
          t -> throwError <<< NEL.singleton <<< ForeignError $ "Unknown volume mode: " <> t

data Parameter = Parameter String String

instance parameterAsForeign :: AsForeign Parameter where
    write (Parameter name value) = toForeign { name: name, value: value }

instance parameterIsForeign :: IsForeign Parameter where
    read obj = do
        name <- readProp "name" obj
        value <- readProp "value" obj
        pure $ Parameter name value

newtype Parameters = Parameters (Array Parameter)

instance parametersAsForeign :: AsForeign Parameters where
    write (Parameters params) = toForeign { parameter: write params }

instance parametersIsForeign :: IsForeign Parameters where
    read obj = do
        params <- readProp "parameter" obj
        pure $ Parameters params

newtype VolumeSourceDocker = VolumeSourceDocker
    { driver :: Maybe String
    , name :: String
    , driverOptions :: Maybe Parameters
    }

instance volumeSourceDockerAsForeign :: AsForeign VolumeSourceDocker where
    write (VolumeSourceDocker obj) = writeObject props where
        props = [ "driver" .= (write $ Undefined obj.driver)
                , "name" .= write obj.name
                , "driver_options" .= (write $ Undefined obj.driverOptions)
                ]

instance volumeSourceDockerIsForeign :: IsForeign VolumeSourceDocker where
    read obj = do
        driver <- readPropNU "driver" obj
        name <- readProp "name" obj
        driverOptions <- readPropNU "driver_options" obj
        pure <<< VolumeSourceDocker $
            { driver: driver
            , name: name
            , driverOptions: driverOptions
            }

newtype VolumeSourceSandbox = VolumeSourceSandbox
    { type :: String
    , path :: String
    }

instance volumeSourceSandboxAsForeign :: AsForeign VolumeSourceSandbox where
    write (VolumeSourceSandbox obj) = toForeign obj

instance volumeSourceSandboxIsForeign :: IsForeign VolumeSourceSandbox where
    read obj = do
        t <- readProp "type" obj
        path <- readProp "path" obj
        pure <<< VolumeSourceSandbox $
            { type: t
            , path: path
            }

newtype Volume = Volume
    { mode :: VolumeMode
    , containerPath :: String
    , hostPath :: Maybe String
    , image :: Maybe Image
    , source :: Maybe (Either VolumeSourceDocker VolumeSourceSandbox)
    }

instance volumeAsForeign :: AsForeign Volume where
    write (Volume obj) = writeObject props where
        props = sourceProps <> [ "mode" .= write obj.mode
                               , "container_path" .= write obj.containerPath
                               , "host_path" .= (write $ Undefined obj.hostPath)
                               , "image" .= (write $ Undefined obj.image)
                               ]
        sourceProps =
            case obj.source of
              Nothing -> []
              Just (Left docker) -> [ "source" .= toForeign { type: "DOCKER_VOLUME", docker_volume: write docker } ]
              Just (Right sandbox) -> [ "source" .= toForeign { type: "SANDBOX_PATH", sandbox_path: write sandbox } ]

instance volumeIsForeign :: IsForeign Volume where
    read obj = do
        mode <- readProp "mode" obj
        containerPath <- readProp "container_path" obj
        hostPath <- readPropNU "host_path" obj
        image <- readPropNU "image" obj
        maybeSource <- catchError (Just <$> prop "source" obj) \_ -> pure Nothing
        let filteredForeignSource =
                do
                    foreignSource <- maybeSource
                    if isNullOrUndefined foreignSource
                       then Nothing
                       else Just foreignSource
        source <- case filteredForeignSource of
                    Nothing -> pure Nothing
                    Just f -> Just <$> readSource f
        pure <<< Volume $
            { mode: mode
            , containerPath: containerPath
            , hostPath: hostPath
            , image: image
            , source: source
            }
        where
            readSource :: Foreign -> F (Either VolumeSourceDocker VolumeSourceSandbox)
            readSource sObj = do
                ty <- readProp "type" sObj
                case ty of
                  "DOCKER_VOLUME" -> Left <$> readProp "docker_volume" sObj
                  "SANDBOX_PATH" -> Right <$> readProp "sandbox_path" sObj
                  t -> throwError <<< NEL.singleton <<< ForeignError $ "Unknown volume source type: " <> t

newtype Credential = Credential
    { principal :: String
    , secret :: Maybe String
    }

instance credentialAsForeign :: AsForeign Credential where
    write (Credential obj) = writeObject props where
        props = [ "principal" .= write obj.principal
                , "secret" .= (write $ Undefined obj.secret)
                ]

instance credentialIsForeign :: IsForeign Credential where
    read obj = do
        principal <- readProp "principal" obj
        secret <- readPropNU "secret" obj
        pure <<< Credential $
            { principal: principal
            , secret: secret
            }

newtype DockerImage = DockerImage
    { name :: String
    , credential :: Maybe Credential
    }

instance dockerImageAsForeign :: AsForeign DockerImage where
    write (DockerImage obj) = writeObject props where
        props = [ "name" .= write obj.name
                , "credential" .= (write $ Undefined obj.credential)
                ]

instance dockerImageIsForeign :: IsForeign DockerImage where
    read obj = do
        name <- readProp "name" obj
        credential <- readPropNU "credential" obj
        pure <<< DockerImage $
            { name: name
            , credential: credential
            }

-- | TODO: Actually implement this
foreign import data AppcImage :: *

instance appcImageAsForeign :: AsForeign AppcImage where
    write = toForeign

instance appcImageIsForeign :: IsForeign AppcImage where
    read = pure <<< unsafeFromForeign

newtype Image = Image 
    { image :: Either AppcImage DockerImage
    , cached :: Boolean
    }

instance imageAsForeign :: AsForeign Image where
    write (Image obj) = writeObject props where
        props = imageProps <> [ "cached" .= write obj.cached
                              ]
        imageProps =
            case obj.image of
              Left appc -> [ "type" .= toForeign "APPC"
                           , "appc" .= write appc
                           ]
              Right docker -> [ "type" .= toForeign "DOCKER"
                              , "docker" .= write docker
                              ]

instance imageIsForeign :: IsForeign Image where
    read obj = do
        cached <- fromMaybe true <$> readPropNU "cached" obj
        image <- readProp "type" obj >>= readImage
        pure <<< Image $
            { image: image
            , cached: cached
            }
        where
            readImage :: String -> F (Either AppcImage DockerImage)
            readImage "APPC" = Left <$> readProp "appc" obj
            readImage "DOCKER" = Right <$> readProp "docker" obj
            readImage t = throwError <<< NEL.singleton <<< ForeignError $ "Unknown image type: " <> t

data ExecutorInfoType = DefaultExecutor
                      | CustomExecutor

instance executorInfoTypeAsForeign :: AsForeign ExecutorInfoType where
    write (DefaultExecutor) = toForeign "DEFAULT"
    write (CustomExecutor) = toForeign "CUSTOM"

instance executorInfoTypeIsForeign :: IsForeign ExecutorInfoType where
    read obj = do
        s <- read obj
        case s of
          "DEFAULT" -> pure DefaultExecutor
          "CUSTOM" -> pure CustomExecutor
          t -> throwError <<< NEL.singleton <<< ForeignError $ "Unknown executor info type: " <> t

data EnvironmentVariable = EnvironmentVariable String String

instance environmentVariableAsForeign :: AsForeign EnvironmentVariable where
    write (EnvironmentVariable name value) = toForeign { name: name, value: value }

instance environmentVariableIsForeign :: IsForeign EnvironmentVariable where
    read obj = do
        name <- readProp "name" obj
        value <- readProp "value" obj
        pure $ EnvironmentVariable name value

newtype Environment = Environment (Array EnvironmentVariable)

instance environmentAsForeign :: AsForeign Environment where
    write (Environment vars) = toForeign $
        { variables: write vars
        }

instance environmentIsForeign :: IsForeign Environment where
    read obj = do
        variables <- readPropNUM "variables" obj
        pure $ Environment variables

newtype CommandInfoURI = CommandInfoURI
    { value :: String
    , executable :: Maybe Boolean
    , extract :: Maybe Boolean
    , cache :: Maybe Boolean
    , outputFile :: Maybe String
    }

instance commandInfoURIAsForeign :: AsForeign CommandInfoURI where
    write (CommandInfoURI obj) = writeObject props where
        props = [ "value" .= write obj.value
                , "executable" .= (write $ Undefined obj.executable)
                , "extract" .= (write $ Undefined obj.extract)
                , "cache" .= (write $ Undefined obj.cache)
                , "output_file" .= (write $ Undefined obj.outputFile)
                ]

instance commandInfoURIIsForeign :: IsForeign CommandInfoURI where
    read obj = do
        value <- readProp "value" obj
        executable <- readPropNU "executable" obj
        extract <- readPropNU "extract" obj
        cache <- readPropNU "cache" obj
        outputFile <- readPropNU "output_file" obj
        pure <<< CommandInfoURI $
            { value: value
            , executable: executable
            , extract: extract <|> Just true
            , cache: cache
            , outputFile: outputFile
            }

newtype CommandInfo = CommandInfo
    { uris :: Array CommandInfoURI
    , environment :: Maybe Environment
    , shell :: Boolean
    , value :: Maybe String
    , arguments :: Array String
    , user :: Maybe String
    }

instance commandInfoAsForeign :: AsForeign CommandInfo where
    write (CommandInfo obj) = writeObject props where
        props = [ "uris" .= write obj.uris
                , "environment" .= (write $ Undefined obj.environment)
                , "shell" .= write obj.shell
                , "value" .= (write $ Undefined obj.value)
                , "arguments" .= write obj.arguments
                , "user" .= (write $ Undefined obj.user)
                ]

instance commandInfoIsForeign :: IsForeign CommandInfo where
    read obj = do
        uris <- readPropNUM "uris" obj
        environment <- readPropNU "environment" obj
        shell <- readPropNU "shell" obj
        value <- readPropNU "value" obj
        arguments <- readPropNUM "arguments" obj
        user <- readPropNU "user" obj
        pure <<< CommandInfo $
            { uris: uris
            , environment: environment
            , shell: fromMaybe true shell
            , value: value
            , arguments: arguments
            , user: user
            }

newtype DurationInfo = DurationInfo Int

instance durationInfoAsForeign :: AsForeign DurationInfo where
    write (DurationInfo nanoseconds) = toForeign { nanoseconds: nanoseconds }

instance durationInfoIsForeign :: IsForeign DurationInfo where
    read obj = do
        nanoseconds <- readProp "nanoseconds" obj
        pure $ DurationInfo nanoseconds

data ContainerType = DockerContainer
                   | MesosContainer

instance containerTypeAsForeign :: AsForeign ContainerType where
    write DockerContainer = toForeign "DOCKER"
    write MesosContainer = toForeign "MESOS"

instance containerTypeIsForeign :: IsForeign ContainerType where
    read obj = do
        s <- read obj
        case s of
          "DOCKER" -> pure DockerContainer
          "MESOS" -> pure MesosContainer
          t -> throwError <<< NEL.singleton <<< ForeignError $ "Unknown container info type: " <> t

newtype MesosInfo = MesosInfo
    { image :: Image
    }

instance mesosInfoAsForeign :: AsForeign MesosInfo where
    write (MesosInfo obj) = toForeign $ { image: write obj.image }

instance mesosInfoIsForeign :: IsForeign MesosInfo where
    read obj = do
        image <- readProp "image" obj
        pure <<< MesosInfo $
            { image: image
            }

newtype ContainerInfo = ContainerInfo
    { type :: ContainerType
    , volumes :: Array Volume
    , hostname :: Maybe String
    , docker :: Maybe DockerInfo
    , mesos :: Maybe MesosInfo
    , networkInfos :: Array NetworkInfo
    , linuxInfo :: Maybe LinuxInfo
    }

instance containerInfoAsForeign :: AsForeign ContainerInfo where
    write (ContainerInfo obj) = writeObject props where
        props = [ "type" .= write obj.type
                , "volumes" .= write obj.volumes
                , "hostname" .= (write $ Undefined obj.hostname)
                , "docker" .= (write $ Undefined obj.docker)
                , "mesos" .= (write $ Undefined obj.mesos)
                , "network_infos" .= write obj.networkInfos
                , "linux_info" .= (write <<< Undefined) obj.linuxInfo
                ]

instance containerInfoIsForeign :: IsForeign ContainerInfo where
    read obj = do
        t <- readProp "type" obj
        volumes <- readPropNUM "volumes" obj
        hostname <- readPropNU "hostname" obj
        docker <- readPropNU "docker" obj
        mesos <- readPropNU "mesos" obj
        networkInfos <- readPropNUM "network_infos" obj
        linuxInfo <- readPropNU "linux_info" obj
        pure <<< ContainerInfo $
            { type: t
            , volumes: volumes
            , hostname: hostname
            , docker: docker
            , mesos: mesos
            , networkInfos: networkInfos
            , linuxInfo: linuxInfo
            }

newtype ExecutorInfo = ExecutorInfo
    { type :: ExecutorInfoType
    , executorId :: ExecutorID
    , frameworkId :: FrameworkID
    , command :: Maybe CommandInfo
    , container :: Maybe ContainerInfo
    , resources :: Array Resource
    , name :: Maybe String
    , data :: Maybe String
    , discovery :: Maybe DiscoveryInfo
    , shutdownGracePeriod :: Maybe DurationInfo
    , labels :: Maybe Labels
    }

instance executorInfoAsForeign :: AsForeign ExecutorInfo where
    write (ExecutorInfo obj) = writeObject props where
        props = [ "type" .= write obj.type
                , "executor_id" .= write obj.executorId
                , "framework_id" .= write obj.frameworkId
                , "command" .= (write $ Undefined obj.command)
                , "container" .= (write $ Undefined obj.container)
                , "resources" .= write obj.resources
                , "name" .= (write $ Undefined obj.name)
                , "data" .= (write $ Undefined obj.data)
                , "discovery" .= (write $ Undefined obj.discovery)
                , "shutdown_grace_period" .= (write $ Undefined obj.shutdownGracePeriod)
                , "labels" .= (write $ Undefined obj.labels)
                ]

instance executorInfoIsForeign :: IsForeign ExecutorInfo where
    read obj = do
        t <- readProp "type" obj
        executorId <- readProp "executor_id" obj
        frameworkId <- readProp "framework_id" obj
        command <- readPropNU "command" obj
        container <- readPropNU "container" obj
        resources <- readPropNUM "resources" obj
        name <- readPropNU "name" obj
        d <- readPropNU "data" obj
        discovery <- readPropNU "discovery" obj
        shutdownGracePeriod <- readPropNU "shutdown_grace_period" obj
        labels <- readPropNU "labels" obj
        pure <<< ExecutorInfo $
            { type: t
            , executorId: executorId
            , frameworkId: frameworkId
            , command: command
            , container: container
            , resources: resources
            , name: name
            , data: d
            , discovery: discovery
            , shutdownGracePeriod: shutdownGracePeriod
            , labels: labels
            }

newtype KillPolicy = KillPolicy
    { gracePeriod :: Maybe DurationInfo
    }

instance killPolicyAsForeign :: AsForeign KillPolicy where
    write (KillPolicy obj) = writeObject props where
        props = [ "grace_period" .= (write $ Undefined obj.gracePeriod)
                ]

instance killPolicyIsForeign :: IsForeign KillPolicy where
    read obj = do
        gracePeriod <- readPropNU "grace_period" obj
        pure <<< KillPolicy $
            { gracePeriod: gracePeriod
            }

newtype TaskInfo = TaskInfo
    { name :: String
    , taskId :: TaskID
    , slaveId :: SlaveID
    , resources :: Array Resource
    , executor :: Maybe ExecutorInfo
    , command :: Maybe CommandInfo
    , container :: Maybe ContainerInfo
    , healthCheck :: Maybe HealthCheck
    , killPolicy :: Maybe KillPolicy
    , data :: Maybe String
    , labels :: Maybe Labels
    , discoveryInfo :: Maybe DiscoveryInfo
    }

instance taskInfoAsForeign :: AsForeign TaskInfo where
    write (TaskInfo obj) = writeObject props where
        props = [ "name" .= write obj.name
                , "task_id" .= write obj.taskId
                , "agent_id" .= write obj.slaveId
                , "resources" .= write obj.resources
                , "executor" .= (write $ Undefined obj.executor)
                , "command" .= (write $ Undefined obj.command)
                , "container" .= (write $ Undefined obj.container)
                , "health_check" .= (write $ Undefined obj.healthCheck)
                , "kill_policy" .= (write $ Undefined obj.killPolicy)
                , "data" .= (write $ Undefined obj.data)
                , "labels" .= (write $ Undefined obj.labels)
                , "discovery_info" .= (write $ Undefined obj.discoveryInfo)
                ]

instance taskInfoIsForeign :: IsForeign TaskInfo where
    read obj = do
        name <- readProp "name" obj
        taskId <- readProp "task_id" obj
        slaveId <- readProp "slave_id" obj <|> readProp "agent_id" obj
        resources <- readPropNUM "resources" obj
        executor <- readPropNU "executor" obj
        command <- readPropNU "command" obj
        container <- readPropNU "container" obj
        healthCheck <- readPropNU "health_check" obj
        killPolicy <- readPropNU "kill_policy" obj
        d <- readPropNU "data" obj
        labels <- readPropNU "labels" obj
        discoveryInfo <- readPropNU "discovery_info" obj
        pure <<< TaskInfo $
            { name: name
            , taskId: taskId
            , slaveId: slaveId
            , resources: resources
            , executor: executor
            , command: command
            , container: container
            , healthCheck: healthCheck
            , killPolicy: killPolicy
            , data: d
            , labels: labels
            , discoveryInfo: discoveryInfo
            }

derive instance taskInfoNewtype :: Newtype TaskInfo _

newtype Filters = Filters
    { refuseSeconds :: Maybe Number
    }

instance filtersAsForeign :: AsForeign Filters where
    write (Filters obj) = toForeign $ { refuse_seconds: write $ Undefined obj.refuseSeconds }

instance filtersIsForeign :: IsForeign Filters where
    read obj = do
        refuseSeconds <- readPropNU "refuse_seconds" obj
        pure $ Filters { refuseSeconds: refuseSeconds
                       }

-- | TODO: actually implement this
foreign import data ContainerStatus :: *

instance containerStatusAsForeign :: AsForeign ContainerStatus where
    write = toForeign

instance containerStatusIsForeign :: IsForeign ContainerStatus where
    read = pure <<< unsafeFromForeign

newtype TaskStatus = TaskStatus
    { taskId :: TaskID
    , state :: String
    , message :: Maybe String
    , source :: Maybe String
    , reason :: Maybe String
    , data :: Maybe String
    , slaveId :: Maybe SlaveID
    , executorId :: Maybe ExecutorID
    , timestamp :: Maybe Number
    , uuid :: Maybe String
    , healthy :: Maybe Boolean
    , labels :: Maybe Labels
    , containerStatus :: Maybe ContainerStatus
    }

instance taskStatusAsForeign :: AsForeign TaskStatus where
    write (TaskStatus obj) = writeObject props where
        props = [ "task_id" .= write obj.taskId
                , "state" .= write obj.state
                , "message" .= (write $ Undefined obj.message)
                , "source" .= (write $ Undefined obj.source)
                , "reason" .= (write $ Undefined obj.reason)
                , "data" .= (write $ Undefined obj.data)
                , "agent_id" .= (write $ Undefined obj.slaveId)
                , "executor_id" .= (write $ Undefined obj.executorId)
                , "timestamp" .= (write $ Undefined obj.timestamp)
                , "uuid" .= (write $ Undefined obj.uuid)
                , "healthy" .= (write $ Undefined obj.healthy)
                , "labels" .= (write $ Undefined obj.labels)
                , "container_status" .= (write $ Undefined obj.containerStatus)
                ]

instance taskStatusIsForeign :: IsForeign TaskStatus where
    read obj = do
        taskId <- readProp "task_id" obj
        state <- readProp "state" obj
        message <- readPropNU "message" obj
        source <- readPropNU "source" obj
        reason <- readPropNU "reason" obj
        d <- readPropNU "data" obj
        slaveId <- catchError (readPropNU "slave_id" obj) \_ -> readPropNU "agent_id" obj
        executorId <- readPropNU "executor_id" obj
        timestamp <- readPropNU "timestamp" obj
        uuid <- readPropNU "uuid" obj
        healthy <- readPropNU "healthy" obj
        labels <- readPropNU "labels" obj
        containerStatus <- readPropNU "conainer_status" obj
        pure <<< TaskStatus $
            { taskId: taskId
            , state: state
            , message: message
            , source: source
            , reason: reason
            , data: d
            , slaveId: slaveId
            , executorId: executorId
            , timestamp: timestamp
            , uuid: uuid
            , healthy: healthy
            , labels: labels
            , containerStatus: containerStatus
            }

newtype Range = Range
    { begin :: Int
    , end :: Int
    }

instance rangeAsForeign :: AsForeign Range where
    write (Range obj) = toForeign obj

instance rangeIsForeign :: IsForeign Range where
    read obj = do
        begin <- readProp "begin" obj
        end <- readProp "end" obj
        pure $ Range { begin: begin
                     , end: end
                     }

newtype Ranges = Ranges (Array Range)

instance rangesAsForeign :: AsForeign Ranges where
    write (Ranges ranges) = toForeign $ { range: write ranges }

instance rangesIsForeign :: IsForeign Ranges where
    read obj = Ranges <$> readProp "range" obj

newtype Set = Set (Array String)

instance setAsForeign :: AsForeign Set where
    write (Set items) = toForeign $ { item: items }

instance setIsForeign :: IsForeign Set where
    read obj = Set <$> readProp "item" obj

data AttributeValue = ScalarAttribute Scalar
                    | RangesAttribute Ranges
                    | SetAttribute Set
                    | TextAttribute Text

data Attribute = Attribute String AttributeValue

instance attributeAsForeign :: AsForeign Attribute where
    write (Attribute name (ScalarAttribute s)) = toForeign $
        { name: name
        , type: "SCALAR"
        , scalar: write s
        }
    write (Attribute name (RangesAttribute ranges)) = toForeign $
        { name: name
        , type: "RANGES"
        , ranges: write ranges
        }
    write (Attribute name (SetAttribute set)) = toForeign $
        { name: name
        , type: "SET"
        , set: write set
        }
    write (Attribute name (TextAttribute text)) = toForeign $
        { name: name
        , type: "TEXT"
        , text: write text
        }

instance attributeIsForeign :: IsForeign Attribute where
    read obj = do
        name <- readProp "name" obj
        v <- readProp "type" obj >>= readAttributeType
        pure $ Attribute name v
        where
            readAttributeType "SCALAR" = ScalarAttribute <$> readProp "scalar" obj
            readAttributeType "RANGES" = RangesAttribute <$> readProp "ranges" obj
            readAttributeType "SET" = SetAttribute <$> readProp "set" obj
            readAttributeType "TEXT" = TextAttribute <$> readProp "text" obj
            readAttributeType t = throwError <<< NEL.singleton <<< ForeignError $ "Unknown attribute type " <> show t

data ResourceValue = ScalarResource Scalar
                   | RangesResource Ranges
                   | SetResource Set

data Resource = Resource String ResourceValue

instance resourceAsForeign :: AsForeign Resource where
    write (Resource name (ScalarResource s)) = toForeign $
        { name: name
        , type: "SCALAR"
        , scalar: write s
        }
    write (Resource name (RangesResource ranges)) = toForeign $
        { name: name
        , type: "RANGES"
        , ranges: write ranges
        }
    write (Resource name (SetResource set)) = toForeign $
        { name: name
        , type: "SET"
        , set: write set
        }

instance resourceIsForiegn :: IsForeign Resource where
    read obj = do
        name <- readProp "name" obj
        v <- readProp "type" obj >>= readResourceType
        pure $ Resource name v
        where
            readResourceType "SCALAR" = ScalarResource <$> readProp "scalar" obj
            readResourceType "RANGES" = RangesResource <$> readProp "ranges" obj
            readResourceType "SET" = SetResource <$> readProp "set" obj
            readResourceType t = throwError <<< NEL.singleton <<< ForeignError $ "Unknown resource type " <> show t

newtype Address = Address
    { hostname :: Maybe String
    , ip :: Maybe String
    , port :: Int
    }

instance addressAsForeign :: AsForeign Address where
    write (Address obj) = writeObject props where
        props = [ "hostname" .= (write $ Undefined obj.hostname)
                , "ip" .= (write $ Undefined obj.ip)
                , "port" .= write obj.port
                ]

instance addressIsForeign :: IsForeign Address where
    read obj = do
        hostname <- readPropNU "hostname" obj
        ip <- readPropNU "ip" obj
        port <- readProp "port" obj
        pure $ Address { hostname: hostname
                       , ip: ip
                       , port: port
                       }

newtype URL = URL
    { scheme :: String
    , address :: Address
    , path :: Maybe String
    , query :: Array Parameter
    , fragment :: Maybe String
    }

instance urlAsForeign :: AsForeign URL where
    write (URL obj) = writeObject props where
        props = [ "scheme" .= write obj.scheme
                , "address" .= write obj.address
                , "path" .= (write $ Undefined obj.path)
                , "query" .= write obj.query
                , "fragment" .= (write $ Undefined obj.fragment)
                ]

instance urlIsForeign :: IsForeign URL where
    read obj = do
        scheme <- readProp "scheme" obj
        address <- readProp "address" obj
        path <- readPropNU "path" obj
        query <- readPropNUM "query" obj
        fragment <- readPropNU "fragment" obj
        pure <<< URL $
            { scheme: scheme
            , address: address
            , path: path
            , query: query
            , fragment: fragment
            }

newtype Offer = Offer
    { id :: OfferID
    , frameworkId :: FrameworkID
    , slaveId :: SlaveID
    , hostname :: String
    , url :: Maybe URL
    , resources :: Array Resource
    , attributes :: Array Attribute
    , executorIds :: Array ExecutorID
    }

instance offerAsForeign :: AsForeign Offer where
    write (Offer obj) = writeObject props where
        props = [ "id" .= write obj.id
                , "framework_id" .= write obj.frameworkId
                , "agent_id" .= write obj.slaveId
                , "hostname" .= write obj.hostname
                , "url" .= (write $ Undefined obj.url)
                , "resources" .= write obj.resources
                , "attributes" .= write obj.attributes
                , "executor_ids" .= write obj.executorIds
                ]

instance offerIsForeign :: IsForeign Offer where
    read obj = do
        id <- readProp "id" obj
        frameworkId <- readProp "framework_id" obj
        slaveId <- catchError (readProp "slave_id" obj) \_ -> readProp "agent_id" obj
        hostname <- readProp "hostname" obj
        url <- readPropNU "url" obj
        resources <- readPropNUM "resources" obj
        attributes <- readPropNUM "attributes" obj
        executorIds <- readPropNUM "executor_ids" obj
        pure <<< Offer $
            { id: id
            , frameworkId: frameworkId
            , slaveId: slaveId
            , hostname: hostname
            , url: url
            , resources: resources
            , attributes: attributes
            , executorIds: executorIds
            }

newtype Value a = Value a

instance valueAsForeign :: (AsForeign a) => AsForeign (Value a) where
    write (Value v) = toForeign $ { value: write v }

instance valueIsForeign :: (IsForeign a) => IsForeign (Value a) where
    read obj = do
        v <- readProp "value" obj
        pure $ Value v

derive instance valueNewtype :: Newtype (Value a) _

type OfferID = Value String
type FrameworkID = Value String
type ExecutorID = Value String
type SlaveID = Value String
type AgentID = SlaveID
type TaskID = Value String
type Text = Value String
type Scalar = Value Number

newtype FrameworkInfoCapability = FrameworkInfoCapability { ty :: Int
                                                          }

instance frameworkInfoCapabilityAsForeign :: AsForeign FrameworkInfoCapability where
    write (FrameworkInfoCapability obj) = writeObject props where
        props = [ "type" .= write obj.ty
                ]

instance frameworkInfoCapabilityIsForeign :: IsForeign FrameworkInfoCapability where
    read obj = do
        ty <- readProp "type" obj
        pure $ FrameworkInfoCapability { ty: ty
                                       }

newtype Label = Label { key :: String
                      , value :: Maybe String
                      }

instance labelAsForeign :: AsForeign Label where
    write (Label obj) = writeObject props where
        props = [ "key" .= write obj.key
                , "value" .= (write $ Undefined obj.value)
                ]

instance labelIsForeign :: IsForeign Label where
    read obj = do
        key <- readProp "key" obj
        value <- readPropNU "value" obj
        pure $ Label { key: key
                     , value: value
                     }

newtype Labels = Labels { labels :: Array Label
                        }

instance labelsAsForeign :: AsForeign Labels where
    write (Labels obj) =
        toForeign { labels: write obj.labels
                  }

instance labelsIsForeign :: IsForeign Labels where
    read obj = do
        labels <- readProp "labels" obj
        pure $ Labels { labels: labels
                      }

newtype FrameworkInfo = FrameworkInfo { user :: String
                                      , name :: String
                                      , id :: Maybe FrameworkID
                                      , failoverTimeout :: Maybe Number
                                      , checkpoint :: Maybe Boolean
                                      , role :: Maybe String
                                      , hostname :: Maybe String
                                      , principal :: Maybe String
                                      , webuiUrl :: Maybe String
                                      , capabilities :: Array FrameworkInfoCapability
                                      , labels :: Maybe Labels
                                      }

instance frameworkInfoAsForeign :: AsForeign FrameworkInfo where
    write (FrameworkInfo obj) = writeObject props where
        props = [ "user" .= write obj.user
                , "name" .= write obj.name
                , "id" .= (write $ Undefined obj.id)
                , "failover_timeout" .= (write $ Undefined obj.failoverTimeout)
                , "checkpoint" .= (write $ Undefined obj.checkpoint)
                , "role" .= (write $ Undefined obj.role)
                , "hostname" .= (write $ Undefined obj.hostname)
                , "principal" .= (write $ Undefined obj.principal)
                , "webui_url" .= (write $ Undefined obj.webuiUrl)
                , "capabilities" .= write obj.capabilities
                , "labels" .= (write $ Undefined obj.labels)
                ]
