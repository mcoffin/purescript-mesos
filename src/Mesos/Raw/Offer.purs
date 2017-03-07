module Mesos.Raw.Offer where

import Prelude
import Control.Monad.Error.Class (throwError)
import Data.Foreign (ForeignError(..), toForeign)
import Data.Foreign.Class (class AsForeign, class IsForeign, write, readProp)
import Data.Foreign.Index (prop)
import Data.List.NonEmpty as NEL
import Mesos.Raw (Resource, TaskInfo, ExecutorInfo, TaskGroupInfo, readPropNUM)

data Operation = LaunchOperation (Array TaskInfo)
               | LaunchGroupOperation ExecutorInfo TaskGroupInfo
               | ReserveOperation (Array Resource)
               | UnreserveOperation (Array Resource)
               | CreateOperation (Array Resource)
               | DestroyOperation (Array Resource)

instance operationAsForeign :: AsForeign Operation where
    write (LaunchOperation taskInfos) = toForeign $
        { type: "LAUNCH"
        , launch: { task_infos: write taskInfos }
        }
    write (LaunchGroupOperation executorInfo taskGroupInfo) = toForeign $
        { type: "LAUNCH_GROUP"
        , launch_group: { executor: write executorInfo, task_group: write taskGroupInfo }
        }
    write (ReserveOperation resources) = toForeign $
        { type: "RESERVE"
        , reserve: { resources: write resources }
        }
    write (UnreserveOperation resources) = toForeign $
        { type: "UNRESERVE"
        , unreserve: { resources: write resources }
        }
    write (CreateOperation volumes) = toForeign $
        { type: "CREATE"
        , create: { volumes: write volumes }
        }
    write (DestroyOperation volumes) = toForeign $
        { type: "DESTROY"
        , destroy: { volumes: write volumes }
        }

instance operationIsForeign :: IsForeign Operation where
    read obj = readProp "type" obj >>= readOperationType where
        readOperationType "LAUNCH" = LaunchOperation <$> (prop "launch" obj >>= readPropNUM "task_infos")
        readOperationType "LAUNCH_GROUP" = do
            launchGroup <- prop "launch_group" obj
            executorInfo <- readProp "executor" launchGroup
            taskGroupInfo <- readProp "task_group" launchGroup
            pure $ LaunchGroupOperation executorInfo taskGroupInfo
        readOperationType "RESERVE" = ReserveOperation <$> (prop "reserve" obj >>= readPropNUM "resources")
        readOperationType "UNRESERVE" = UnreserveOperation <$> (prop "unreserve" obj >>= readPropNUM "resources")
        readOperationType "CREATE" = CreateOperation <$> (prop "create" obj >>= readPropNUM "volumes")
        readOperationType "DESTROY" = DestroyOperation <$> (prop "destroy" obj >>= readPropNUM "volumes")
        readOperationType t = throwError <<< NEL.singleton <<< ForeignError $ "Unknown operation type: " <> t
