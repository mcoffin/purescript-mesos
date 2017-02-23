module Mesos.Raw where

import Prelude
import Data.Foreign (F, Foreign, toForeign, writeObject)
import Data.Foreign.Class (class AsForeign, class IsForeign, read, write, readProp, (.=))
import Data.Foreign.Index (prop)
import Data.Foreign.NullOrUndefined (readNullOrUndefined, unNullOrUndefined)
import Data.Foreign.Undefined (Undefined(..))
import Data.Maybe (Maybe)

readPropNU :: forall a. (IsForeign a) => String -> Foreign -> F (Maybe a)
readPropNU p v = unNullOrUndefined <$> (prop p v >>= readNullOrUndefined read)

newtype FrameworkID = FrameworkID { value :: String
                                  }

instance frameworkIdAsForeign :: AsForeign FrameworkID where
    write (FrameworkID obj) = toForeign obj

instance frameworkIdIsForeign :: IsForeign FrameworkID where
    read obj = do
        value <- readProp "value" obj
        pure $ FrameworkID { value: value
                           }

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
