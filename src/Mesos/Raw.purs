module Mesos.Raw where

import Prelude
import Control.Monad.Error.Class (throwError)
import Data.Foreign (F, Foreign, ForeignError(..), toForeign, writeObject)
import Data.Foreign.Class (class AsForeign, class IsForeign, read, write, readProp, (.=))
import Data.Foreign.Index (prop)
import Data.Foreign.NullOrUndefined (readNullOrUndefined, unNullOrUndefined)
import Data.Foreign.Undefined (Undefined(..))
import Data.Maybe (Maybe, fromMaybe)
import Data.Monoid (class Monoid, mempty)
import Data.List.NonEmpty as NEL

readPropNU :: forall a. (IsForeign a) => String -> Foreign -> F (Maybe a)
readPropNU p v = unNullOrUndefined <$> (prop p v >>= readNullOrUndefined read)

readPropNUM :: forall a. (IsForeign a, Monoid a) => String -> Foreign -> F a
readPropNUM p v = fromMaybe mempty <$> readPropNU p v

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
        , type: 0
        , scalar: write s
        }
    write (Attribute name (RangesAttribute ranges)) = toForeign $
        { name: name
        , type: 1
        , ranges: write ranges
        }
    write (Attribute name (SetAttribute set)) = toForeign $
        { name: name
        , type: 2
        , set: write set
        }
    write (Attribute name (TextAttribute text)) = toForeign $
        { name: name
        , type: 3
        , text: write text
        }

instance attributeIsForeign :: IsForeign Attribute where
    read obj = do
        name <- readProp "name" obj
        v <- readProp "type" obj >>= readAttributeType
        pure $ Attribute name v
        where
            readAttributeType 0 = ScalarAttribute <$> readProp "scalar" obj
            readAttributeType 1 = RangesAttribute <$> readProp "ranges" obj
            readAttributeType 2 = SetAttribute <$> readProp "set" obj
            readAttributeType 3 = TextAttribute <$> readProp "text" obj
            readAttributeType t = throwError <<< NEL.singleton <<< ForeignError $ "Unknown attribute type " <> show t

data ResourceValue = ScalarResource Scalar
                   | RangesResource Ranges
                   | SetResource Set

data Resource = Resource String ResourceValue

instance resourceAsForeign :: AsForeign Resource where
    write (Resource name (ScalarResource s)) = toForeign $
        { name: name
        , type: 0
        , scalar: write s
        }
    write (Resource name (RangesResource ranges)) = toForeign $
        { name: name
        , type: 1
        , ranges: write ranges
        }
    write (Resource name (SetResource set)) = toForeign $
        { name: name
        , type: 2
        , set: write set
        }

instance resourceIsForiegn :: IsForeign Resource where
    read obj = do
        name <- readProp "name" obj
        v <- readProp "type" obj >>= readResourceType
        pure $ Resource name v
        where
            readResourceType 0 = ScalarResource <$> readProp "scalar" obj
            readResourceType 1 = RangesResource <$> readProp "ranges" obj
            readResourceType 2 = SetResource <$> readProp "set" obj
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

newtype Parameter = Parameter
    { key :: String
    , value :: String
    }

instance parameterAsForeign :: AsForeign Parameter where
    write (Parameter obj) = toForeign obj

instance parameterIsForeign :: IsForeign Parameter where
    read obj = do
        key <- readProp "key" obj
        value <- readProp "value" obj
        pure $ Parameter { key: key
                         , value: value
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
                , "slave_id" .= write obj.slaveId
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
        slaveId <- readProp "slave_id" obj
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

type OfferID = Value String
type FrameworkID = Value String
type ExecutorID = Value String
type SlaveID = Value String
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
