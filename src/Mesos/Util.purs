module Mesos.Util where

import Prelude
import Data.Foreign (Foreign)

foreign import jsonStringify :: Foreign -> String
