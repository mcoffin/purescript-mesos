module Mesos.Util where

import Prelude
import Control.Monad.Eff.Exception (Error, error)
import Control.Monad.Error.Class (class MonadError, throwError)
import Data.Foreign (F, Foreign, parseJSON)
import Data.Foreign.Class (class IsForeign, read)

foreign import jsonStringify :: Foreign -> String

-- | Parse an object from a JSON string
fromJSON :: forall a. (IsForeign a) => String -> F a
fromJSON str = parseJSON str >>= read

-- | Shortcut for throwing error
throwErrorS :: forall m a. (MonadError Error m) => String -> m a
throwErrorS = throwError <<< error
