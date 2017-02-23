module Test.Main where

import Prelude
import Control.Alt ((<|>))
import Control.Monad.Aff (runAff)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Class (liftEff)
import Control.Monad.Eff.Console (CONSOLE, logShow)
import Control.Monad.Eff.Exception (throw, throwException)
import Control.Monad.Error.Class (class MonadError, throwError)
import Data.Either (either)
import Data.Foldable (fold)
import Data.Int as I
import Data.Maybe (Maybe(..), maybe, fromMaybe)
import Data.Nullable (toMaybe)
import Data.Options (Options, (:=))
import Data.String (Pattern(..))
import Data.StrMap (StrMap, lookup)
import Data.Tuple (Tuple(..))
import Mesos.Scheduler (Subscribe(..), subscribe)
import Mesos.Raw (FrameworkInfo(..))
import Minimist (Arg(..), MinimistOptions, interpretAsStrings, parseArgs)
import Node.HTTP.Client (RequestOptions, auth, protocol, port, hostname)
import Node.Process (argv)
import Node.URL as URL

minimistOptions :: Options MinimistOptions
minimistOptions =
    fold [ interpretAsStrings := ["master", "principal", "secret"]
         ]

defaultProtocolPort :: String -> Maybe Int
defaultProtocolPort "http:" = Just 80
defaultProtocolPort "https:" = Just 443
defaultProtocolPort _ = Nothing

frameworkInfo :: FrameworkInfo
frameworkInfo = FrameworkInfo
    { user: "root"
    , name: "purescript-test-framework"
    , id: Nothing
    , failoverTimeout: Nothing
    , checkpoint: Nothing
    , role: Nothing
    , hostname: Nothing
    , principal: Nothing
    , webuiUrl: Nothing
    , capabilities: []
    , labels: Nothing
    }

main = do
    args <- flip parseArgs minimistOptions <$> argv
    Tuple _ masterUrlStr <- either throw pure $ manditoryArg "master" args >>= stringArg'
    let masterUrl = URL.parse masterUrlStr
    masterProtocol <- maybe (throw "Couldn't read master protocol from --master arg") pure $ toMaybe masterUrl.protocol
    masterPort <- maybe (throw "Couldn't derive master port form --master arg") pure $ (toMaybe masterUrl.port >>= I.fromString) <|> defaultProtocolPort masterProtocol
    masterHostname <- maybe (throw "Couldn't parse master hostname from --master arg") pure $ toMaybe masterUrl.hostname
    masterAuth <- maybe (throw "Couldn't parse auth info from args") pure $ authString args
    let subscribeOpts = fold $ [ protocol := masterProtocol
                               , port := masterPort
                               , hostname := masterHostname
                               , auth := masterAuth
                               ]
    runAff throwException (\_ -> pure unit) $ subscribe subscribeOpts (Subscribe { frameworkInfo: frameworkInfo }) (liftEff <<< logShow)
    where
        manditoryArg :: forall m. (MonadError String m) => String -> StrMap Arg -> m (Tuple String Arg)
        manditoryArg key args = Tuple key <$> (maybe e pure $ lookup key args) where
            e = throwError $ "--" <> key <> " argument is manditory"
        stringArg :: Arg -> Maybe String
        stringArg (ArgString s) = Just s
        stringArg _ = Nothing
        stringArg' :: forall m. (MonadError String m) => Tuple String Arg -> m (Tuple String String)
        stringArg' (Tuple key arg) = Tuple key <$> (maybe e pure $ stringArg arg) where
            e = throwError $ "--" <> key <> " expects a single string argument"
        authString :: StrMap Arg -> Maybe String
        authString args = do
            p <- lookup "principal" args >>= stringArg
            s <- lookup "secret" args >>= stringArg
            pure $ p <> ":" <> s
