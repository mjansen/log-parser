{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module LogParser
  ( LogEntry(..)
  , parseLine
  , parseTimeStamp
  , parseRepeat
  , contains
  , parseMessage
  , parseLogWith
  , convertLog
  , readLog
  , readSerializedLog
  ) where

import Control.Applicative
import Control.Monad (when)

import Data.Int

import Data.ByteString (ByteString)
import qualified Data.ByteString       as B
import qualified Data.ByteString.Lazy  as L
import qualified Data.ByteString.Char8 as BC
-- import qualified Data.ByteString.Lazy.Char8 as LC

import qualified Data.Attoparsec.ByteString.Char8 as P
-- import qualified Data.Attoparsec.ByteString.Lazy  as PL

import Data.Maybe
import Data.Either
import Data.Time
import Data.Time.LocalTime.TimeZone.Series
import Data.Time.LocalTime.TimeZone.Olson
import Data.Time.Calendar
import qualified Data.Map as Map

import Control.DeepSeq

import Data.Serialize

import GHC.Generics

import System.FilePath
import System.Directory
import System.IO

data LogEntry = LogEntry
  { le_timeStamp :: {-# UNPACK #-} !UTCTime
  , le_machine   :: {-# UNPACK #-} !ByteString
  , le_program   :: {-# UNPACK #-} !ByteString
  , le_pid       :: {-# UNPACK #-} !Int
  , le_message   :: {-# UNPACK #-} !ByteString
  } deriving (Eq, Ord, Show, Read, Generic)

instance NFData    LogEntry
instance Serialize LogEntry

------------------------------------------------------------------------

-- Unfortunately the timestamps in syslog files often require
-- context. They are expressed in local time and often do not have a
-- year.  This means that we require some context to guess what the
-- real timestamp is.

-- is is possible the the timezone file used when the log was written
-- is different from the one we are using to make this computation.
-- that means we are computing the wrong thing here.

data TimeStampInfo = TimeStampInfo
  { tsi_year       :: Maybe Integer
  , tsi_month      :: Maybe Int
  , tsi_day        :: Maybe Int
  , tsi_hour       :: Maybe Int
  , tsi_minute     :: Maybe Int
  , tsi_second     :: Maybe Int
  , tsi_partSecond :: Maybe Int
  , tsi_offset     :: Maybe Int
  } deriving (Eq, Ord, Show)

parseTimeStampStandard :: P.Parser TimeStampInfo
parseTimeStampStandard = do
  monthName <- P.take 3
  P.char ' '
  _ <- P.choice [ P.try (P.char ' ') >> return (), return () ]
  dayOfMonth <- P.decimal
  P.char ' '
  hour   <- P.decimal
  P.char ':'
  minute <- P.decimal
  P.char ':'
  second <- P.decimal
  let month = monthNameToIndex monthName
  return $ TimeStampInfo Nothing (Just month) (Just dayOfMonth) (Just hour) (Just minute) (Just second) Nothing Nothing

timeStampInfoToLocalTime :: TimeStampInfo -> LocalTime
timeStampInfoToLocalTime (TimeStampInfo (Just y) (Just m) (Just d) (Just h) (Just mn) (Just s) _ Nothing)
  = LocalTime (fromGregorian y m d) (TimeOfDay h mn (fromIntegral s))

timeStampInfoToLocalTimeForYear :: Integer -> TimeStampInfo -> LocalTime
timeStampInfoToLocalTimeForYear year tsi = timeStampInfoToLocalTime (tsi { tsi_year = Just year })

-- Is there a fixed timezone offset (in minutes) we can apply?
  
computeOffset :: TimeZoneSeries -> TimeStampInfo -> TimeStampInfo -> Maybe Int
computeOffset tzs
              (TimeStampInfo (Just y0) (Just m0) (Just d0) (Just h0) (Just mn0) (Just s0) _ Nothing)
              (TimeStampInfo (Just y1) (Just m1) (Just d1) (Just h1) (Just mn1) (Just s1) _ Nothing) =
  -- tzs <- getTimeZoneSeriesFromOlsonFile "/etc/localtime"
  let lt0 = LocalTime (fromGregorian y0 m0 d0) (TimeOfDay h0 mn0 (fromIntegral s0))
      lt1 = LocalTime (fromGregorian y1 m1 d1) (TimeOfDay h1 mn1 (fromIntegral s1))
      zst0 = localTimeToZoneSeriesTime tzs lt0
      zst1 = localTimeToZoneSeriesTime tzs lt1
      t0   = zoneSeriesTimeToUTC zst0
      t1   = zoneSeriesTimeToUTC zst1
      transitions = filter (<= t1) . filter (>= t0) . map fst . tzsTransitions $ tzs
  in case transitions of
    [] -> Just (timeZoneMinutes . zoneSeriesTimeZone $ zst0)
    _  -> Nothing
      

parseLocalTimeStamp :: TimeZoneSeries -> Integer -> P.Parser UTCTime
parseLocalTimeStamp tzs year = do
  monthName <- P.take 3
  P.char ' '
  _ <- P.choice [ P.try (P.char ' ') >> return (), return () ]
  dayOfMonth <- P.decimal
  P.char ' '
  hour   <- P.decimal
  P.char ':'
  minute <- P.decimal
  P.char ':'
  second <- P.decimal
  let month = monthNameToIndex monthName
      lt    = LocalTime (fromGregorian year month dayOfMonth) (TimeOfDay hour minute (fromIntegral second))
      t     = localTimeToUTC' tzs lt
  deepseq t . return $ t
  
------------------------------------------------------------------------

parseTimeStamp :: Integer -> NominalDiffTime -> P.Parser UTCTime
parseTimeStamp year offset = do
  monthName <- P.take 3
  P.char ' '
  _ <- P.choice [ P.try (P.char ' ') >> return (), return () ]
  dayOfMonth <- P.decimal
  P.char ' '
  hour   <- P.decimal
  P.char ':'
  minute <- P.decimal
  P.char ':'
  second <- P.decimal
  let month = monthNameToIndex monthName
  let t = addUTCTime offset $ UTCTime (fromGregorian year month dayOfMonth) (fromIntegral $ 60*(60*hour + minute) + second)
  deepseq t . return $ t
  
monthDB = Map.fromList . zip (map BC.pack . words $ "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec") $ [(1::Int)..]
                           
monthNameToIndex :: ByteString -> Int
monthNameToIndex str = fromJust $ Map.lookup str monthDB

parseLine :: P.Parser UTCTime -> P.Parser (UTCTime, ByteString, Maybe (ByteString, Int, ByteString))
parseLine parseTS = do
  ts  <- parseTS
  P.char ' '
  machine <- P.takeWhile (/= ' ')
  P.char ' '
  entry <- parseLogEntry'
  return (ts, machine, entry)

parseRepeat :: P.Parser Int
parseRepeat = do
  P.string $ BC.pack "last message repeated "
  count <- P.decimal
  P.string $ BC.pack " times"
  P.endOfInput
  return count

parseLogEntry' :: P.Parser (Maybe (ByteString, Int, ByteString))
parseLogEntry' = P.choice [ P.try parseRepeat >> return Nothing, fmap Just parseLogEntryL]

parseLogEntryL :: P.Parser (ByteString, Int, ByteString)
parseLogEntryL = do
  process <- P.takeWhile (/= '[')
  P.char '['
  pid <- P.decimal
  P.string (BC.pack "]: ")
  rol <- P.takeByteString
  return (process, pid, rol)

contains str (_, _, Just (_, _, msg)) = str `B.isInfixOf` msg
contains _ _ = False

parseMessage p (ts, machine, Just (process, pid, msg)) =
  case P.parseOnly p msg of
    Left message -> Nothing
    Right x      -> Just (ts, machine, process, pid, x)

parseLogWith :: P.Parser a -> [LogEntry] -> [(LogEntry, a)]
parseLogWith parser rs = rights . map (\e -> fmap (e,) . P.parseOnly parser . le_message $ e) $ rs

-- We assume that there was no timezone change for the log duration.
-- If there was we will get an error.  As we would like to stream the
-- data, we will not know until the end.

convertLog :: Integer -> FilePath -> FilePath -> IO ()
convertLog year fileName fixedName = do
  tzs  <- getTimeZoneSeriesFromOlsonFile "/etc/localtime"
  hIn  <- openFile fileName  ReadMode
  hOut <- openFile fixedName WriteMode
  let p = parseLocalTimeStamp tzs year
      loop = do
        ok <- not <$> hIsEOF hIn
        when ok $ do
          l <- BC.hGetLine hIn
          case P.parseOnly (parseLine p) l of
            Left msg -> print msg
            Right (ts, machine, Nothing) -> return ()
            Right (ts, machine, Just (program, pid, message)) -> do
              let str  = encode $ LogEntry ts machine program pid message
                  str' = (encode :: Int64 -> ByteString) (toEnum . fromEnum . B.length $ str)
              B.hPut hOut $ B.append str' str
          loop
  loop
  hClose hOut
  hClose hIn

convertLog2 :: Integer -> FilePath -> FilePath -> IO ()
convertLog2 year fileName fixedName = do
  hIn  <- openFile fileName  ReadMode
  hOut <- openFile fixedName WriteMode
  ok <- not <$> hIsEOF hIn
  when ok $ do
    tzs <- getTimeZoneSeriesFromOlsonFile "/etc/localtime"
    l <- BC.hGetLine hIn
    let Just offset = case P.parseOnly parseTimeStampStandard l of
          Left msg  -> error $ "cannot parse first timestamp in file " ++ fileName
          Right tsi -> computeOffset tzs (tsi { tsi_year = Just year}) (tsi { tsi_year = Just year })
        p = parseTimeStamp year (fromIntegral $ (-1) * offset * 60)
        loop l = do
          case P.parseOnly (parseLine p) l of
            Left msg -> print msg
            Right (ts, machine, Nothing) -> return ()
            Right (ts, machine, Just (program, pid, message)) -> do
              let str  = encode $ LogEntry ts machine program pid message
                  str' = (encode :: Int64 -> ByteString) (toEnum . fromEnum . B.length $ str)
              B.hPut hOut $ B.append str' str
          ok <- not <$> hIsEOF hIn
          when ok $ do
            BC.hGetLine hIn >>= loop
    loop l
  hClose hOut
  hClose hIn

readLog :: FilePath -> IO [LogEntry]
readLog fileName = do
  content <- L.readFile $ fileName <.> "fixed"
  let helper c | L.null c = []
               | otherwise = let Right (len :: Int64)  = decode . L.toStrict . L.take 8              $ c
                                 Right (e :: LogEntry) = decode . L.toStrict . L.take len . L.drop 8 $ c
                             in e:helper (L.drop (8 + len) c)
                                 
  return $ helper content

readSerializedLog :: FilePath -> IO [LogEntry]
readSerializedLog fileName = do
  content <- L.readFile fileName
  let helper c | L.null c = []
               | otherwise = let Right (len :: Int64)  = decode . L.toStrict . L.take 8              $ c
                                 Right (e :: LogEntry) = decode . L.toStrict . L.take len . L.drop 8 $ c
                             in e:helper (L.drop (8 + len) c)
                                 
  return $ helper content

------------------------------------------------------------------------

instance Serialize UTCTime where
    get = (\ a b -> UTCTime (toEnum a) (toEnum b)) <$> get <*> get
    put (UTCTime day secs) = do
      put . fromEnum $ day
      put . fromEnum $ secs
