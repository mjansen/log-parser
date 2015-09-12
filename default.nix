{ mkDerivation, attoparsec, base, binary, bytestring, cereal
, containers, deepseq, directory, filepath, stdenv, time
, timezone-olson, timezone-series, zlib
}:
mkDerivation {
  pname = "log-parser";
  version = "0.1.0.0";
  src = ./.;
  buildDepends = [
    attoparsec base binary bytestring cereal containers deepseq
    directory filepath time timezone-olson timezone-series zlib
  ];
  description = "Basic parsing for syslog";
  license = stdenv.lib.licenses.mit;
}
