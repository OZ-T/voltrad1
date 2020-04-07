CREATE TABLE IF NOT EXISTS "yahoo_earnings" (
"index" INTEGER,
  "companyshortname" TEXT,
  "epsactual" REAL,
  "epsestimate" REAL,
  "epssurprisepct" REAL,
  "gmtOffsetMilliSeconds" INTEGER,
  "startdatetime" TEXT,
  "startdatetimetype" TEXT,
  "ticker" TEXT
);
CREATE INDEX "ix_yahoo_earnings_index"ON "yahoo_earnings" ("index");
