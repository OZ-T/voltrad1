CREATE TABLE IF NOT EXISTS "AAPL" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_AAPL_date"ON "AAPL" ("date");
CREATE TABLE IF NOT EXISTS "AMZN" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_AMZN_date"ON "AMZN" ("date");
CREATE TABLE IF NOT EXISTS "ES" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_ES_date"ON "ES" ("date");
CREATE TABLE IF NOT EXISTS "GOOG" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_GOOG_date"ON "GOOG" ("date");
CREATE TABLE IF NOT EXISTS "IWM" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_IWM_date"ON "IWM" ("date");
CREATE TABLE IF NOT EXISTS "NDX" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_NDX_date"ON "NDX" ("date");
CREATE TABLE IF NOT EXISTS "ORCL" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_ORCL_date"ON "ORCL" ("date");
CREATE TABLE IF NOT EXISTS "QQQ" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_QQQ_date"ON "QQQ" ("date");
CREATE TABLE IF NOT EXISTS "SPX" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_SPX_date"ON "SPX" ("date");
CREATE TABLE IF NOT EXISTS "SPY" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_SPY_date"ON "SPY" ("date");
CREATE TABLE IF NOT EXISTS "USO" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_USO_date"ON "USO" ("date");
CREATE TABLE IF NOT EXISTS "VIX" (
"date" TEXT,
  "WAP" REAL,
  "close" REAL,
  "count" INTEGER,
  "hasGaps" INTEGER,
  "high" REAL,
  "low" REAL,
  "open" REAL,
  "reqId" INTEGER,
  "volume" INTEGER,
  "symbol" TEXT,
  "expiry" TEXT,
  "type" TEXT,
  "load_dttm" TEXT
);
CREATE INDEX "ix_VIX_date"ON "VIX" ("date");
