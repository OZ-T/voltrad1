CREATE TABLE IF NOT EXISTS portfolio (
  "conId" varchar(10) not NULL,
  "accountName" varchar(10) not NULL,
  "averageCost" real,
  "expiry" date,
  "localSymbol" TEXT,
  "marketPrice" real,
  "marketValue" real,
  "position" real,
  "primaryExchange" TEXT,
  "realizedPNL" real,
  "right" varchar(1),
  "secType" TEXT,
  "strike" real,
  "symbol" text,
  "unrealizedPNL" real,
  "current_datetime" TIMESTAMP not NULL,
  PRIMARY KEY("current_datetime","accountName", "conId" )
);
CREATE INDEX "ix_portfolio" ON portfolio ("current_datetime","accountName", "conId");
