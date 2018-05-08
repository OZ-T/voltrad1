CREATE TABLE IF NOT EXISTS "OPTIONS_CHAIN_YHOO" (
"Quote_Time" TIMESTAMP not NULL,
  "Strike" REAL not NULL,
  "Type" varchar(1) not NULL,
  "Last" REAL,
  "Bid" REAL,
  "Ask" REAL,
  "Chg" REAL,
  "PctChg" REAL,
  "Vol" REAL,
  "Open_Int" REAL,
  "IV" REAL,
  "Underlying" varchar(4)  not NULL,
  "Underlying_Price" REAL,
  "Last_Trade_Date" TIMESTAMP,
  "Expiry" date not NULL,
   PRIMARY KEY("Quote_Time","Underlying", "Expiry","Strike", "Type" )
);
CREATE INDEX "ix_OPTIONS_CHAIN_YHOO_Quote_Time"ON "OPTIONS_CHAIN_YHOO" ("Quote_Time");
