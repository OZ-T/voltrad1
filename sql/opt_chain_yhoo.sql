CREATE TABLE IF NOT EXISTS "OPTIONS_CHAIN_YHOO" (
"Quote_Time" TIMESTAMP not NULL,
  "Strike" REAL not NULL,
  "Type" TEXT not NULL,
  "Symbol" TEXT not NULL,
  "Last" REAL,
  "Bid" REAL,
  "Ask" REAL,
  "Chg" REAL,
  "PctChg" REAL,
  "Vol" REAL,
  "Open_Int" REAL,
  "IV" REAL,
  "Root" TEXT,
  "IsNonstandard" INTEGER,
  "Underlying" TEXT,
  "Underlying_Price" REAL,
  "Last_Trade_Date" TIMESTAMP,
  "Expiry_txt" TEXT not NULL,
   PRIMARY KEY("Quote_Time","Symbol", "Expiry_txt","Strike", "Type" )
);
CREATE INDEX "ix_OPTIONS_CHAIN_YHOO_Quote_Time"ON "OPTIONS_CHAIN_YHOO" ("Quote_Time");
