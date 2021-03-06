CREATE TABLE IF NOT EXISTS options_chain_yhoo (
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
CREATE INDEX "ix_OPTIONS_CHAIN_YHOO_Quote_Time"ON options_chain_yhoo ("Quote_Time");



-- This creates a hypertable that is partitioned by time
--   using the values in the `time` column.
SELECT create_hypertable('options_chain_yhoo', 'Quote_Time');
-- USING https://docs.timescale.com/v0.9/getting-started/creating-hypertables
-- RUN THE following en una session de sudo -u postgres psql
-- SELECT create_hypertable("OPTIONS_CHAIN_YHOO", "Quote_Time");
