CREATE TABLE IF NOT EXISTS "yahoo_ecocal" (
"index" INTEGER,
  "after_release_actual" TEXT,
  "consensus_estimate" TEXT,
  "country_code" TEXT,
  "econ_release" TEXT,
  "gmtOffsetMilliSeconds" INTEGER,
  "originally_reported_actual" TEXT,
  "period" TEXT,
  "prior_release_actual" TEXT,
  "startdatetime" TEXT
);
CREATE INDEX "ix_yahoo_ecocal_index"ON "yahoo_ecocal" ("index");
