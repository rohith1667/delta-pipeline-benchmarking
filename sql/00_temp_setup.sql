-- Run this first to load the sample CSV into a temp view

CREATE OR REPLACE TEMP VIEW benchmark_logs
USING CSV
OPTIONS (
  path 'dbfs:/FileStore/bench/sample_benchmark_logs.csv',
  header 'true',
  inferSchema 'true'
);

-- Optional: create the stats view for convenience
CREATE OR REPLACE TEMP VIEW stats AS
SELECT
  cluster_profile,
  dataset_label,
  photon_enabled,
  percentile_approx(duration_sec, 0.5) AS median_sec,
  percentile_approx(cost_usd, 0.5)     AS median_cost_usd,
  percentile_approx(rows_in, 0.5)      AS median_rows,
  COUNT(*)                              AS runs,
  percentile_approx(cost_usd, 0.5) / (NULLIF(percentile_approx(rows_in, 0.5),0) / 1e6) AS median_cost_per_million,
  NULLIF(percentile_approx(rows_in, 0.5),0) / NULLIF(percentile_approx(duration_sec, 0.5),0) AS median_rows_per_sec
FROM benchmark_logs
GROUP BY cluster_profile, dataset_label, photon_enabled;
