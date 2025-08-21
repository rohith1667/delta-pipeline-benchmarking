
CREATE OR REPLACE VIEW benchmark_txn.stats AS
SELECT
  cluster_profile,
  dataset_label,
  photon_enabled,
  percentile_approx(duration_sec, 0.5) AS median_sec,
  percentile_approx(cost_usd, 0.5)     AS median_cost_usd,
  percentile_approx(rows_in, 0.5)      AS median_rows,
  COUNT(*)                              AS runs,
  -- derived:
  percentile_approx(cost_usd, 0.5) / (NULLIF(percentile_approx(rows_in, 0.5),0) / 1e6)
    AS median_cost_per_million,
  NULLIF(percentile_approx(rows_in, 0.5),0) / NULLIF(percentile_approx(duration_sec, 0.5),0)
    AS median_rows_per_sec
FROM delta.`/Volumes/tabular/dataexpert/benchmarking_capstone/gold/benchmark_results_v2`
GROUP BY cluster_profile, dataset_label, photon_enabled;
