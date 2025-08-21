SELECT
  cluster_profile, dataset_label, photon_enabled,
  COUNT(*) AS runs,
  percentile_approx(duration_sec, 0.5) AS p50_sec,
  percentile_approx(duration_sec, 0.9) AS p90_sec,
  percentile_approx(duration_sec, 0.95) AS p95_sec,
  AVG(duration_sec) AS mean_sec,
  STDDEV_SAMP(duration_sec) AS stddev_sec
FROM delta.`/Volumes/tabular/dataexpert/benchmarking_capstone/gold/benchmark_results_v2`
GROUP BY cluster_profile, dataset_label, photon_enabled
ORDER BY dataset_label, cluster_profile, photon_enabled;
