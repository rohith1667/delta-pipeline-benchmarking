WITH med AS (
  SELECT cluster_profile, dataset_label, photon_enabled,
         percentile_approx(duration_sec, 0.5) AS median_sec
  FROM delta.`/Volumes/tabular/dataexpert/benchmarking_capstone/gold/benchmark_results_v2`
  GROUP BY cluster_profile, dataset_label, photon_enabled
),
p AS (
  SELECT
    cluster_profile, dataset_label,
    MAX(CASE WHEN photon_enabled THEN median_sec END)  AS on_sec,
    MAX(CASE WHEN NOT photon_enabled THEN median_sec END) AS off_sec
  FROM med
  GROUP BY cluster_profile, dataset_label
)
SELECT cluster_profile, dataset_label,
       on_sec, off_sec, ROUND(off_sec / on_sec, 2) AS speedup_x
FROM p
ORDER BY dataset_label, cluster_profile;
