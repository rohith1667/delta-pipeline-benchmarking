
SELECT
  cluster_profile,
  MAX(CASE WHEN photon_enabled = FALSE THEN median_cost_per_million END) AS `Photon Off`,
  MAX(CASE WHEN photon_enabled = TRUE  THEN median_cost_per_million END) AS `Photon On`
FROM benchmark_txn.stats
WHERE dataset_label = 'small_9m'
GROUP BY cluster_profile
ORDER BY cluster_profile;

-- File: sql/04b_cost_per_million_large_pivot.sql
SELECT
  cluster_profile,
  MAX(CASE WHEN photon_enabled = FALSE THEN median_cost_per_million END) AS `Photon Off`,
  MAX(CASE WHEN photon_enabled = TRUE  THEN median_cost_per_million END) AS `Photon On`
FROM benchmark_txn.stats
WHERE dataset_label = 'large_142m'
GROUP BY cluster_profile
ORDER BY cluster_profile;

-- File: sql/04c_cost_per_million_xl_pivot.sql
SELECT
  cluster_profile,
  MAX(CASE WHEN photon_enabled = FALSE THEN median_cost_per_million END) AS `Photon Off`,
  MAX(CASE WHEN photon_enabled = TRUE  THEN median_cost_per_million END) AS `Photon On`
FROM benchmark_txn.stats
WHERE dataset_label = 'xl_250m'
GROUP BY cluster_profile
ORDER BY cluster_profile;
