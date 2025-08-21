WITH metrics AS (
  SELECT
    cluster_profile,
    dataset_label,
    photon_enabled,
    median_sec,
    median_cost_usd,
    median_rows,
    median_cost_per_million,
    median_rows_per_sec
  FROM benchmark_txn.stats
)
SELECT
  -- ordering keys for charts
  CASE cluster_profile
    WHEN 'dev_small'  THEN 1
    WHEN 'dev_medium' THEN 2
    WHEN 'dev_large'  THEN 3
  END AS cluster_order,
  CASE dataset_label
    WHEN 'small_9m'   THEN 1
    WHEN 'large_142m' THEN 2
    WHEN 'xl_250m'    THEN 3
  END AS ds_order,

  -- display fields
  cluster_profile,
  dataset_label       AS dataset_size,
  CASE WHEN photon_enabled THEN 'Photon On' ELSE 'Photon Off' END AS photon_status,

  -- metrics
  ROUND(median_cost_per_million, 6) AS median_cost_per_million,
  ROUND(median_sec, 2)              AS median_sec,
  ROUND(median_rows_per_sec, 0)     AS median_rows_per_sec
FROM metrics
ORDER BY ds_order, cluster_order, photon_status;
