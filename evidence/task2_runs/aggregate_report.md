### dev / reference  (3 runs, 3 OK)

| rep | status | total stage s | wall s | peak primary MB | peak YARN MB | output GiB (x2) |
|---|---|---|---|---|---|---|
| 1 | OK | 475.4 | 506 | 8026 | 24576 | 0.92 |
| 2 | OK | 475.3 | 506 | 8319 | 24576 | 0.92 |
| 3 | OK | 480.4 | 510 | 7936 | 24576 | 0.92 |

| stage | rep1 | rep2 | rep3 | median | min | max |
|---|---|---|---|---|---|---|
| gdelt_streaming | 84.3 | 83.3 | 82.3 | 83.3 | 82.3 | 84.3 |
| hive_raw_tables | 9.9 | 9.9 | 10.7 | 9.9 | 9.9 | 10.7 |
| hive_clean_prices | 27.3 | 28.4 | 27.2 | 27.3 | 27.2 | 28.4 |
| hive_price_splits | 25.6 | 26.6 | 25.2 | 25.6 | 25.2 | 26.6 |
| hive_clean_gdelt | 142.0 | 145.3 | 147.9 | 145.3 | 142.0 | 147.9 |
| hive_reconcile | 36.2 | 34.5 | 34.3 | 34.5 | 34.3 | 36.2 |
| spark_features | 45.6 | 42.1 | 42.0 | 42.1 | 42.0 | 45.6 |
| spark_validate | 31.1 | 30.2 | 31.4 | 31.1 | 30.2 | 31.4 |
| train | 73.4 | 75.0 | 79.4 | 75.0 | 73.4 | 79.4 |
| TOTAL (sum of stages) | 475.4 | 475.3 | 480.4 | 475.4 | 475.3 | 480.4 |

### full / reference  (3 runs, 3 OK)

| rep | status | total stage s | wall s | peak primary MB | peak YARN MB | output GiB (x2) |
|---|---|---|---|---|---|---|
| 1 | OK | 1065.2 | 1095 | 8331 | 24576 | 9.72 |
| 2 | OK | 1071.3 | 1102 | 8312 | 24576 | 9.72 |
| 3 | OK | 1069.4 | 1101 | 8424 | 24576 | 9.72 |

| stage | rep1 | rep2 | rep3 | median | min | max |
|---|---|---|---|---|---|---|
| gdelt_streaming | 330.2 | 335.9 | 335.1 | 335.1 | 330.2 | 335.9 |
| hive_raw_tables | 11.6 | 10.1 | 10.4 | 10.4 | 10.1 | 11.6 |
| hive_clean_prices | 34.9 | 35.5 | 37.4 | 35.5 | 34.9 | 37.4 |
| hive_price_splits | 26.4 | 27.9 | 29.7 | 27.9 | 26.4 | 29.7 |
| hive_clean_gdelt | 405.0 | 405.4 | 403.5 | 405.0 | 403.5 | 405.4 |
| hive_reconcile | 77.5 | 80.0 | 79.3 | 79.3 | 77.5 | 80.0 |
| spark_features | 56.7 | 55.0 | 54.8 | 55.0 | 54.8 | 56.7 |
| spark_validate | 32.0 | 32.6 | 31.6 | 32.0 | 31.6 | 32.6 |
| train | 90.9 | 88.9 | 87.6 | 88.9 | 87.6 | 90.9 |
| TOTAL (sum of stages) | 1065.2 | 1071.3 | 1069.4 | 1069.4 | 1065.2 | 1071.3 |

### Output fingerprint check (identical inputs must give identical outputs)

- dev: 3 runs compared with dev_reference_rep1_20261120T103154Z -> IDENTICAL
- full: 3 runs compared with full_reference_rep1_20261120T112117Z -> IDENTICAL
