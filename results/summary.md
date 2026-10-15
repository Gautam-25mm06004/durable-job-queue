# Results

## Throughput vs worker processes (1,000 jobs x 5 ms)

| workers | jobs | seconds | jobs_per_s | done |
|---|---|---|---|---|
| 1 | 1000 | 6.51 | 153.6 | 1000 |
| 2 | 1000 | 3.83 | 261.1 | 1000 |
| 4 | 1000 | 2.43 | 411.5 | 1000 |
| 8 | 1000 | 1.73 | 578.0 | 1000 |
| 16 | 1000 | 1.46 | 684.9 | 1000 |

## Worker crashes after the work, before the ack (1,000 jobs, 4 workers, 0.3 s lease)

| crash_prob_per_job | worker_crashes | done | stuck | executions | jobs_run_twice_or_more | transactional_effects (rows) | naive_effects (rows) | naive_duplicates | wrong_results | seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.0 | 0 | 1000 | 0 | 1000 | 0 | 1000 | 1000 | 0 | 0 | 1.17 |
| 0.01 | 9 | 1000 | 0 | 1009 | 9 | 1000 | 1009 | 9 | 0 | 1.54 |
| 0.05 | 53 | 1000 | 0 | 1053 | 47 | 1000 | 1053 | 53 | 0 | 2.51 |
| 0.1 | 124 | 1000 | 0 | 1124 | 110 | 1000 | 1124 | 124 | 0 | 3.37 |
| 0.2 | 248 | 1000 | 0 | 1248 | 198 | 1000 | 1248 | 248 | 0 | 5.55 |

## Retries and dead-lettering

| jobs | poison_jobs | transient_failure_prob | done | dead_lettered | stuck | executions | seconds |
|---|---|---|---|---|---|---|---|
| 1000 | 20 | 0.2 | 980 | 20 | 0 | 1230 | 2.38 |
