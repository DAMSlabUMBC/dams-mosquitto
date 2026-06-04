# Priority-queue FIVE-rep gate result

**VERDICT: PASS**

Broker: `dapbroker:mybranch` (branch dap-output-priority-queue). Sweep: sets i–v, 20 cells x 5 reps = 100 runs, restart-before-each-cell. Judged by real per-cell data across all reps, not exit code.

| cell | type | valid mean | valid spread | invalid mean | completion (reps) | coverage (reps) | leakage (reps) | FAR mean | FRR mean | data |
|---|---|---|---|---|---|---|---|---|---|---|
| v2_set1_static_1p_unified | data | 143640.8 | 142704..144728 | 0.0 | — | — | — | 0.0 | 0.0 | real |
| v2_set1_static_10p_unified | data | 134624.0 | 132432..137048 | 0.0 | — | — | — | 0.0 | 0.0 | real |
| v2_set1_static_100p_unified | data | 126966.4 | 124592..128832 | 0.0 | — | — | — | 0.0 | 0.0 | real |
| v2_set2_dynamic_mp_10p_unified | data | 132920.8 | 130644..135388 | 0.0 | — | — | — | 0.0 | 0.0 | real |
| v2_set2_dynamic_sp_10p_unified | data | 131617.8 | 123767..138767 | 15.6 | — | — | — | 0.0001 | 0.0 | real |
| v2_set2_dynamic_both_10p_unified | data | 191254.8 | 187262..199380 | 14.0 | — | — | — | 0.0001 | 0.0 | real |
| v2_set2_dynamic_mp_100p_unified | data | 132192.0 | 129880..134292 | 0.0 | — | — | — | 0.0 | 0.0 | real |
| v2_set2_dynamic_sp_100p_unified | data | 120646.8 | 116180..123863 | 0.0 | — | — | — | 0.0 | 0.0 | real |
| v2_set2_dynamic_both_100p_unified | data | 336869.8 | 307567..353447 | 16.0 | — | — | — | 0.0 | 0.0 | real |
| v2_set3_static_ops_1p_unified | ops | 142145.4 | 139273..145136 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0028 | real |
| v2_set3_static_ops_10p_unified | ops | 132712.2 | 126357..136530 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0017 | real |
| v2_set3_static_ops_100p_unified | ops | 128474.6 | 121857..135724 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0005 | real |
| v2_set4_dynamic_mp_10p_unified | ops | 129914.0 | 123032..133039 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0028 | real |
| v2_set4_dynamic_sp_10p_unified | ops | 128256.0 | 123925..134219 | 36.8 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0003 | 0.0008 | real |
| v2_set4_dynamic_both_10p_unified | ops | 190796.2 | 181243..199717 | 59.8 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0002 | 0.0031 | real |
| v2_set4_dynamic_mp_100p_unified | ops | 127874.4 | 123748..131916 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0017 | real |
| v2_set4_dynamic_sp_100p_unified | ops | 126774.6 | 120091..144747 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0001 | real |
| v2_set4_dynamic_both_100p_unified | ops | 325969.4 | 304453..357062 | 74.2 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0001 | 0.0028 | real |
| v2_set5_connectivity_10p_unified | ops | 117489.8 | 114544..123970 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0018 | real |
| v2_set5_connectivity_100p_unified | ops | 118532.2 | 113239..130113 | 0.0 | 1.0000..1.0000 | 1.0000..1.0000 | 0.0000..0.0000 | 0.0 | 0.0005 | real |

All 20 cells across all 5 reps have real data (valid>0); every ops cell completion/coverage/leakage = 1.0/1.0/0 in every rep.

_Data note: All cells real across all reps._
