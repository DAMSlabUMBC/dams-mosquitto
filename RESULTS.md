# Priority-queue one-rep gate result

**VERDICT: PASS**

Broker: `dapbroker:mybranch` (branch dap-output-priority-queue, output-side priority queue, paper 5.2(ii)). Sweep: sets i–v, 20 cells x 1 rep, restart-before-each-cell. Judged by real per-cell data, not exit code.

| cell | type | valid | invalid | completion | coverage | leakage | FAR | FRR | data |
|---|---|---|---|---|---|---|---|---|---|
| v2_set1_static_1p_unified | data | 153424 | 0 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set1_static_10p_unified | data | 141532 | 0 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set1_static_100p_unified | data | 146888 | 0 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set2_dynamic_mp_10p_unified | data | 153408 | 0 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set2_dynamic_sp_10p_unified | data | 132382 | 2 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set2_dynamic_both_10p_unified | data | 199662 | 8 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set2_dynamic_mp_100p_unified | data | 129428 | 0 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set2_dynamic_sp_100p_unified | data | 130675 | 0 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set2_dynamic_both_100p_unified | data | 310368 | 15 | — | — | — | 0.0000 | 0.0000 | real |
| v2_set3_static_ops_1p_unified | ops | 148673 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0017 | real |
| v2_set3_static_ops_10p_unified | ops | 126343 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0010 | real |
| v2_set3_static_ops_100p_unified | ops | 136398 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0003 | real |
| v2_set4_dynamic_mp_10p_unified | ops | 124061 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0036 | real |
| v2_set4_dynamic_sp_10p_unified | ops | 134773 | 37 | 1.0000 | 1.0000 | 0.0000 | 0.0003 | 0.0007 | real |
| v2_set4_dynamic_both_10p_unified | ops | 180192 | 26 | 1.0000 | 1.0000 | 0.0000 | 0.0001 | 0.0031 | real |
| v2_set4_dynamic_mp_100p_unified | ops | 133653 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0017 | real |
| v2_set4_dynamic_sp_100p_unified | ops | 119555 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0000 | real |
| v2_set4_dynamic_both_100p_unified | ops | 319706 | 83 | 1.0000 | 1.0000 | 0.0000 | 0.0001 | 0.0033 | real |
| v2_set5_connectivity_10p_unified | ops | 120198 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0008 | real |
| v2_set5_connectivity_100p_unified | ops | 118782 | 0 | 1.0000 | 1.0000 | 0.0000 | 0.0000 | 0.0007 | real |

All 20 cells have real data (valid>0); every ops cell completion/coverage/leakage = 1.0/1.0/0.

_Data note: All cells real (no empty/zero-valid)._
