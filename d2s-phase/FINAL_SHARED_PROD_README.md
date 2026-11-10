# PRISM NEXUS combined Inplay production rollout (2026-11-04)

Two independent deployment archives: D2S and Gold. Deploy Gold at the actual GOLD_PHASE path configured for D2S; do not replace external common/config/config.toml or credentials.

## Common destination for game transactions
- Target DB: iestdbrds
- Staging: etl_stage.gametransaction_prod
- Gold audit / strict delivery control: db_admin
- Production: public.gametransaction_prod (existing monthly RANGE partitions)
- Production monthly partition preflight: existing public.ensure_gametx_partitions() via gametxprod_partition_maker.sh; requires dba_partition_maint configuration and permission.
- Both brands persist brand=Inplay. Distinguish with fulletl=true (InplayV1 historic) / false (InplayV2 active).
- V1 last reporting window: 2026-05-25 06:00 to 2026-05-26 06:00 exclusive.
- V2 first source day: 2026-05-26 midnight to 2026-05-27 midnight, Asia/Manila window via existing --time-pht-aware. Mapped target timestamps follow +6h UTC-labelled reporting convention once, before staging. V2 first production reporting window: 2026-05-26 06:00 to 2026-05-27 06:00 exclusive.
- V2 jackpot computed columns copy source jackpotContribution/jackpotPayout (no derived jackpot formula). No changes to V1 map.
- V2 wallet transaction: NOT AVAILABLE YET.

## Entry points
D2S InplayV1 full: bash run_full_etl.sh --from 2026-05-25 --to 2026-05-26 --brand inplayv1 --jmp-task full
Gold InplayV2 full: bash run_imp_nexusall.sh 2026-05-26 2026-05-27 gameTransaction --brands inplayv2
D2S InplayV2 Gold-only handoff: bash run_full_etl.sh --from 2026-05-26 --to 2026-05-27 --brand inplayv2 --jmp-task full
Gold verification only: bash run_imp_nexusall.sh 2026-05-26 2026-05-27 gameTransaction --brands inplayv2 --jmp-task posteval

## Safety and verification
- Shared production is LIVE-shaped. No automatic cleanup/reset and no truncation. Before deployment verify installed partition function and target table column definitions with your DBA.
- The V2 source-day DQ in this package checks row-count equality (source vs stage), staging duplicate/blank keys and other structural conditions; it DOES NOT independently compare source IDs, bets/payouts, or jackpots row-by-row. Do not call it a complete financial reconciliation.
- V2 source timestamp values are timestamptz but the developer's query selects using SETTLED_DATETIME AT TIME ZONE Asia/Manila. This package reproduces that specific business population; it is a legacy reporting-clock transformation, NOT a correction of true transaction instants.
- Existing DEV releases in dev_dba_* are separate; their DQ approvals are not valid approvals for new db_admin/public shared-table releases. Do not reuse old DQ run IDs or force backfill into already-released dates.
- D2S handles an already-released Gold window by terminating promptly instead of waiting for a new STRICT RELEASED marker.
- SQL, schema ownership, data, privileges and run-control states were not modified in any database. No live DB end-to-end test has been run.
