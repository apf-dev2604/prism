# InplayV2 externalid mapping correction

- InplayV1: source JSON `transactionId` maps to production `externalid` (unchanged).
- InplayV2: source physical `public."GameTransactionInplayV2".id` maps to staging/production `externalid`.
- InplayV2 full DQ now independently reconciles `id` against staged `externalid` and checks bet, payout, computed jackpot contribution and payout.
- InplayV2 source `settled_datetime` continues to determine the PHT extraction window.
- Shared schemas: `etl_stage`, `db_admin`, `public.gametransaction_prod`; `fulletl=false` for V2.
- Do not resume a previously approved/released window after changing external IDs; use a controlled reprocessing plan.
- D2S scripts require no changes for this correction.
