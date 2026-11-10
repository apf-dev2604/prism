# Stored audit label alignment — 2026-11-04

The only executable change to D2S is the `MySQL-DLAKE` Raw DQ audit-row brand label from `inplayv1` to `inplay`; entity remains `gameTransaction`.
InplayV1 CLI routing, MySQL extraction, Kemet destination, Raw DQ algorithm, `run_imp_nexusall.sh` handoff, and already-released fail-fast behavior are unchanged.
The matching Gold Phase upstream DQ gate now reads this label. Deploy both ZIP packages together.
