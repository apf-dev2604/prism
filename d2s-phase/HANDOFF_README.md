# D2S to Gold handoff (2026-11-04)

InplayV1 retains its existing MySQL extraction -> PostgreSQL Raw load -> Raw DQ processing. After Raw DQ PASS, `egms_gametx_dqraw.py` launches `run_imp_nexusall.sh FROM TO gameTransaction --brands inplayv1` asynchronously. The existing Raw DQ pass gate remains. Gold-only InplayV2 routes through `run_full_etl.sh` or `run_etl.sh` directly to `run_imp_nexusall.sh FROM TO gameTransaction --brands inplayv2` and bypasses V1 extraction. InplayV2 walletTransaction returns NOT AVAILABLE YET.

Requires the existing external `$HOME/scripts/prism/common/config/load_config.sh`, Python venv, and the correct `GOLD_PHASE` environment setting. The source D2S attachment did not contain a `config.toml`; no shared settings were replaced.

Example:
`bash run_full_etl.sh --from 2026-05-26 --to 2026-05-27 --brand inplayv2 --jmp-task full`
`bash run_full_etl.sh --from 2024-11-05 --to 2024-11-06 --brand inplayv1 --jmp-task full`
