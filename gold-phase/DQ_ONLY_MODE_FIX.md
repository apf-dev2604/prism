# Corrected Gold Phase DQ mode (2026-11-04)

- `--jmp-task dqstgprd` performs validation and records DQ PASS only. It does **not** call `strict_publish.py`.
- `--jmp-task loadstgprd` continues to use existing explicit publication path.
- InplayV2 publication overlap check is limited to the requested production and staging time windows, with brand and `fulletl` conditions. InplayV1 source/mapping remains unchanged.
- Full default execution still runs staging, DQ, publication, and post-release when a new release is made.
- Window-scoped collision checks do not guarantee cross-window global ID uniqueness.
- Database execution has not been performed.

Example after staging:

```bash
bash run_imp_nexusall.sh 2026-05-26 2026-05-27 gameTransaction --brands inplayv2 --jmp-task dqstgprd
bash run_imp_nexusall.sh 2026-05-26 2026-05-27 gameTransaction --brands inplayv2 --jmp-task loadstgprd
bash run_imp_nexusall.sh 2026-05-26 2026-05-27 gameTransaction --brands inplayv2 --jmp-task posteval
```
