# Gold Phase: DQ-only argparse fix and clean console output

- Removes the accidental duplicate `--dq-only` `argparse` registration in `helpers/strict_pipeline.py`.
- In `run_imp_nexusall.sh`, sends child command output and Python tracebacks to the existing timestamped `logs/nexus_*.log` file instead of printing the traceback on screen.
- On the terminal, failure now shows a single `[NEXUS ERROR]` line containing phase, exit status and the most recent recognizable error line, followed by `[NEXUS LOG]` with the full log location.
- Successful terminal runs print `[NEXUS OK]`, with the log location. The full detailed pipeline messages remain in the log.
- No change to source extraction, InplayV1/InplayV2 mappings, stage DQ, DQ-only vs publish boundaries, partition logic, or production writes.
- The previously interrupted DQ run may have left delivery-control state `VALIDATING` or `FAILED`. This patch does not modify that database state; check existing control records before rerunning.

Example:

```bash
bash run_imp_nexusall.sh 2026-05-26 2026-05-27 gameTransaction --brands inplayv2 --jmp-task dqstgprd
```

For live diagnostics:

```bash
ls -t logs/nexus_inplay_gameTransaction_2026-05-26_2026-05-27_*.log | head -1
```
