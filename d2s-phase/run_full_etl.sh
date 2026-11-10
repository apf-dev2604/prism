
#!/usr/bin/env bash
# PRISM sequential ETL controller. Does not alter existing processing scripts.
set -Eeuo pipefail
umask 077
source "$HOME/scripts/prism/common/config/load_config.sh" || exit 1

PRISM_ROOT="${BASE_DIR:-$HOME/scripts/prism}"
D2S="${DIRECT2SILVER_PHASE:-$PRISM_ROOT/d2s-phase}"
GOLD="${GOLD_PHASE:-$PRISM_ROOT/gold-phase}"
LOG_ROOT="${BASE_LOG_DIR:-$D2S/logs}"
PYTHON_BIN="${VENV_DIR:-$HOME/scripts/.venv}/bin/python"
[[ "$PYTHON_BIN" == '~/'* ]] && PYTHON_BIN="$HOME/${PYTHON_BIN#\~/}"
[[ -x "$PYTHON_BIN" ]] || { echo "[ERROR] Missing Python: $PYTHON_BIN" >&2; exit 2; }
[[ -f "$D2S/run_etl.sh" ]] || { echo "[ERROR] Missing $D2S/run_etl.sh" >&2; exit 2; }

# Avoid relying on optional configuration variable BASE_DIR for log placement.
CONTROLLER_LOG_DIR="$LOG_ROOT/full_etl"
mkdir -p "$CONTROLLER_LOG_DIR"
LOCK="$CONTROLLER_LOG_DIR/run_full_etl.lock"
exec 9>"$LOCK"
flock -n 9 || { echo '[ERROR] Another run_full_etl controller is active.' >&2; exit 75; }

# Argument validation and monitoring are handled inside Python only. This is
# orchestration: no source/target transaction rows are retrieved or modified.
exec "$PYTHON_BIN" - "$D2S" "$GOLD" "$LOG_ROOT" "$CONTROLLER_LOG_DIR" "$@" <<'PY'
import argparse
import datetime as dt
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import logging

D2S, GOLD, LOG_ROOT, LOG_DIR = map(Path, sys.argv[1:5])
MODES = ('full', 'all', 'extract', 'load', 'loaddq', 'rawdq', 'extractloaddq')
p = argparse.ArgumentParser(description='PRISM sequential windows; stop unless each window finishes its selected stage')
dates = p.add_mutually_exclusive_group(required=True)
dates.add_argument('--dates', help='Comma-separated business dates, e.g. 2024-02-01,2024-02-03')
dates.add_argument('--from', dest='date_from', help='First business date (inclusive), requires --to')
p.add_argument('--to', dest='date_to', help='Exclusive end date with --from')
p.add_argument('--jmp-task', choices=MODES + ('loadstg','dqstgprd','loadstgprd','posteval'), default='full')
p.add_argument('--brand', '--brands', '-brand', '-brands', dest='brand', default='inplayv1')
p.add_argument('--entity', choices=('gameTransaction','walletTransaction'), default='gameTransaction')
p.add_argument('--poll-seconds', type=int, default=30)
p.add_argument('--timeout-minutes', type=int, default=240)
p.add_argument('--archive', help='Exact existing .jsonl.gz archive for a SINGLE rawdq business date; bypasses ambiguous wrapper discovery')
p.add_argument('--archive-template', help='For rawdq date lists/ranges: exact archive path template with {date} (YYYYMMDD)')
p.add_argument('--no-email', action='store_true', help='Disable controller failure alert; underlying scripts retain their own behavior')
a = p.parse_args(sys.argv[5:])
if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', a.brand):
    p.error('Invalid brand key')
if a.entity == 'walletTransaction':
    print('[NOT AVAILABLE YET] %s walletTransaction (deposits/withdrawals)' % a.brand, flush=True)
    sys.exit(0)
if a.brand == 'inplayv1' and a.jmp_task in ('loadstg','dqstgprd','loadstgprd','posteval'):
    p.error('Gold-only --jmp-task modes require a Gold-only brand')
if a.poll_seconds < 1 or a.timeout_minutes < 1:
    p.error('--poll-seconds and --timeout-minutes must be >= 1')
if a.archive and a.archive_template:
    p.error('Choose only --archive or --archive-template')
if (a.archive or a.archive_template) and a.jmp_task != 'rawdq':
    p.error('--archive options are supported only for --jmp-task rawdq')

def parse_date(s):
    try:
        val = dt.date.fromisoformat(s)
        if val.isoformat() != s:
            raise ValueError()
        return val
    except ValueError:
        p.error('Invalid YYYY-MM-DD date: ' + s)

if a.dates is not None:
    if a.date_to:
        p.error('--to cannot be combined with --dates')
    days = [parse_date(x.strip()) for x in a.dates.split(',')]
    if not days or len(days) != len(set(days)):
        p.error('Date list must be nonempty and have no duplicate business dates')
else:
    if not a.date_to:
        p.error('--from requires --to')
    first, last = parse_date(a.date_from), parse_date(a.date_to)
    if first >= last:
        p.error('--to must be later than --from')
    days = [first + dt.timedelta(days=i) for i in range((last-first).days)]
if len(days) > 366:
    p.error('Maximum 366 windows per controller run')
if a.archive and len(days) != 1:
    p.error('--archive requires one business date; use --archive-template for multiple dates')

stamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
runlog = LOG_DIR / f'run_full_etl_{stamp}_{os.getpid()}.log'
logger = logging.getLogger('prism_full_etl')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
for handler in (logging.FileHandler(runlog), logging.StreamHandler(sys.stdout)):
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# No stale artifacts: only use files created/updated after current window launch.
def fresh_files(folder, glob, t0):
    if not folder.is_dir():
        return []
    return sorted((x for x in folder.glob(glob) if x.is_file() and x.stat().st_mtime >= t0),
                  key=lambda x: x.stat().st_mtime, reverse=True)

def read_tail(path, limit=600000):
    with path.open('rb') as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell()-limit))
        return f.read().decode('utf-8', errors='replace')

def postrelease_ok(day, next_day, t0):
    # Must have fresh, matching strict RELEASED evidence and a fresh JSON verifier PASS.
    relevant = fresh_files(GOLD / 'logs',
                           f'nexus_{"inplay" if a.brand == "inplayv2" else a.brand}_gameTransaction_{day}_{next_day}_*.log', t0)
    released = any('[STRICT RELEASED] entity=gameTransaction ' in read_tail(x) for x in relevant)
    if not released:
        return False, None
    for report in fresh_files(GOLD / 'logs',
             f'post_release_gameTransaction_{day}_{next_day}_*.json', t0):
        try:
            payload = json.loads(report.read_text(encoding='utf-8'))
            if (payload.get('entity') == 'gameTransaction' and
                payload.get('status') == 'PASS_RELEASE_VERIFIED' and
                str(payload.get('window_from','')).startswith(day+' 06:00:00') and
                str(payload.get('window_to','')).startswith(next_day+' 06:00:00') and
                payload.get('checks') and all(payload['checks'].values())):
                return True, report
        except (OSError, ValueError, TypeError):
            pass
    return False, None

def dq_ok(day, next_day, t0):
    dq_reports = D2S / 'work' / 'dq_reports'
    for path in fresh_files(dq_reports, f'mysql_dlake_{day.replace("-", "")}_*_summary.json', t0):
        try:
            obj = json.loads(path.read_text(encoding='utf-8'))
            if (obj.get('status') == 'PASS' and
                obj.get('mysql_dlake_trust_result') == 'PASS_FOR_NEXUS_IMPORT' and
                str(obj.get('window_from','')).startswith(day+' 06:00:00') and
                str(obj.get('window_to','')).startswith(next_day+' 06:00:00') and
                obj.get('audit_status') == 'PASS'):
                return True, path
        except (OSError, ValueError, TypeError):
            pass
    return False, None

def loader_ok(day, t0):
    # Loader logs include source archive and PASS marker; never infer success from file existence.
    for file in fresh_files(LOG_ROOT, 'egms_games_txn_pg_loader_*.log', t0):
        tail = read_tail(file)
        if ('Load completed successfully.' in tail and 'to_final=True' in tail and
                re.search(r'Source file\s*:\s*[^\n]*'+day.replace('-',''), tail)):
            return True, file
    return False, None

def extract_ok(day, t0):
    # The extractor's exact output contract has not been supplied. A new, intact
    # archive is necessary evidence, but not a sufficient completion attestation.
    dump = os.environ.get('DMP_DIR', '')
    if not dump:
        return False, None
    root = Path(dump)
    if not root.is_dir():
        return False, None
    # Search expected YYYYMM subtree and its files; do not scan entire archive history.
    paths = root / day.replace('-', '')[:6]
    candidates = fresh_files(paths, f'*{day.replace("-", "")}*.jsonl.gz', t0)
    if not candidates:
        candidates = fresh_files(root, f'*{day.replace("-", "")}*.jsonl.gz', t0)
    for file in candidates:
        try:
            with gzip.open(file, 'rb') as gz:
                while gz.read(1024 * 1024):
                    pass
            return True, file
        except (OSError, EOFError):
            pass
    return False, None

def downstream_failure(day, next_day, t0, mode):
    if mode in ('full', 'all'):
        for path in fresh_files(GOLD / 'logs', f'nexus_{"inplay" if a.brand == "inplayv2" else a.brand}_gameTransaction_{day}_{next_day}_*.log', t0):
            data = read_tail(path)
            if re.search(r'\| ERROR \| NEXUS .*failed rc=', data):
                return 'Gold Phase failed: ' + str(path)
        for path in fresh_files(GOLD / 'logs', f'post_release_gameTransaction_{day}_{next_day}_*.json', t0):
            try:
                obj = json.loads(path.read_text(encoding='utf-8'))
                if obj.get('status') == 'FAIL_RELEASE_VERIFICATION':
                    return 'Post-release verification failed: ' + str(path)
            except (OSError, ValueError, TypeError):
                pass
    if mode in ('full','all','rawdq','loaddq','extractloaddq'):
        for path in fresh_files(D2S / 'work' / 'dq_reports',
                                f'mysql_dlake_{day.replace("-", "")}_*_summary.json', t0):
            try:
                obj = json.loads(path.read_text(encoding='utf-8'))
                if (str(obj.get('window_from', '')).startswith(day+' 06:00:00') and
                    obj.get('status') == 'FAIL'):
                    return 'Raw DQ failed: ' + str(path)
            except (OSError, ValueError, TypeError):
                pass
    return None

def alert(day, next_day, reason):
    if a.no_email:
        return
    shared = Path(os.environ.get('SHARED_MODS', str(D2S.parent/'common/shared_modules')))
    script = shared / 'etl_alert.py'
    if not script.is_file():
        logger.error('SMTP alert helper missing: %s', script)
        return
    command = [sys.executable, str(script), '--stage', 'PRISM-FULL-ETL-CONTROLLER',
               '--entity', 'gameTransaction', '--window-from', day,
               '--window-to', next_day, '--exit-code', '1', '--error-log', str(runlog)]
    try:
        subprocess.run(command, timeout=35, check=False)
    except Exception as exc:
        logger.error('Failure alert invocation error: %s', exc)

controller_started = time.monotonic()
window_results = []
active_window = None
final_status = 'FAIL'

def fmt_duration(seconds):
    elapsed = int(seconds)
    return f'{elapsed // 3600:02d}:{(elapsed % 3600) // 60:02d}:{elapsed % 60:02d}'

logger.info('START | mode=%s windows=%d timeout=%dm poll=%ds log=%s',
            a.jmp_task, len(days), a.timeout_minutes, a.poll_seconds, runlog)
try:
    for pos, date in enumerate(days, start=1):
        day = date.isoformat()
        active_window = {'date': day, 'started': time.monotonic()}
        next_day = (date + dt.timedelta(days=1)).isoformat()
        name = f'{pos:03d}_{day}'
        childlog = LOG_DIR / f'child_{stamp}_{name}.log'
        started_marker = LOG_DIR / f'raw_dq_started_{stamp}_{name}.txt'
        status_marker = LOG_DIR / f'raw_dq_status_{stamp}_{name}.txt'
        env = os.environ.copy()
        env['PRISM_D2S_BRAND'] = a.brand
        archive = None
        if a.archive or a.archive_template:
            archive = Path(a.archive or a.archive_template.replace('{date}', day.replace('-','')))
            if not archive.is_file() or not archive.name.endswith('.jsonl.gz'):
                raise RuntimeError(f'Archive unavailable or wrong extension: {archive}')
        if a.jmp_task == 'rawdq' and archive:
            wrapper = D2S / 'batch_daily_dqraw.sh'
            if not wrapper.is_file():
                raise RuntimeError(f'Missing raw DQ runner: {wrapper}')
            env['SKIP_PROD_IMPORT'] = 'true'
            command = ['/usr/bin/bash', str(wrapper),
                       day+' 06:00:00', next_day+' 06:00:00', str(archive),
                       str(started_marker), str(status_marker)]
        elif a.brand != 'inplayv1':
            # Gold-only brands start at Gold Phase; do not use V1 MySQL D2S.
            gold_runner = GOLD / 'run_imp_nexusall.sh'
            if not gold_runner.is_file():
                raise RuntimeError(f'Gold runner missing: {gold_runner}')
            command = ['/usr/bin/bash', str(gold_runner), day, next_day,
                       a.entity, '--brands', a.brand]
            if a.jmp_task not in ('full', 'all'):
                if a.jmp_task == 'load':
                    command += ['--jmp-task', 'loadstg']
                elif a.jmp_task in ('loadstg','dqstgprd','loadstgprd','posteval'):
                    command += ['--jmp-task', a.jmp_task]
                else:
                    raise RuntimeError(f'Unsupported Gold-only mode: {a.jmp_task}')
        else:
            command = ['/usr/bin/bash', str(D2S/'run_etl.sh'), day, next_day,
                       '--jmp-task', a.jmp_task, '--brands', a.brand]
        logger.info('WINDOW [%d/%d] %s 06:00 -> %s 06:00 | START mode=%s',
                    pos, len(days), day, next_day, a.jmp_task)
        # Launch timestamp after starting diagnostics, before the subprocess.
        start_epoch = time.time()
        with childlog.open('ab') as output:
            proc = subprocess.Popen(command, env=env, cwd=str(D2S),
                                    stdout=output, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL, start_new_session=True, close_fds=True)
        logger.info('CHILD pid=%s log=%s', proc.pid, childlog)
        deadline = time.monotonic() + 60*a.timeout_minutes
        last_progress = 0
        while True:
            rc = proc.poll()
            if rc is not None and rc != 0:
                raise RuntimeError(f'Launcher/phase failed rc={rc} for {day}; inspect {childlog}: {read_tail(childlog, 2200)}')
            if rc == 0 and a.brand == 'inplayv1' and a.jmp_task in ('full', 'all'):
                child_tail = read_tail(childlog)
                if '[STRICT][ALREADY RELEASED] No further import' in child_tail:
                    raise RuntimeError(
                        f'ALREADY RELEASED: Gold Phase reports {day} to {next_day} already released. '
                        'No new release/post-release markers will be created; refusing to wait or '
                        'claim a new PASS. Verify existing release and run posteval explicitly if required. '
                        f'Log: {childlog}'
                    )
            if a.brand != 'inplayv1':
                done, evidence = (rc == 0), childlog
            elif a.jmp_task in ('full', 'all'):
                done, evidence = postrelease_ok(day, next_day, start_epoch)
            elif a.jmp_task in ('rawdq', 'loaddq', 'extractloaddq'):
                done, evidence = dq_ok(day, next_day, start_epoch)
            elif a.jmp_task == 'load':
                done, evidence = loader_ok(day, start_epoch)
            else:
                # Extract only: conservatively require archive plus parent completion.
                done, evidence = extract_ok(day, start_epoch)
            failure = downstream_failure(day, next_day, start_epoch, a.jmp_task) if a.brand == 'inplayv1' else None
            if failure:
                raise RuntimeError(failure)
            if done and rc == 0:
                window_duration = fmt_duration(time.monotonic() - active_window['started'])
                window_results.append((day, 'PASS_RELEASE_VERIFIED' if a.jmp_task in ('full', 'all') else 'PASS', window_duration))
                logger.info('WINDOW PASS | %s | duration=%s | evidence=%s', day, window_duration, evidence)
                active_window = None
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f'Completion evidence not found for {day} mode={a.jmp_task}; '
                                   f'launcher_rc={rc}; childlog={childlog}. '
                                   'FAIL CLOSED: check downstream status manually before retry.')
            if time.monotonic() - last_progress >= 60:
                logger.info('WAIT | %s | launcher=%s | waiting for %s completion evidence',
                            day, 'running' if rc is None else f'exit={rc}', a.jmp_task)
                last_progress = time.monotonic()
            time.sleep(a.poll_seconds)
    final_status = 'PASS'
    logger.info('FINAL PASS | mode=%s windows=%d | all required completion evidence confirmed', a.jmp_task, len(days))
except Exception as exc:
    logger.error('FINAL FAIL | %s', exc)
    if active_window is not None:
        window_results.append((active_window['date'], 'FAIL',
                               fmt_duration(time.monotonic() - active_window['started'])))
        active_window = None
    # day is populated when processing a particular window.
    alert(locals().get('day','unknown'), locals().get('next_day','unknown'), str(exc))
finally:
    total_duration = fmt_duration(time.monotonic() - controller_started)
    passed = sum(status not in ('FAIL',) for _, status, _ in window_results)
    failed = sum(status == 'FAIL' for _, status, _ in window_results)
    lines = [
        '=' * 50,
        '          NEXUS FULL ETL SUMMARY',
        '=' * 50,
        'Mode                 : ' + a.jmp_task,
        'Business Dates       : ' + str(len(days)),
        '',
    ]
    for business_date, status, duration in window_results:
        lines += [business_date, 'Status               : ' + status,
                  'Duration             : ' + duration, '']
    lines += [
        '-' * 50,
        'Final Status         : ' + final_status,
        'Total Windows        : ' + str(len(days)),
        'Passed               : ' + str(passed),
        'Failed               : ' + str(failed),
        'Not Started          : ' + str(len(days) - passed - failed),
        'Total Duration       : ' + total_duration,
        '=' * 50,
    ]
    summary = '\n'.join(lines)
    print(summary, flush=True)
    for line in lines:
        logger.info('%s', line)
    logger.info('TOTAL PROCESS DURATION | %s (HH:MM:SS)', total_duration)
if final_status != 'PASS':
    sys.exit(1)
PY

