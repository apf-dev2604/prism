
#!/usr/bin/env python3
"""PRISM shared terminal and log formatting. No ETL decisions or side effects."""
import logging
import sys


def configure_logging(log_path, level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path)],
    )


def print_summary(title, fields):
    print('\n' + title)
    print('=' * len(title))
    for name, value in fields:
        print('%-26s: %s' % (name, value))


def exception_summary(exc):
    """Short error suitable for terminal, operational log, and notifications."""
    import traceback
    frames = traceback.extract_tb(exc.__traceback__)
    frame = frames[-1] if frames else None
    location = ("%s:%s" % (frame.filename, frame.lineno)) if frame else "unknown"
    return "%s | %s: %s" % (location, type(exc).__name__, exc)


def log_exception(message, exc, detail_dir):
    """One-line operational error; full traceback in separate diagnostics."""
    import traceback
    from pathlib import Path
    from datetime import datetime, timezone
    directory = Path(detail_dir)
    directory.mkdir(parents=True, exist_ok=True)
    detail_file = directory / ("dq_traceback_%s_%s.log" % (
        datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f"), __import__('os').getpid()))
    detail_file.write_text(''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                           encoding='utf-8')
    logging.error('%s | %s | diagnostic=%s', message, exception_summary(exc), detail_file)
    return detail_file


def summarize_traceback_file(path):
    """Extract last Python exception and its file:line from unhandled stderr."""
    import re
    from pathlib import Path
    content = Path(path).read_text(errors='replace').splitlines()
    locations = re.findall(r'^\s*File "([^"]+)", line (\d+)', '\n'.join(content), re.MULTILINE)
    last = next((line.strip() for line in reversed(content) if
                 re.match(r'^[\w.]+(?:Error|Exception|Exit):', line.strip())), None)
    if last is None:
        last = next((line.strip() for line in reversed(content) if line.strip()), 'Unknown process error')
    where = ('%s:%s' % locations[-1]) if locations else 'unknown'
    return '%s | %s' % (where, last)


def setup_named_logger(path, name="migration_engine", level=logging.INFO):
    """Existing NEXUS migration_engine logger, implemented in shared reporting."""
    from pathlib import Path
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()
    handler = logging.FileHandler(str(output), mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def emit_event(message, *, level=logging.INFO, **fields):
    """Preserve NEXUS event prints and structured JSON audit fields."""
    import json
    from datetime import datetime, timezone
    payload = None
    if fields:
        values = {"timestamp": datetime.now(timezone.utc).isoformat(), **fields}
        try:
            payload = json.dumps(values, ensure_ascii=False, default=str)
        except Exception:
            payload = str(values)
    line = message if payload is None else f"{message} | {payload}"
    print(line, flush=True)
    logging.getLogger("migration_engine").log(level, line)


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--summarize-error', required=True)
    arguments = ap.parse_args()
    print(summarize_traceback_file(arguments.summarize_error))

