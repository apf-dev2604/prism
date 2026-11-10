#!/usr/bin/env python3
"""PRISM InplayV1 seven-business-day, count-only source/lake/production gap watcher.
Python 3.9+. Transaction tables are read only; only db_admin monitoring table is written.
Run through run_gap_watcher.sh after PRISM load_config.sh has exported configuration.
"""
import argparse
import fcntl
import hashlib
import logging
import os
import smtplib
import sys
import time
from datetime import date, datetime, time as dtime, timedelta
from email.message import EmailMessage
from pathlib import Path
from zoneinfo import ZoneInfo

import pymysql
import psycopg2
from psycopg2 import sql

SHARED = os.environ.get('SHARED_MODS', '')
if not SHARED or not Path(SHARED, 'etl_report.py').is_file():
    raise RuntimeError('SHARED_MODS/etl_report.py missing: run via run_gap_watcher.sh')
sys.path.insert(0, SHARED)
from etl_report import configure_logging, print_summary

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / 'logs'
LOCK_PATH = ROOT / 'gap_watcher.lock'
BRAND = 'inplayv1'
ENTITY = 'gameTransaction'
OBS = 'etl_gap_watch_observations'
PHT = ZoneInfo('Asia/Manila')


def env_required(*names):
    for key in names:
        if os.environ.get(key):
            return os.environ[key]
    raise RuntimeError('Missing required exported configuration: ' + ' or '.join(names))


def settings():
    return {
        'mysql_host': env_required('DB_SETTINGS_INPLAY1_DB_HOST'),
        'mysql_port': int(env_required('DB_SETTINGS_INPLAY1_DB_PORT')),
        'mysql_user': env_required('DB_SETTINGS_INPLAY1_DB_USER'),
        'mysql_password': env_required('GAP_MYSQL_PASSWORD', 'INPLAYV1_SRC_PWD', 'INPLAY1_DB_USER_PWD', 'SRC_PWD'),
        'mysql_db': env_required('DB_SETTINGS_INPLAY1_DB_NAME'),
        'mysql_table': env_required('DB_SETTINGS_INPLAY1_GAMETX_TABLENAME'),
        'pg_host': env_required('DB_SETTINGS_PIPELINE_DB_HOST'),
        'pg_port': int(env_required('DB_SETTINGS_PIPELINE_DB_PORT')),
        'pg_user': env_required('DB_SETTINGS_PIPELINE_PIPELINE_USER'),
        'pg_password': env_required('ETL_DB_USER_PWD'),
        'lake_db': env_required('DB_SETTINGS_DW_RAW_DB_NAME'),
        'prd_db': env_required('DB_SETTINGS_PRD_RAW_DB_NAME'),
        'lake_schema': env_required('DB_SETTINGS_DW_RAW_SCHEMA_NAME'),
        'lake_table': env_required('DB_SETTINGS_DW_RAW_DB_TABLENAME'),
    }


def args_parse():
    p = argparse.ArgumentParser(description='Count-only 3-system game transaction gap watcher')
    p.add_argument('--lookback-days', type=int, default=7)
    p.add_argument('--as-of-date', help='PHT YYYY-MM-DD; check preceding completed windows')
    p.add_argument('--check-only', action='store_true', help='Connectivity/schema validation, no observations')
    p.add_argument('--dry-run', action='store_true', help='Run all count comparisons, but do not write monitoring data or send email')
    p.add_argument('--no-email', action='store_true')
    a = p.parse_args()
    if a.check_only and a.dry_run:
        p.error('--check-only and --dry-run are mutually exclusive')
    if not 1 <= a.lookback_days <= 60:
        p.error('--lookback-days must be between 1 and 60')
    if a.as_of_date:
        date.fromisoformat(a.as_of_date)
    return a


def windows(a):
    now = datetime.now(PHT)
    boundary = date.fromisoformat(a.as_of_date) if a.as_of_date else (
        now.date() if now.time() >= dtime(6) else now.date() - timedelta(days=1))
    for i in range(a.lookback_days, 0, -1):
        day = boundary - timedelta(days=i)
        yield day, datetime.combine(day, dtime(6)), datetime.combine(day + timedelta(days=1), dtime(6))


def mysql_connect(s):
    return pymysql.connect(host=s['mysql_host'], port=s['mysql_port'], user=s['mysql_user'],
                           password=s['mysql_password'], database=s['mysql_db'], charset='utf8mb4',
                           autocommit=True, connect_timeout=12, read_timeout=300,
                           cursorclass=pymysql.cursors.DictCursor)


def pg_connect(s, database, readonly=False):
    c = psycopg2.connect(host=s['pg_host'], port=s['pg_port'], dbname=database,
                         user=s['pg_user'], password=s['pg_password'], sslmode='require',
                         connect_timeout=12, options='-c statement_timeout=300000 -c lock_timeout=5000')
    if readonly:
        c.set_session(readonly=True, autocommit=True)
    return c


def mysql_count(c, s, start, stop):
    table = '`%s`.`%s`' % (s['mysql_db'].replace('`','``'), s['mysql_table'].replace('`','``'))
    with c.cursor() as cur:
        cur.execute('SELECT COUNT(*) AS cnt FROM ' + table +
                    ' WHERE `GameDate` >= %s AND `GameDate` < %s', (start, stop))
        return int(cur.fetchone()['cnt'])


def pg_count(c, table_schema, table_name, column_name, start, stop, timestamp_type='timestamp with time zone'):
    # Match the physical column type. Do not shift PRISM's UTC-assumed clock values.
    bound = "(%s::timestamp AT TIME ZONE 'UTC')" if timestamp_type == 'timestamp with time zone' else '%s::timestamp'
    query = sql.SQL('SELECT COUNT(*) FROM {}.{} WHERE {} >= ' + bound + ' AND {} < ' + bound).format(
        sql.Identifier(table_schema), sql.Identifier(table_name), sql.Identifier(column_name),
        sql.Identifier(column_name))
    with c.cursor() as cur:
        cur.execute(query, (start, stop))
        return int(cur.fetchone()[0])


def check_schema(lake, prd, s):
    types = {}
    for connection, schema, name, column in (
        (lake, s['lake_schema'], s['lake_table'], 'game_dt'),
        (prd, 'public', 'gametransaction_prod', 'enddatetime')):
        with connection.cursor() as cur:
            cur.execute('SELECT data_type FROM information_schema.columns '
                        'WHERE table_schema=%s AND table_name=%s AND column_name=%s',
                        (schema, name, column))
            row = cur.fetchone()
            if row is None or row[0] not in ('timestamp with time zone', 'timestamp without time zone'):
                raise RuntimeError('%s.%s.%s must be timestamp/timestamptz; found %r' %
                                   (schema, name, column, row))
            types[column] = row[0]
    with prd.cursor() as cur:
        cur.execute('SELECT to_regclass(%s)', ('db_admin.' + OBS,))
        if cur.fetchone()[0] is None:
            raise RuntimeError('Install db_admin.%s using 01_gap_observations.sql' % OBS)
    return types


def previous(prd, start, stop):
    with prd.cursor() as cur:
        cur.execute('SELECT check_id, mysql_rows, datalake_rows, production_rows, status '
                    'FROM db_admin.etl_gap_watch_observations '
                    'WHERE brand=%s AND entity=%s AND window_from=%s AND window_to=%s '
                    'ORDER BY check_id DESC LIMIT 1', (BRAND, ENTITY, start, stop))
        return cur.fetchone()


def last_sent(prd, start, stop):
    with prd.cursor() as cur:
        cur.execute('SELECT alert_fingerprint FROM db_admin.etl_gap_watch_observations '
                    'WHERE brand=%s AND entity=%s AND window_from=%s AND window_to=%s '
                    "AND notification_status='SENT' ORDER BY check_id DESC LIMIT 1",
                    (BRAND, ENTITY, start, stop))
        r = cur.fetchone()
        return r[0] if r else None


def insert(prd, day, start, stop, previous_record, counts, deltas, status, fingerprint, note):
    old = previous_record[1:4] if previous_record else (None, None, None)
    with prd.cursor() as cur:
        cur.execute('INSERT INTO db_admin.etl_gap_watch_observations '
                    '(brand,entity,business_date,window_from,window_to,'
                    'previous_mysql_rows,previous_datalake_rows,previous_production_rows,'
                    'mysql_rows,datalake_rows,production_rows,'
                    'new_mysql_rows,new_datalake_rows,new_production_rows,'
                    'status,alert_fingerprint,notes) '
                    'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING check_id',
                    (BRAND, ENTITY, day, start, stop, *old, *counts, *deltas, status, fingerprint, note))
        result = cur.fetchone()[0]
    prd.commit()
    return result


def mark_email(prd, ids, state):
    if not ids:
        return
    with prd.cursor() as cur:
        cur.execute('UPDATE db_admin.etl_gap_watch_observations '
                    "SET notification_status=%s, notified_at=CASE WHEN %s='SENT' THEN now() ELSE NULL END "
                    'WHERE check_id=ANY(%s)', (state, state, ids))
    prd.commit()


def smtp_send(alerts, no_email=False):
    if no_email:
        return 'DISABLED'
    # Values are supplied by PRISM shared load_config.sh; never print credentials.
    host = os.getenv('SMTP_HOST') or os.getenv('SMTP_SETTINGS_HOST') or os.getenv('COMMON_SMTP_HOST')
    port = int(os.getenv('SMTP_PORT') or os.getenv('SMTP_SETTINGS_PORT') or '587')
    username = os.getenv('SMTP_USER') or os.getenv('SMTP_SETTINGS_USER') or ''
    password = os.getenv('SMTP_PASSWORD') or os.getenv('SMTP_SETTINGS_PASSWORD') or ''
    sender = os.getenv('SMTP_FROM') or os.getenv('SMTP_SETTINGS_FROM') or ''
    recipient = os.getenv('SMTP_TO') or os.getenv('SMTP_SETTINGS_TO') or ''
    if not (host and sender and recipient):
        logging.error('SMTP configuration unavailable; no mail sent. Align exported keys with shared_modules/etl_alert.py')
        return 'NOT_CONFIGURED'
    window_from = min(x['start'] for x in alerts)
    window_to = max(x['stop'] for x in alerts)
    msg = EmailMessage()
    msg['Subject'] = ('[Nexus Gap Watcher] : Game Transaction [InplayV1] Variance : '
                      '%s to %s' % (window_from.strftime('%Y-%m-%d 06:00'), window_to.strftime('%Y-%m-%d 06:00')))
    msg['From'] = sender
    msg['To'] = recipient
    lines = ['PRISM / NEXUS count-only variance report',
             'Brand: InplayV1 | Entity: gameTransaction',
             'Timezone convention: MySQL PHT clock values are UTC-assumed in PostgreSQL; NO 8-hour shift.',
             'No transaction data is modified or backfilled by this watcher.', '']
    for x in alerts:
        lines += [
            'Business date: %s | Window: %s -> %s (exclusive)' % (x['day'],x['start'],x['stop']),
            'Status: %s | Observation: %s' % (x['status'], x['id']),
            '%-18s %15s %15s %15s' % ('System','Previous','Current','New records'),
            '%-18s %15s %15s %15s' % ('MySQL',str(x['old'][0]),str(x['counts'][0]),str(x['deltas'][0])),
            '%-18s %15s %15s %15s' % ('Data Lake',str(x['old'][1]),str(x['counts'][1]),str(x['deltas'][1])),
            '%-18s %15s %15s %15s' % ('NEXUS Production',str(x['old'][2]),str(x['counts'][2]),str(x['deltas'][2])),
            'Total current rows across three systems (not unique transactions): %s' % (sum(c for c in x['counts'] if c is not None) if all(c is not None for
 c in x['counts']) else 'UNAVAILABLE'),
            'Count differences (MySQL-Lake / MySQL-Prod / Lake-Prod): %s' %
            (('%+d / %+d / %+d' % (x['counts'][0]-x['counts'][1], x['counts'][0]-x['counts'][2], x['counts'][1]-x['counts'][2])) if all(c is not None for c
in x['counts']) else 'UNAVAILABLE'),
            'New MySQL rows to investigate (positive delta): %s' % max(x['deltas'][0] or 0,0),
            'Action: investigate the business-day window; use approved PRISM ETL/DQ for recovery.', '']
    msg.set_content('\n'.join(lines))
    try:
        with smtplib.SMTP(host, port, timeout=20) as server:
            server.ehlo()
            if os.getenv('SMTP_STARTTLS', 'true').lower() == 'true':
                server.starttls()
                server.ehlo()
            if username:
                server.login(username, password)
            server.send_message(msg)
        return 'SENT'
    except Exception:
        logging.exception('Watcher SMTP delivery failure')
        return 'FAILED'


def main():
    args = args_parse()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / ('gap_watcher_%s_%s.log' % (datetime.now(PHT).strftime('%Y%m%d_%H%M%S'),os.getpid()))
    configure_logging(log)
    started = time.monotonic()
    s = settings()
    with LOCK_PATH.open('a+') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logging.info('SKIPPED: another gap watcher instance is running')
            return 0
        mysql = lake = prd = None
        alerts = []
        alert_ids = []
        findings = 0
        checked = 0
        window_statuses = []
        try:
            mysql = mysql_connect(s)
            lake = pg_connect(s, s['lake_db'], readonly=True)
            prd = pg_connect(s, s['prd_db'], readonly=args.dry_run)
            timestamp_types = check_schema(lake, prd, s)
            if args.check_only:
                with mysql.cursor() as cur:
                    cur.execute('SELECT 1')
                logging.info('[GAP CHECK ONLY] PASS connectivity and schema')
                return 0
            for day, start, stop in windows(args):
                prev = previous(prd, start, stop)
                old = prev[1:4] if prev else (None, None, None)
                try:
                    counts = (mysql_count(mysql,s,start,stop),
                              pg_count(lake,s['lake_schema'],s['lake_table'],'game_dt',start,stop,timestamp_types['game_dt']),
                              pg_count(prd,'public','gametransaction_prod','enddatetime',start,stop,timestamp_types['enddatetime']))
                    deltas = tuple(c - p if p is not None else None for c,p in zip(counts,old))
                    gap = len(set(counts)) != 1
                    source_changed = deltas[0] is not None and deltas[0] != 0
                    other_changed = any(d is not None and d != 0 for d in deltas[1:])
                    status = ('COUNT_GAP' if gap else
                              'SOURCE_COUNT_CHANGED' if source_changed else
                              'DOWNSTREAM_COUNT_CHANGED' if other_changed else 'PASS')
                    note = 'Count-only; matching counts do not guarantee matching transaction IDs or financial amounts.'
                except Exception as exc:
                    if not args.dry_run:
                        prd.rollback()
                    logging.exception('WINDOW CHECK ERROR %s', day)
                    counts = (None, None, None)
                    deltas = (None, None, None)
                    status, note = 'CHECK_ERROR', str(exc)[:1200]
                fp_data = repr((status,counts))
                fingerprint = hashlib.sha256(fp_data.encode()).hexdigest() if status != 'PASS' else None
                check_id = None if args.dry_run else insert(prd,day,start,stop,prev,counts,deltas,status,fingerprint,note)
                checked += 1
                window_statuses.append(status)
                logging.info('[WINDOW SUMMARY]%s date=%s previous=%s current=%s new=%s status=%s check_id=%s',
                             ' [DRY RUN]' if args.dry_run else '',day,old,counts,deltas,status,check_id)
                if status != 'PASS':
                    findings += 1
                    if not args.dry_run and ((prev is not None and prev[4] == 'PASS') or fingerprint != last_sent(prd,start,stop)):
                        alerts.append(dict(id=check_id,day=day,start=start,stop=stop,status=status,
                                           old=old,counts=counts,deltas=deltas))
                        alert_ids.append(check_id)
                time.sleep(.15)
            state = 'DRY_RUN_NO_EMAIL' if args.dry_run else (smtp_send(alerts,args.no_email) if alerts else 'NONE_NEW')
            if not args.dry_run:
                mark_email(prd,alert_ids,state)
            print_summary('[RUN SUMMARY][Nexus Gap Watcher]', [
                ('mode', 'DRY RUN (NO DB WRITES / NO EMAIL)' if args.dry_run else 'NORMAL'),
                ('brand', BRAND), ('entity', ENTITY), ('windowsChecked', checked),
                ('windowsWithFindings', findings), ('newAlertWindows', len(alerts)),
                ('emailStatus', state), ('durationSeconds', round(time.monotonic()-started,2)),
                ('log', str(log))])
            return 2 if any(x == 'CHECK_ERROR' for x in window_statuses) else 0
        except Exception:
            logging.exception('FATAL: gap watcher failed')
            return 2
        finally:
            for c in (mysql,lake,prd):
                if c:
                    c.close()


if __name__ == '__main__':
    sys.exit(main())
