
#!/usr/bin/env python3
"""PRISM read-only release verification for the existing gameTransaction strict publish.

Run from GOLD_PHASE as:
python -m helpers.post_release_verify --date-from YYYY-MM-DD --date-to YYYY-MM-DD --entity gameTransaction

Uses environment variables previously exported by the existing load_config.sh.

This is a release-integrity check, not a new full source-to-production DQ.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from psycopg2 import sql

from .db import connect

if not os.environ.get('SHARED_MODS'):
    raise RuntimeError('SHARED_MODS must be set by the existing PRISM configuration loader')

sys.path.insert(0, os.environ['SHARED_MODS'])

from etl_report import configure_logging, print_summary


TABLES = {
    'gameTransaction': (
        'gametransaction_prod',
        'enddatetime',
        'externalid',
        'gameTransaction'
    )
}


def main():
    p = argparse.ArgumentParser(description='PRISM read-only post-release verification')
    p.add_argument('--date-from', required=True)
    p.add_argument('--date-to', required=True)
    p.add_argument('--brand', required=True)
    p.add_argument('--entity', choices=tuple(TABLES), required=True)
    args = p.parse_args()

    d0 = datetime.strptime(args.date_from, '%Y-%m-%d')
    d1 = datetime.strptime(args.date_to, '%Y-%m-%d')

    if d1 <= d0:
        p.error('--date-to must be later than --date-from')

    lo, hi = d0 + timedelta(hours=6), d1 + timedelta(hours=6)
    table, date_col, key_col, audit_entity = TABLES[args.entity]
    admin_schema='db_admin'
    prod_schema='public'
    stage_schema='etl_stage'
    stage_table=table

    base = Path(os.environ['GOLD_PHASE'])
    log_dir = base / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    name = 'post_release_%s_%s_%s_%s' % (
        args.entity, args.date_from, args.date_to, stamp
    )
    verification_run_id = name
    log_path = log_dir / (name + '.log')
    json_path = log_dir / (name + '.json')

    configure_logging(log_path)

    zone = ZoneInfo(os.environ['COMMON_ETL_SETTINGS_PHT_TIMEZONE'])
    started = datetime.now(zone)
    begun = time.monotonic()

    checks = {}
    metrics = {}
    conn = None
    error = None

    try:
        conn = connect('iestdbrds', env_prefix='TARGET')

        # No data, audit, or checkpoint records can be changed by this script.
        conn.set_session(readonly=True)

        with conn.cursor() as cur:
            cur.execute("SET LOCAL TIME ZONE 'UTC'")

            cur.execute(f'''SELECT state, dq_run_id, notes
                           FROM {admin_schema}.etl_strict_delivery_control
                           WHERE brand=%s AND entity=%s
                             AND window_date_from=%s
                             AND window_date_to=%s''',
                        ('inplay' if args.brand in ('inplayv1', 'inplayv2') else args.brand,args.entity, d0.date(), d1.date())) 

            rows = cur.fetchall()

            checks['exact_delivery_control'] = len(rows) == 1
            control = rows[0] if len(rows) == 1 else None

            checks['delivery_released'] = bool(
                control and control[0] == 'RELEASED' and control[1]
            )

            run_id = control[1] if control else None

            metrics['dq_run_id'] = run_id
            metrics['delivery_state'] = control[0] if control else None

            dq = None

            if run_id:
                cur.execute(f'''SELECT status, trust_result,
                                      source_rows, target_rows,
                                      missing_in_target, hash_mismatch_rows
                               FROM {admin_schema}.etl_dq_run_summary
                               WHERE run_id=%s
                                 AND brand=%s
                                 AND entity=%s
                               ORDER BY created_at DESC LIMIT 2''',
                            (run_id, 'inplay', audit_entity))

                audit_rows = cur.fetchall()

                checks['exact_approved_dq'] = len(audit_rows) == 1
                dq = audit_rows[0] if len(audit_rows) == 1 else None

            else:
                checks['exact_approved_dq'] = False

            checks['approved_dq_pass'] = bool(
                dq
                and dq[0] == 'PASS'
                and dq[1] == 'PASS_PROD_RELEASED'
                and dq[2] == dq[3]
                and dq[2] > 0
                and not dq[4]
                and not dq[5]
            )

            expected = dq[3] if dq else None

            metrics['approved_rows'] = expected
            metrics['approved_source_rows'] = dq[2] if dq else None

            filt = sql.SQL('{} >= %s AND {} < %s').format(
                sql.Identifier(date_col),
                sql.Identifier(date_col)
            )

            filt = sql.SQL('(') + filt + sql.SQL(') AND brand=%s AND fulletl=%s')
            table_brand = 'inplay'
            full_etl = args.brand == 'inplayv1'

            statement = sql.SQL('''SELECT count(*),
                                         count(DISTINCT {}),
                                         count(*) FILTER (WHERE {} IS NULL)
                                  FROM {} WHERE {}''').format(
                sql.Identifier(key_col),
                sql.Identifier(key_col),
                sql.Identifier(prod_schema, table),
                filt
            )

            cur.execute(statement, (lo, hi, table_brand, full_etl))

            prod_count, distinct_keys, null_keys = cur.fetchone()

            metrics['production_window_rows'] = prod_count
            metrics['production_distinct_keys'] = distinct_keys
            metrics['production_null_keys'] = null_keys

            checks['production_count_matches_approval'] = bool(
                expected is not None and prod_count == expected
            )

            checks['production_keys_unique_and_nonnull'] = (
                prod_count == distinct_keys and null_keys == 0
            )

            cur.execute(
                sql.SQL('SELECT count(*) FROM {} WHERE {}').format(
                    sql.Identifier(stage_schema, stage_table),
                    filt
                ),
                (lo, hi, table_brand, full_etl)
            )

            stage_count = cur.fetchone()[0]

            metrics['remaining_staging_rows'] = stage_count

            checks['staging_window_cleared'] = stage_count == 0

        conn.rollback()  # End read-only transaction.

    except Exception as exc:
        error = '%s: %s' % (type(exc).__name__, exc)
        logging.exception('[POST RELEASE] Verification failed')

        if conn and not conn.closed:
            conn.rollback()

    finally:
        if conn and not conn.closed:
            conn.close()

    status = (
        'PASS_RELEASE_VERIFIED'
        if not error and checks and all(checks.values())
        else 'FAIL_RELEASE_VERIFICATION'
    )

    fields = [
        ('entity', args.entity),
        ('windowFrom', lo.strftime('%Y-%m-%d %H:%M:%S')),
        ('windowTo', hi.strftime('%Y-%m-%d %H:%M:%S')),
        ('approvedRows', metrics.get('approved_rows')),
        ('productionRows', metrics.get('production_window_rows')),
        ('productionDistinctKeys', metrics.get('production_distinct_keys')),
        ('productionNullKeys', metrics.get('production_null_keys')),
        ('remainingStagingRows', metrics.get('remaining_staging_rows')),
    ]

    fields.extend(
        (key, 'PASS' if result else 'FAIL')
        for key, result in checks.items()
    )

    fields.append(('status', status))

    print_summary(
        f'PHASE SUMMARY][{args.brand} post release verification',
        fields
    )

    completed = datetime.now(zone)
    duration = round(time.monotonic() - begun, 2)

    report = {
        'verification_run_id': verification_run_id,
        'stage': 'POST_RELEASE_VERIFICATION',
        'brand': args.brand,
        'entity': args.entity,
        'window_from': lo.isoformat(sep=' '),
        'window_to': hi.isoformat(sep=' '),
        'status': status,
        'checks': checks,
        'metrics': metrics,
        'error': error,
        'started_at_pht': started.isoformat(),
        'completed_at_pht': completed.isoformat(),
        'duration_seconds': duration,
        'log_path': str(log_path),
        'scope': (
            'Release control, pre-publish DQ approval, production '
            'window count/keys, staging cleanup; not full '
            'source-to-production value comparison'
        ),
    }

    json_path.write_text(
        json.dumps(report, indent=2, default=str),
        encoding='utf-8'
    )

    # ============================================================
    # Persist Post-Release Verification into the brand-resolved admin schema.
    # Existing verification checks and JSON output remain intact.
    # ============================================================
    
    # Persist the completed Posteval result.
    # Keep the existing read-only verification transaction unchanged.
    audit_conn = None

    try:
        audit_conn = connect('iestdbrds', env_prefix='TARGET')

        with audit_conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {admin_schema}.etl_post_release_verification (
                    verification_run_id,
                    dq_run_id,
                    brand,
                    entity,
                    window_from,
                    window_to,
                    verification_status,
                    approved_rows,
                    production_rows,
                    production_distinct_keys,
                    production_null_keys,
                    remaining_staging_rows,
                    checks,
                    verified_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s::jsonb, %s
                )
            """, (
                verification_run_id,
                metrics.get('dq_run_id'),
                report['brand'],
                args.entity,
                lo,
                hi,
                status,
                metrics.get('approved_rows'),
                metrics.get('production_window_rows'),
                metrics.get('production_distinct_keys'),
                metrics.get('production_null_keys'),
                metrics.get('remaining_staging_rows'),
                json.dumps(checks),
                completed
            ))

        audit_conn.commit()

        logging.info(
            '[POST RELEASE AUDIT] persisted verification_run_id=%s '
            'status=%s dq_run_id=%s',
            verification_run_id,
            status,
            metrics.get('dq_run_id')
        )

    except Exception:
        if audit_conn and not audit_conn.closed:
            audit_conn.rollback()

        logging.exception(
            '[POST RELEASE AUDIT] Database persistence failed; '
            'JSON report retained'
        )

    finally:
        if audit_conn and not audit_conn.closed:
            audit_conn.close()

   
    # END NEW POST-RELEASE AUDIT PERSISTENCE
    # ============================================================

    logging.info(
        '[RUN SUMMARY][inplayv1 post release] '
        'status=%s durationSeconds=%.2f report=%s',
        status,
        duration,
        json_path
    )

    if error:
        logging.error('Verification error: %s', error)

    return 0 if status == 'PASS_RELEASE_VERIFIED' else 1


if __name__ == '__main__':
    sys.exit(main())

