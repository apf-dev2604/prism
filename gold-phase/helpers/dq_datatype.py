"""Read-only strict InplayV1 game JSONB to PostgreSQL datatype compatibility gate.
Python 3.9; uses existing helpers.db connection settings. No DDL and no DML.
"""
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from .db import connect

DEC18 = {'betamount','validbet','payoutamount',*(f'pc{i}' for i in range(1,6)),*(f'jw{i}' for i in range(1,6))}
DEC20 = {'computed_jackpot_contribution','computed_jackpot_payout'}
TEXT = {'externalid','providername','gamename','playerusername','roundid','tableroomid'}
DATES = {'startdatetime','enddatetime'}
# Source keys absent by design are represented as mappings with a fallback or optional=False.
POLICY = [
    ('TransactionID', ('externalid',), 'text', True, None),
    ('GameProvider', ('providername',), 'text', True, None),
    ('GameName', ('gamename',), 'text', True, None),
    ('PlayerAccount', ('playerusername',), 'text', True, None),
    ('SessionID', ('roundid',), 'text', False, None),
    ('Outlet', ('tableroomid',), 'text', False, None),
    ('GameDate', ('startdatetime','enddatetime'), 'utc_datetime', True, None),
    ('TotalStakes', ('betamount',), 'decimal18', True, None),
    ('ValidBet', ('validbet',), 'decimal18', False, 'TotalStakes'),
    ('TotalWins', ('payoutamount',), 'decimal18', True, None),
    ('JACKPOT_CONTRIBUTION', ('computed_jackpot_contribution',), 'decimal20', True, None),
    ('JACKPOT_PAYOUT', ('computed_jackpot_payout',), 'decimal20', True, None),
    *[(f'PC{i}', (f'pc{i}',), 'decimal18', True, None) for i in range(1,6)],
    *[(f'JW{i}', (f'jw{i}',), 'decimal18', True, None) for i in range(1,6)],
    # Source-specific optional progressive value can be absent; if present, validate.
    ('PROGRESSIVE_CONTRIBUTION_PAID', ('progressioncontributionpaid',), 'decimal18', False, None),
]
NUMERIC_PATTERN = re.compile(r'^[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)$')


def validate_value(data, field, kind, required, fallback):
    raw = data.get(field)
    if raw is None and fallback:
        raw = data.get(fallback)
    if raw is None:
        return None if not required else ('MISSING_REQUIRED', 'Missing/null required field')
    if not isinstance(raw, str):
        # JSON numeric would lose guaranteed exact decimal fidelity through float handling.
        # Only source JSON string is approved by this deployed InplayV1 policy.
        return ('JSON_TYPE', 'Expected JSON string; got %s' % type(raw).__name__)
    if kind == 'text':
        return None if raw != '' or not required else ('EMPTY_REQUIRED', 'Empty required text')
    if kind.startswith('decimal'):
        if not NUMERIC_PATTERN.fullmatch(raw):
            return ('DECIMAL_FORMAT', 'Not a plain decimal string')
        try:
            d = Decimal(raw)
        except InvalidOperation:
            return ('DECIMAL_FORMAT', 'Invalid decimal')
        if not d.is_finite():
            return ('DECIMAL_FORMAT', 'Non-finite decimal')
        precision = 20 if kind == 'decimal20' else 18
        if d != d.quantize(Decimal('0.000001')):
            return ('DECIMAL_SCALE', 'More than 6 significant fractional decimal places')
        # Numeric(precision,6) bound; numeric strings like 0.0000000 are exactly representable.
        if abs(d) >= Decimal(10) ** (precision-6):
            return ('DECIMAL_PRECISION', 'Numeric(%s,6) overflow' % precision)
        return None
    if kind == 'utc_datetime':
        try:
            dt = datetime.fromisoformat(raw.replace('Z','+00:00'))
            if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
                return ('TIMEZONE', 'Explicit UTC offset required')
            if dt.year < 1 or dt.year > 9999:
                return ('DATETIME_RANGE', 'Timestamp outside supported range')
            return None
        except (ValueError, OverflowError):
            return ('DATETIME_FORMAT', 'Invalid ISO 8601 timestamp')
    return ('UNKNOWN_POLICY', 'Unknown policy rule')


def verify_schema(conn):
    with conn.cursor() as cur:
        cur.execute('''SELECT table_schema,column_name,data_type,character_maximum_length,
                             numeric_precision,numeric_scale,is_nullable
                      FROM information_schema.columns
                      WHERE table_schema IN ('etl_stage','public')
                        AND table_name='gametransaction_prod' ''')
        rows=cur.fetchall()
    conn.rollback()
    cols={(sch, name):(typ, length, prec, scale, nullable) for sch,name,typ,length,prec,scale,nullable in rows}
    issues=[]
    for field, targets, kind, required, fallback in POLICY:
        for col in targets:
            s=cols.get(('etl_stage',col)); p=cols.get(('public',col))
            if s is None or p is None:
                issues.append((field,col,'COLUMN_MISSING',repr((s,p))))
                continue
            if s != p:
                issues.append((field,col,'STAGING_PRODUCTION_SCHEMA_MISMATCH',repr((s,p))))
            typ,length,prec,scale,nullable=s
            good=(typ in ('text','character varying') if kind=='text'
                  else typ=='timestamp with time zone' if kind=='utc_datetime'
                  else typ=='numeric' and prec==(20 if kind=='decimal20' else 18) and scale==6)
            if not good:
                issues.append((field,col,'INCOMPATIBLE_TARGET_TYPE',repr(s)))
    return issues


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--brand', required=True)
    ap.add_argument('--entity', choices=['gameTransaction'], default='gameTransaction')
    ap.add_argument('--date-from', required=True); ap.add_argument('--date-to', required=True)
    ap.add_argument('--chunk-size',type=int,default=2000)
    ap.add_argument('--report',default='')
    ap.add_argument('--max-issues',type=int,default=20000)
    args=ap.parse_args()
    if args.brand != 'inplayv1':
        raise SystemExit('BLOCK: unverified datatype policy for '+args.brand)
    if args.chunk_size < 1 or args.max_issues < 1: ap.error('chunk-size and max-issues must be positive')
    d0=datetime.strptime(args.date_from,'%Y-%m-%d')+timedelta(hours=6)
    d1=datetime.strptime(args.date_to,'%Y-%m-%d')+timedelta(hours=6)
    if d1<=d0: ap.error('date-to must be after date-from')
    report=Path(args.report or ('reports/dq_datatype_inplayv1_%s_%s.csv' % (args.date_from.replace('-',''),args.date_to.replace('-',''))))
    report.parent.mkdir(parents=True, exist_ok=True)
    src=connect('iestdl',env_prefix='SOURCE'); tgt=connect('iestdbrds',env_prefix='TARGET')
    bad=0; examined=0; emitted=0
    try:
        issues=verify_schema(tgt)
        if issues:
            for issue in issues: print('[DATATYPE SCHEMA FAIL]',*issue,flush=True)
            return 2
        print('[DATATYPE SCHEMA PASS] staging and production column types compatible',flush=True)
        with report.open('w',newline='',encoding='utf-8') as f:
            writer=csv.writer(f); writer.writerow(['source_id','source_field','target_columns','failure_code','reason'])
            # Named cursor keeps source read bounded; transaction stays open until scan completes.
            with src.cursor(name='nexus_datatype_scan') as cur:
                cur.itersize=args.chunk_size
                cur.execute('''SELECT id::text, data FROM kemet."GameTransactionInplayV1_dmp"
                               WHERE game_dt >= %s AND game_dt < %s AND data IS NOT NULL
                               ORDER BY game_dt,id''',(d0,d1))
                for sid,data in cur:
                    examined+=1
                    if not isinstance(data,dict):
                        data=json.loads(data)
                    for field,targets,kind,required,fallback in POLICY:
                        issue=validate_value(data,field,kind,required,fallback)
                        if issue:
                            bad+=1
                            if emitted<args.max_issues:
                                writer.writerow([sid,field,','.join(targets),*issue]); emitted+=1
                    if examined % 100000 == 0:
                        print('[DATATYPE PROGRESS] rows=%s violations=%s' % (examined,bad),flush=True)
        print('[DATATYPE RESULT] status=%s checked_rows=%d violations=%d recorded=%d report=%s' %
              ('PASS' if not bad else 'FAIL',examined,bad,emitted,report),flush=True)
        return 0 if not bad and examined else 2
    finally:
        src.close();tgt.close()

if __name__=='__main__': sys.exit(main())

