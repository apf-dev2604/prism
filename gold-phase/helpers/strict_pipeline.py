# Controlled strict pipeline; never legacy direct INSERT.
import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from .db import connect

TABLES={'gameTransaction':'gametransaction_prod','walletTransaction':'wallettransaction_prod'}

#def control(conn, ent, d0, d1, status, note='', run=None, brand=None):
#    with conn.cursor() as c:
#        c.execute(f'''INSERT INTO {"db_admin"}.etl_strict_delivery_control
#          (brand,entity,window_date_from,window_date_to,state,notes,dq_run_id)
#          VALUES(%s,%s,%s,%s,%s,%s,%s)
#          ON CONFLICT (brand,entity,window_date_from,window_date_to)
#          DO UPDATE SET state=EXCLUDED.state,notes=EXCLUDED.notes,dq_run_id=EXCLUDED.dq_run_id,updated_at=now()''',
#          (brand,ent,d0,d1,status,note,run))
#    conn.commit()


def control(conn, ent, d0, d1, status, note='', run=None, brand=None):
    with conn.cursor() as c:
        c.execute(f'''INSERT INTO {"db_admin"}.etl_strict_delivery_control
          (brand,entity,window_date_from,window_date_to,state,notes,dq_run_id)
          VALUES(%s,%s,%s,%s,%s,%s,%s)
          ON CONFLICT (brand,entity,window_date_from,window_date_to)
          DO UPDATE SET state=EXCLUDED.state,notes=EXCLUDED.notes,dq_run_id=EXCLUDED.dq_run_id,updated_at=now()''',
          ("inplay" if brand in ("inplayv1", "inplayv2") else brand,ent,d0,d1,status,note,run))
    conn.commit()


def phase_marker(phase, note=''):
    print('[STRICT PHASE] phase=%s %s' % (phase, note), flush=True)

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--date-from',required=True);p.add_argument('--date-to',required=True)
    p.add_argument('--entity',required=True,choices=tuple(TABLES))
    p.add_argument('--brand', required=True)
    p.add_argument('--stage-only', action='store_true',help='Load into staging only; skip DQ and production publishing')
    p.add_argument('--dq-only', action='store_true',help='Validate existing staging and record DQ PASS without publishing')

    p.add_argument('--resume-from',choices=('staging','dq','publish'),default=None,
                   help='Explicit recovery phase. staging replays safely from window start; dq reuses full staging; publish requires approved DQ.')
    a=p.parse_args()
    if a.dq_only and a.resume_from != 'dq':
        raise SystemExit('--dq-only requires --resume-from dq')
    from .main import load_brand
    adapter, config = load_brand(a.brand)
    brand = config.BRAND_KEY
    strict_brand = "inplay" if brand in ("inplayv1", "inplayv2") else brand
    start=datetime.strptime(a.date_from,'%Y-%m-%d'); end=datetime.strptime(a.date_to,'%Y-%m-%d')
    if end<=start: raise SystemExit('end must be after start')
    if a.entity == 'gameTransaction':
        if brand == 'inplayv1' and end.date() > datetime(2026, 5, 26).date():
            raise RuntimeError('BLOCK: InplayV1 last eligible reporting day is 2026-05-25')
        if brand == 'inplay' and start.date() < datetime(2026, 5, 26).date():
            raise RuntimeError('BLOCK: InplayV2 first eligible source day is 2026-05-26')
    conn=connect('iestdbrds',env_prefix='TARGET')
    table=TABLES[a.entity]; lo=start+timedelta(hours=6);hi=end+timedelta(hours=6)
    reporting_offset=config.NEXUS_REPORTING_TIME_OFFSET_HOURS
    stage_lo=lo if brand == 'inplay' else lo-timedelta(hours=reporting_offset)
    stage_hi=hi if brand == 'inplay' else hi-timedelta(hours=reporting_offset)
    admin_schema = 'db_admin'
    stage_schema = 'etl_stage'
    prod_schema = 'public'
    # No restoration/reset of failed staging without deliberate DBA-controlled action.
    try:
        # Serializes strict stage/DQ/publish runs across hosts using one session-level DB lock.
        with conn.cursor() as lock_cur:
            lock_cur.execute("SET TIME ZONE 'UTC'")
            lock_cur.execute('SELECT pg_advisory_lock(hashtext(%s),hashtext(%s))', (brand+'_strict_pipeline','all_entities'))
        conn.commit()
        
        with conn.cursor() as c:
            c.execute(
                f'SELECT state,dq_run_id,notes FROM {admin_schema}.etl_strict_delivery_control WHERE brand=%s AND entity=%s AND window_date_from=%s AND window_date_to=%s',
                (strict_brand, a.entity, start.date(), end.date())
            )
            previous=c.fetchone()

            c.execute(f'SELECT count(*) FROM {stage_schema}.'+table)
            staged_count=c.fetchone()[0]
            # Daily fresh imports remain strictly separated from recovery.
            if a.entity=='gameTransaction':
                c.execute(f"SELECT count(*) FROM {prod_schema}.{table} WHERE brand=%s AND fulletl=%s AND enddatetime >= %s AND enddatetime < %s",(config.BRAND,config.FULL_ETL,lo,hi))
            else:
                c.execute(f"SELECT count(*) FROM {prod_schema}.wallettransaction_prod WHERE platform=%s AND createddatetime >= %s AND createddatetime < %s",(config.WALLET_PLATFORM,lo,hi))
            existing=c.fetchone()[0]
        conn.rollback()
        if previous and previous[0]=='RELEASED':
            print('[STRICT][ALREADY RELEASED] No further import',flush=True)
            return
        if existing:
            raise RuntimeError('BLOCK: %d production rows exist in window; do not overwrite or delete automatically' % existing)
        if a.resume_from is None:
            if previous: raise RuntimeError('BLOCK: delivery state=%s; use --resume-from dq/staging/publish after verification' % previous[0])
            if staged_count: raise RuntimeError('BLOCK: staging is not empty; no new daily import while staging held')
        else:
            if not previous: raise RuntimeError('BLOCK: no existing delivery record for recovery')
            if previous[0] not in ('FAILED','STAGING','VALIDATING','PASS'):
                raise RuntimeError('BLOCK: recovery not permitted from state=%s' % previous[0])
            if staged_count <= 0:
                raise RuntimeError('BLOCK: recovery requires existing staging; new import must use normal command')
            if a.resume_from=='publish' and (previous[0]!='PASS' or not previous[1]):
                raise RuntimeError('BLOCK: publish recovery requires control state PASS and approved dq_run_id; failed states must rerun DQ')
            if a.resume_from=='staging' and previous[0]=='PASS':
                raise RuntimeError('BLOCK: cannot replay stage after approved DQ; use publish recovery')
            print('[STRICT RESUME] phase=%s old_state=%s staged_rows=%d' % (a.resume_from,previous[0],staged_count),flush=True)
        if a.resume_from in (None,'staging'):
            control(conn,a.entity,start.date(),end.date(),'STAGING',
                'Initial stage import' if a.resume_from is None else 'Manual replay from window start; existing staged keys retained',brand=brand)
        env=os.environ.copy()
        env['PGTZ']='UTC'
        env['NEXUS_STRICT_STAGE_PROCESS']='YES'
        env['SKIP_NEXUS_DQ']='True'    # must run exact DQ ourselves before any publish
        env['MIGRATION_AUTO_COMPARE_AFTER_RUN']='false'
        env['MIGRATION_EMAIL_COMPARISON_ON_VARIANCE']='false'
        python=sys.executable
        cmd=[python,'-m','helpers.main','--brand',brand,'--migrate-all','--entity',a.entity,
             '--date-from',a.date_from,'--date-to',a.date_to,'--batch-size',os.getenv('BATCH_SIZE','1000'),
             '--commit-every',os.getenv('COMMIT_EVERY','1000'),'--show-progress']
        # InplayV2 source business window is midnight-to-midnight in Asia/Manila.
        # Reuse the existing time-aware extraction option; InplayV1 is unchanged.
        if brand == 'inplay' and a.entity == 'gameTransaction':
            cmd.append('--time-pht-aware')
        if a.resume_from in (None,'staging'):
            phase_marker('STAGING', 'action=load')
            print('[STRICT] Stage import:', ' '.join(cmd),flush=True)
            subprocess.run(cmd,env=env,check=True)
            control(conn,a.entity,start.date(),end.date(),'VALIDATING','stage import complete; DQ pending',brand=brand)
            if a.stage_only:
                phase_marker(
                    'STAGING_COMPLETE',
                    'Staging committed; DQ and production publishing skipped'
                )
                print(
                    '[STRICT] Load-only completed successfully. '
                    'No DQ or production publishing performed.',
                    flush=True
                )
                return
             
        elif a.resume_from=='dq':
            phase_marker('STAGING_CHECK', 'action=verify-complete')
            if brand == 'inplay':
                # The DQ validates the full source-day row count and staged population.
                if not staged_count:
                    raise RuntimeError('No InplayV2 DEV rows staged for DQ')
                control(conn,a.entity,start.date(),end.date(),'VALIDATING',
                        'Staging DQ resume; staging rows=%d' % staged_count,brand=brand)
            else:
            # Reject partial stages BEFORE DQ; a completed stage should contain the
            # same source count. DQ will additionally verify identifiers and finances.
                src=connect('iestdl',env_prefix='SOURCE')
                try:
                    with src.cursor() as cur:
                        cur.execute('SET TIME ZONE %s',('UTC',))
                        if a.entity=='gameTransaction' and brand=='inplayv1':
                            cur.execute('SELECT count(*) FROM kemet."GameTransactionInplayV1_dmp" WHERE game_dt >= %s AND game_dt < %s',(lo,hi))
                        else:
                            # Wallet data comes from two source tables; its date expressions
                            # must be validated by the existing DQ, not guessed here.
                            raise RuntimeError('Wallet DQ-only resume needs wallet source counts verified by DBA; not enabled in this release')
                        source_n=cur.fetchone()[0]
                    src.rollback()
                finally:
                    src.close()
                if staged_count!=source_n:
                    raise RuntimeError('BLOCK: staging incomplete: stage=%d source=%d. Use --resume-from staging for idempotent replay.'%(staged_count,source_n))
                control(conn,a.entity,start.date(),end.date(),'VALIDATING','manual DQ resume; staging population count verified',brand=brand)
        elif a.resume_from=='publish':
            phase_marker('PUBLISH_PRECHECK', 'action=verify-approval')
            # Last strict DQ was already approved. Do not rerun stage or DQ.
            run=previous[1]
            phase_marker('PUBLISH', 'action=resume')
            subprocess.run([python,'-m','helpers.strict_publish','--brand',brand,'--entity',a.entity,
                '--date-from',a.date_from,'--date-to',a.date_to,'--dq-run-id',run],env=env,check=True)
            print('[STRICT RESUME] Published approved staged batch',flush=True)
            return
        dq_started=datetime.now(timezone.utc)
        if a.entity=='gameTransaction':
            if brand == 'inplayv1':
                phase_marker('DATATYPE_DQ', 'action=verify-jsonb-column-compatibility')
                subprocess.run([
                python, '-m', 'helpers.dq_datatype',
                '--brand', brand, '--entity', a.entity,
                '--date-from', a.date_from, '--date-to', a.date_to,
                '--chunk-size', os.getenv('DQ_DATATYPE_CHUNK_SIZE','2000')
                ], env=env, check=True)
            else:
                phase_marker('DATATYPE_DQ', 'InplayV2 full-window staging structural checks in dq_reconcile')
        else:
            raise RuntimeError('Wallet datatype policy not installed')

        dq=[python,'-m','helpers.dq_reconcile','--brand',brand,'--entity',a.entity,
             '--date-from',a.date_from,'--date-to',a.date_to,'--chunk-size',os.getenv('DQ_CHUNK_SIZE','1000')]
        if brand == 'inplay': dq.extend(['--dq-schema', admin_schema])
        phase_marker('DQ', 'action=validate-staging')
        print('[STRICT] Staging DQ:', ' '.join(dq),flush=True)
        subprocess.run(dq,env=env,check=True)
        with conn.cursor() as c:
            c.execute(f'''SELECT run_id,status,trust_result,source_rows,target_rows,missing_in_target,hash_mismatch_rows
              FROM {admin_schema}.etl_dq_run_summary
              WHERE brand=%s AND entity=%s
                AND window_from=%s AND window_to=%s AND created_at >= %s
              ORDER BY created_at DESC LIMIT 1''',
              ('inplay',a.entity,lo,hi,dq_started))
            result=c.fetchone()
        conn.rollback()
        if not result or result[1]!='PASS' or result[2]!='PASS_PROD_RELEASED' or result[3]!=result[4] or result[5] or result[6]:
            raise RuntimeError('BLOCK: DQ is not exact PASS: %r'%(result,))
        run=result[0]
        control(conn,a.entity,start.date(),end.date(),'PASS','Exact staging DQ passed; publication pending',run,brand=brand)
        if a.dq_only:
            print('[STRICT DQ ONLY] PASS; staging approved. Publication NOT started.',flush=True)
            return
        phase_marker('PUBLISH', 'action=after-dq-pass')
        subprocess.run([python,'-m','helpers.strict_publish','--brand',brand,'--entity',a.entity,
             '--date-from',a.date_from,'--date-to',a.date_to,'--dq-run-id',run],env=env,check=True)
        print('[STRICT] Completed successfully',flush=True)
    except BaseException as exc:
        try:
            if not conn.closed: conn.rollback()
            # Failures before creating control record should not forge a new job.
            with conn.cursor() as c:
                c.execute(f'''SELECT 1 FROM {admin_schema}.etl_strict_delivery_control WHERE brand=%s AND entity=%s AND window_date_from=%s AND window_date_to=%s''',(strict_brand,a.entity,start.date(),end.date()))
                exists=bool(c.fetchone())
            conn.rollback()
            if exists:
                with conn.cursor() as chk:
                    chk.execute(f'SELECT state FROM {admin_schema}.etl_strict_delivery_control WHERE brand=%s AND entity=%s AND window_date_from=%s AND window_date_to=%s',(strict_brand,a.entity,start.date(),end.date()))
                    current=chk.fetchone()[0]
                conn.rollback()
                # Keep a successful DQ approval intact if the publisher failed.
                # The publisher commits RELEASED atomically with the INSERT.
                if current not in ('RELEASED','PASS'):
                    control(conn,a.entity,start.date(),end.date(),'FAILED',str(exc)[:1000],brand=brand)
                elif current=='PASS':
                    print('[STRICT][PUBLISH ERROR] DQ approval retained; verify database, then --resume-from publish',flush=True)
        except Exception as note_exc:
            print('[AUDIT WRITE ERROR]',repr(note_exc),file=sys.stderr,flush=True)
        phase_marker('FAILED', 'error_type=%s' % type(exc).__name__)
        raise
    finally:
        conn.close()
if __name__=='__main__': 
    main()

