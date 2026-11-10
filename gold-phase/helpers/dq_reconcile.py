import time
import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from typing import Any, Dict

from .dq_common import (
    dq_arg_parser, open_context, ensure_dq_tables, source_game_summary,
    target_game_summary, iter_source_game_chunks, build_expected_game,
    fetch_target_games, game_hash, write_summary, _selected_entity,
    TOTAL_AMOUNT_TOLERANCE, decimal_abs_diff, exceeds_total_amount_tolerance,
    DQ_STATUS_PASS, DQ_STATUS_PASS_WITH_ROUNDING_NOTE, DQ_STATUS_FAIL,
    dq_trust_result, game_rounding_only, ROW_AMOUNT_TOLERANCE,
    write_mismatches, GAME_IDENTITY_FIELDS, GAME_MONETARY_FIELDS, norm_text, decimal_abs_diff,
)


def log_current_query(task: str) -> None:
    print(f"[DQ CURRENT QUERY] {task}", flush=True)

def send_dq_failure_email(row: Dict[str, Any]) -> None:
    """Best-effort alert only for material FAIL; never alter DQ persistence/exit status."""
    if row.get("status") != DQ_STATUS_FAIL:
        return
    recipient_string = os.getenv("DQ_ALERT_TO", "")
    recipients = [r.strip() for r in recipient_string.split(",") if r.strip()]
    host = os.getenv("SMTP_HOST", "")
    username = os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASSWORD", "")
    sender = os.getenv("SMTP_FROM", username)
    if not (recipients and host and sender):
        print("[DQ EMAIL][SKIP] Set DQ_ALERT_TO, SMTP_HOST and SMTP_FROM (or SMTP_USER).", flush=True)
        return
    msg = EmailMessage()
    msg["Subject"] = f"[DQ ALERT][FAIL] {row['brand']} {row['entity']} {row['window_from']}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content("\n".join(f"{k}: {row.get(k)}" for k in (
        "run_id", "brand", "entity", "window_from", "window_to", "status",
        "trust_result", "missing_in_target", "hash_mismatch_rows", "notes",
    )))
    try:
        with smtplib.SMTP(host, int(os.getenv("SMTP_PORT", "587")), timeout=30) as smtp:
            smtp.starttls()
            if username and password:
                smtp.login(username, password)
            smtp.send_message(msg)
        print(f"[DQ EMAIL][SENT] recipients={len(recipients)}", flush=True)
    except Exception as exc:
        print(f"[DQ EMAIL][ERROR] {type(exc).__name__}: {exc}", flush=True)



def run_wallet_dq(args, adapter, config, from_dt, until_dt, src, tgt) -> int:
    """Full-window DLAKE/NEXUS wallet amount and key reconciliation using existing adapter mappings."""
    from decimal import Decimal, InvalidOperation
    from psycopg2.extras import RealDictCursor
    from .db import table_ref
    from .source_fetch import fetch_json_batch
    from .dq_common import norm_ts

    started = time.perf_counter()
    run_id = f"dqrecon_{config.BRAND_KEY}_wallet_{args.date_from}_{args.date_to}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    issues = []
    mismatches = []
    src_count = 0
    total_source = Decimal(0)
    total_target = Decimal(0)
    missing = 0
    differing = 0
    duplicate_source = 0
    seen_keys = set()
    try:
        if not args.no_write_dq_tables:
            ensure_dq_tables(tgt, args.dq_schema)
        # Existing shared wallet mapping uses deposits and withdrawals as one entity.
        for kind, source_key in (("deposit", "deposits"), ("withdrawal", "withdrawals")):
            date_expr = adapter.source_date_expr_for_table(source_key, "data")
            after_dt, after_id = None, None
            while True:
                batch = fetch_json_batch(src, config.SOURCE_SCHEMA, config.SOURCE_TABLES[source_key],
                    date_expr, after_dt, after_id, args.chunk_size, from_dt, until_dt)
                src.rollback()
                if not batch:
                    break
                expected = []
                for entry in batch:
                    mapped = adapter.map_wallet(entry, kind)
                    if not mapped or not mapped.get("reference_id"):
                        issues.append(f"source row unmappable kind={kind} id={entry.get('id')}")
                        continue
                    key = (kind, str(mapped['reference_id']))
                    if key in seen_keys:
                        duplicate_source += 1
                    seen_keys.add(key)
                    expected.append(mapped)
                    src_count += 1
                    try:
                        total_source += Decimal(str(mapped.get("amount") or 0))
                    except (InvalidOperation, ValueError):
                        issues.append(f"invalid source amount key={key}")
                ids = [str(e['reference_id']) for e in expected]
                targets = {}
                if ids:
                    with tgt.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(f"SELECT referenceId AS reference_id, transactionType AS kind, amount, status, createdDatetime AS created, platform FROM {table_ref(config.TARGET_SCHEMA, config.WALLET_TRANSACTION_TABLE)} WHERE platform=%s AND transactionType=%s AND referenceId=ANY(%s)",
                            (config.WALLET_PLATFORM, kind, ids))
                        for rr in cur.fetchall():
                            targets[(str(rr['kind']).lower(), str(rr['reference_id']))] = dict(rr)
                    tgt.rollback()
                for e in expected:
                    key = (kind, str(e['reference_id']))
                    actual = targets.get(key)
                    if actual is None:
                        missing += 1
                        mismatches.append(dict(run_id=run_id, brand=config.BRAND_KEY, entity='wallet_transaction',
                            business_key=f"{kind}:{e['reference_id']}", mismatch_type='MISSING_IN_TARGET',
                            mismatch_columns=['referenceId'], source_hash=None, target_hash=None,
                            source_sample={'kind': kind, 'amount': str(e.get('amount'))}, target_sample=None))
                        continue
                    try:
                        got = Decimal(str(actual['amount']))
                        want = Decimal(str(e.get('amount') or 0))
                        diff_fields = []
                        if got != want:
                            diff_fields.append('amount')
                        if str(actual['status']).lower() != str(e.get('status') or '').lower():
                            diff_fields.append('status')
                        if norm_ts(actual['created']) != norm_ts(e.get('created')):
                            diff_fields.append('createdDatetime')
                        if diff_fields:
                            differing += 1
                            mismatches.append(dict(run_id=run_id, brand=config.BRAND_KEY, entity='wallet_transaction',
                                business_key=f"{kind}:{e['reference_id']}", mismatch_type='VALUE_MISMATCH',
                                mismatch_columns=diff_fields, source_hash=None, target_hash=None,
                                source_sample={'amount': str(want), 'status': str(e.get('status'))},
                                target_sample={'amount': str(got), 'status': str(actual['status'])}))
                    except (InvalidOperation, ValueError, TypeError):
                        differing += 1
                        issues.append(f"invalid target amount key={key}")
                after_id = str(batch[-1]['id'])
                last_data = batch[-1].get('data') or {}
                date_keys = ('transferDate', 'transferdate', 'TransferDate')
                from .dates import parse_iso_dt
                last_date = adapter._first(last_data, *date_keys)
                after_dt = str(last_date) if last_date is not None else None
                if after_dt is None:
                    raise RuntimeError(f"Cannot advance wallet source pagination kind={kind}")
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
        with tgt.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT count(*) AS n, count(DISTINCT (transactionType, referenceId)) AS distinct_n, coalesce(sum(amount),0) AS total FROM {table_ref(config.TARGET_SCHEMA, config.WALLET_TRANSACTION_TABLE)} WHERE platform=%s AND transactionType IN ('deposit','withdrawal') AND createdDatetime >= %s::timestamp AND createdDatetime < %s::timestamp",
                (config.WALLET_PLATFORM, from_dt, until_dt))
            agg = dict(cur.fetchone())
        tgt.rollback()
        target_count = int(agg['n'])
        total_target = Decimal(str(agg['total']))
        checks = [f"source_unmappable={len(issues)}" if issues else None,
                  f"missing={missing}" if missing else None,
                  f"value_mismatch={differing}" if differing else None,
                  f"source_duplicates={duplicate_source}" if duplicate_source else None,
                  f"row_count={src_count}/{target_count}" if src_count != target_count else None,
                  f"financial_amount={total_source}/{total_target}" if total_source != total_target else None,
                  f"target_duplicates={target_count-int(agg['distinct_n'])}" if target_count != int(agg['distinct_n']) else None]
        failures = [v for v in checks if v]
        status = DQ_STATUS_FAIL if failures else DQ_STATUS_PASS
        duration = time.perf_counter()-started
        #row = dict(run_id=run_id, brand=config.BRAND_KEY, entity='wallet_transaction', window_from=from_dt,
        row = dict(run_id=run_id, brand=config.BRAND_KEY, entity='wallet_transaction', etl_stage='DLAKE-NEXUS', window_from=from_dt,
            window_to=until_dt, source_rows=src_count, target_rows=target_count,
            source_distinct_keys=len(seen_keys), target_distinct_keys=int(agg['distinct_n']),
            source_duplicate_keys=duplicate_source, target_duplicate_keys=target_count-int(agg['distinct_n']),
            source_total_bet=total_source, target_total_bet=total_target,
            missing_in_target=missing, extra_in_target=max(0,target_count-src_count), hash_mismatch_rows=differing,
            status=status, trust_result=dq_trust_result(status, config.BRAND_KEY), notes='; '.join(failures) or 'All wallet DQ checks passed.',
            duration_seconds=round(duration,3), duration_minutes=round(duration/60,3))
        if not args.no_write_dq_tables:
            write_summary(tgt,args.dq_schema,row)
            if mismatches:
                write_mismatches(tgt,args.dq_schema,mismatches)
            send_dq_failure_email(row)
        print(f"[WALLET DQ] status={status} trust_result={row['trust_result']} source={src_count} target={target_count} amount_source={total_source} amount_target={total_target}",flush=True)
        return 0 if not failures else 2
    finally:
        src.close(); tgt.close()


def run_inplayv2_staging_dq(args) -> int:
    """Fail-closed, full-window InplayV2 ID and monetary source-to-staging DQ.

    Source identity: public."GameTransactionInplayV2".id
    Source table id is used for both externalid mapping and stable pagination.
    Target identity: etl_stage.gametransaction_prod.externalid
    Both use the developer-approved Asia/Manila source window and the
    corresponding +6-hour UTC-labelled NEXUS reporting window.
    """
    from datetime import timedelta
    from decimal import Decimal, InvalidOperation
    from .db import connect
    from .dq_common import ensure_dq_tables, write_summary, write_mismatches

    start = datetime.combine(args.date_from.date(), datetime.min.time()) + timedelta(hours=6)
    end = datetime.combine(args.date_to.date(), datetime.min.time()) + timedelta(hours=6)
    if end <= start:
        raise RuntimeError('Invalid InplayV2 business window')
    begun = time.perf_counter()
    run_id = f"dqrecon_inplay_game_{args.date_from.date()}_{args.date_to.date()}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    conn = connect('iestdbrds', env_prefix='TARGET')
    source = None
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute("""SELECT count(*),count(DISTINCT externalid),
                count(*) FILTER (WHERE externalid IS NULL OR btrim(externalid::text)=''),
                count(*) FILTER (WHERE fulletl IS DISTINCT FROM false),
                count(*) FILTER (WHERE startdatetime IS NULL OR enddatetime IS NULL),
                count(*) FILTER (WHERE tableroomid IS NULL),
                coalesce(sum(betamount),0),coalesce(sum(payoutamount),0),
                coalesce(sum(computed_jackpot_contribution),0),
                coalesce(sum(computed_jackpot_payout),0)
                FROM etl_stage.gametransaction_prod
                WHERE enddatetime >= %s AND enddatetime < %s AND brand=%s AND fulletl=false""",
                (start,end,'inplay'))
            (count, distinct, empty_keys, wrong_fulletl, null_dates, null_rooms,
             bet, payout, jackpot_contribution, jackpot_payout) = cur.fetchone()
            cur.execute('SELECT count(*) FROM etl_stage.gametransaction_prod')
            all_staged = cur.fetchone()[0]
            cur.execute("""SELECT count(*) FROM public.gametransaction_prod
                WHERE brand=%s AND fulletl=false AND enddatetime >= %s AND enddatetime < %s""",
                ('inplay',start,end))
            already_published = cur.fetchone()[0]
        conn.rollback()

        source = connect('iestdl', env_prefix='SOURCE')
        # One source snapshot for the count, distinct-ID check and entire ID scan.
        source.set_session(isolation_level='REPEATABLE READ', readonly=True)
        with source.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute("""SELECT count(*),count(DISTINCT id),
                    count(*) FILTER (WHERE id IS NULL)
                FROM public."GameTransactionInplayV2"
                WHERE data IS NOT NULL
                  AND settled_datetime >= (%s::timestamp AT TIME ZONE 'Asia/Manila')
                  AND settled_datetime < (%s::timestamp AT TIME ZONE 'Asia/Manila')""",
                (args.date_from.date(),args.date_to.date()))
            source_count, source_distinct, source_missing_ids = cur.fetchone()

        failures = []
        details = []
        missing = 0
        differing = 0
        bad_source_amounts = 0
        scanned = 0
        # These totals are independently computed from source JSONB values.
        source_bet = Decimal(0)
        source_payout = Decimal(0)
        source_jc = Decimal(0)
        source_jp = Decimal(0)
        source_keys = ('bet', 'payout', 'jackpotContribution', 'jackpotPayout')
        target_keys = ('betamount', 'payoutamount',
                       'computed_jackpot_contribution', 'computed_jackpot_payout')
        row_tolerance = ROW_AMOUNT_TOLERANCE

        def money(raw, field, key):
            nonlocal bad_source_amounts
            try:
                return Decimal(str(raw if raw not in (None, '') else 0))
            except (InvalidOperation, ValueError, TypeError):
                bad_source_amounts += 1
                if len(details) < 100:
                    details.append(dict(run_id=run_id,brand='inplay',entity='gameTransaction',
                        business_key=key,mismatch_type='INVALID_SOURCE_AMOUNT',
                        mismatch_columns=[field],source_hash=None,target_hash=None,
                        source_sample={field:str(raw)},target_sample=None))
                return Decimal(0)

        # A named cursor keeps source memory bounded for large business dates.
        with source.cursor(name='inplayv2_full_day_dq') as sc:
            sc.itersize = args.chunk_size
            sc.execute("""SELECT id::text, id::text,
                    data->>'bet', data->>'payout',
                    data->>'jackpotContribution', data->>'jackpotPayout'
                FROM public."GameTransactionInplayV2"
                WHERE data IS NOT NULL
                  AND settled_datetime >= (%s::timestamp AT TIME ZONE 'Asia/Manila')
                  AND settled_datetime < (%s::timestamp AT TIME ZONE 'Asia/Manila')
                ORDER BY settled_datetime, id""",
                (args.date_from.date(),args.date_to.date()))
            while True:
                batch = sc.fetchmany(args.chunk_size)
                if not batch:
                    break
                keys = [r[1] for r in batch if r[1] is not None]
                with conn.cursor() as cur:
                    cur.execute("""SELECT externalid::text,betamount,payoutamount,
                            computed_jackpot_contribution,computed_jackpot_payout
                        FROM etl_stage.gametransaction_prod
                        WHERE brand=%s AND fulletl=false
                          AND enddatetime >= %s AND enddatetime < %s
                          AND externalid::text = ANY(%s)""",
                        ('inplay',start,end,keys))
                    rows = cur.fetchall()
                conn.rollback()
                actual = {}
                for r in rows:
                    actual.setdefault(r[0], []).append(r[1:])
                for source_row_id, key, *raw_values in batch:
                    scanned += 1
                    if key is None:
                        missing += 1
                        if len(details) < 100:
                            details.append(dict(run_id=run_id,brand='inplay',entity='gameTransaction',
                                business_key=source_row_id,mismatch_type='MISSING_SOURCE_ID',
                                mismatch_columns=['id'],source_hash=None,target_hash=None,
                                source_sample={'source_row_id':source_row_id},target_sample=None))
                    expected = [money(raw, name, key or source_row_id)
                                for raw,name in zip(raw_values,source_keys)]
                    source_bet += expected[0]
                    source_payout += expected[1]
                    source_jc += expected[2]
                    source_jp += expected[3]
                    if key is None:
                        continue
                    matches = actual.get(key, [])
                    if len(matches) != 1:
                        missing += 1
                        if len(details) < 100:
                            details.append(dict(run_id=run_id,brand='inplay',entity='gameTransaction',
                                business_key=key,mismatch_type='MISSING_OR_DUPLICATE_IN_STAGING',
                                mismatch_columns=['externalid'],source_hash=None,target_hash=None,
                                source_sample={'id':key},target_sample={'matched_rows':len(matches)}))
                        continue
                    got = [Decimal(str(v if v is not None else 0)) for v in matches[0]]
                    changed = [field for field,want,value in zip(target_keys,expected,got)
                               if abs(want-value) > row_tolerance]
                    if changed:
                        differing += 1
                        if len(details) < 100:
                            details.append(dict(run_id=run_id,brand='inplay',entity='gameTransaction',
                                business_key=key,mismatch_type='FINANCIAL_VALUE_MISMATCH',
                                mismatch_columns=changed,source_hash=None,target_hash=None,
                                source_sample=dict(zip(target_keys,map(str,expected))),
                                target_sample=dict(zip(target_keys,map(str,got)))))
                if scanned % (args.chunk_size * 20) < len(batch):
                    print(f'[DQ INPLAYV2 SCAN] checked={scanned} source_total={source_count}',flush=True)

        if source_count != count:
            failures.append(f'source-to-staging count mismatch: source={source_count}, stage={count}')
        if scanned != source_count:
            failures.append(f'source scan count mismatch: scanned={scanned}, expected={source_count}')
        if source_missing_ids:
            failures.append(f'source rows missing id: {source_missing_ids}')
        if source_distinct != source_count:
            failures.append(f'duplicate or missing source ids: {source_count-source_distinct}')
        if not count:
            failures.append('empty staged batch')
        if count != all_staged:
            failures.append('staging contains rows outside requested window')
        if count != distinct or empty_keys:
            failures.append('duplicate or null/blank externalid')
        if wrong_fulletl:
            failures.append('fulletl not FALSE')
        if null_dates:
            failures.append('null startdatetime/enddatetime')
        if already_published:
            failures.append('production window already contains rows')
        if missing:
            failures.append(f'source IDs missing/duplicated in staging: {missing}')
        if differing:
            failures.append(f'per-ID financial mismatches: {differing}')
        if bad_source_amounts:
            failures.append(f'invalid source monetary values: {bad_source_amounts}')
        for name, left, right in (
            ('bet',source_bet,bet),('payout',source_payout,payout),
            ('computed_jackpot_contribution',source_jc,jackpot_contribution),
            ('computed_jackpot_payout',source_jp,jackpot_payout)):
            if abs(left-Decimal(str(right))) > TOTAL_AMOUNT_TOLERANCE:
                failures.append(f'{name} total mismatch: source={left} stage={right}')
        status = 'FAIL' if failures else 'PASS'
        elapsed = round(time.perf_counter()-begun,3)
        row = dict(run_id=run_id,brand='inplay',entity='gameTransaction',
            etl_stage='DLAKE-NEXUS',window_from=start,window_to=end,
            source_rows=source_count,target_rows=count,
            source_distinct_keys=source_distinct,target_distinct_keys=distinct,
            source_duplicate_keys=source_count-source_distinct,
            target_duplicate_keys=count-distinct,
            source_total_bet=source_bet,target_total_bet=bet,
            source_total_payout=source_payout,target_total_payout=payout,
            source_total_jackpot_contribution=source_jc,
            target_total_jackpot_contribution=jackpot_contribution,
            missing_in_target=missing,extra_in_target=max(0,count-source_count),
            hash_mismatch_rows=differing,
            status=status,trust_result='FAIL_IMPT_TO_NEXUS' if failures else 'PASS_PROD_RELEASED',
            notes='Full source ID and monetary reconciliation; '+
                  ('; '.join(failures) if failures else
                   f'verified {scanned} source IDs and amounts; null tableroomid={null_rooms}'),
            duration_seconds=elapsed,duration_minutes=round(elapsed/60,3))
        if not args.no_write_dq_tables:
            ensure_dq_tables(conn,'db_admin')
            write_summary(conn,'db_admin',row)
            if details:
                write_mismatches(conn,'db_admin',details)
        print(f'[DQ INPLAYV2 FULL] status={status} source={source_count} stage={count} '
              f'missing={missing} mismatches={differing} run_id={run_id} notes={row["notes"]}',flush=True)
        return 2 if failures else 0
    finally:
        if source is not None:
            source.close()
        conn.close()

def main() -> int:
    parser = dq_arg_parser("Post-load DQ reconciliation summary for fact tables.")
    args = parser.parse_args()
    entity = _selected_entity(args)
    if entity not in ("game", "wallet"):
        raise SystemExit("Select --entity gameTransaction or walletTransaction")
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be greater than zero")

    if args.brand in ('inplay','inplayv2'):
        if entity != 'game':
            raise SystemExit('InplayV2 gameTransaction routing only')
        return run_inplayv2_staging_dq(args)
    if args.brand != 'inplayv1':
        raise SystemExit('Selected brand does not have a configured DQ implementation: '+args.brand)
    adapter, config, from_dt, until_dt, src, tgt = open_context(args)
    # DISPLAY ONLY: the DQ uses the same configured source/target routing.
    if entity == "game":
        print(f"[DQ ROUTE][GAME] SOURCE={config.SOURCE_DB_NAME}.{config.SOURCE_SCHEMA}.\"{config.SOURCE_TABLES['game_transactions']}\"", flush=True)
        print(f"[DQ ROUTE][GAME] TARGET={config.TARGET_DB_NAME}.{config.TARGET_SCHEMA}.\"{config.GAME_TRANSACTION_TABLE}\"", flush=True)
    else:
        for source_key in ("deposits", "withdrawals"):
            print(f"[DQ ROUTE][WALLET] SOURCE={config.SOURCE_DB_NAME}.{config.SOURCE_SCHEMA}.\"{config.SOURCE_TABLES[source_key]}\"", flush=True)
        print(f"[DQ ROUTE][WALLET] TARGET={config.TARGET_DB_NAME}.{config.TARGET_SCHEMA}.\"{config.WALLET_TRANSACTION_TABLE}\"", flush=True)
    if entity == "wallet":
        return run_wallet_dq(args, adapter, config, from_dt, until_dt, src, tgt)
    started = time.perf_counter()
    run_id = f"dqrecon_{config.BRAND_KEY}_game_{args.date_from}_{args.date_to}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    print(f"[DQ NUMERIC TOTAL TOLERANCE] {TOTAL_AMOUNT_TOLERANCE}")
    print(f"[DQ ROW AMOUNT TOLERANCE] {ROW_AMOUNT_TOLERANCE}")
    try:
        if not args.no_write_dq_tables:
            log_current_query(f"ensure DQ tables schema={args.dq_schema}")
            ensure_dq_tables(tgt, args.dq_schema)

        log_current_query(f"source game summary brand={config.BRAND_KEY} window={from_dt} -> {until_dt}")
        src_sum = source_game_summary(src, config, from_dt, until_dt)
        log_current_query(f"target game summary brand={config.BRAND_KEY} window={from_dt} -> {until_dt}")
        tgt_sum = target_game_summary(tgt, config, from_dt, until_dt)

        missing = 0
        hash_mismatch = 0
        rounding_hash_rows = 0
        checked = 0
        mismatch_details = []
        rounding_external_ids = []
        failed_external_ids = []
        missing_external_ids = []
        notes_id_limit = max(0, int(os.getenv("DQ_NOTES_ID_LIMIT", "100")))
        chunk_no = 0
        log_current_query(f"source game chunk scan chunk_size={args.chunk_size} window={from_dt} -> {until_dt}")
        for chunk in iter_source_game_chunks(src, config, from_dt, until_dt, args.chunk_size):
            chunk_no += 1
            expected = [build_expected_game(r, config) for r in chunk]
            expected = [e for e in expected if e.get("external_id")]
            log_current_query(f"target game lookup chunk={chunk_no} external_ids={len(expected)}")
            targets = fetch_target_games(tgt, config, [e["external_id"] for e in expected])
            for e in expected:
                checked += 1
                t = targets.get(e["external_id"])
                if not t:
                    missing += 1
                    missing_external_ids.append(e["external_id"])
                    mismatch_details.append({
                        "run_id": run_id, "brand": "inplay",
                        "entity": "gameTransaction", "business_key": e["external_id"],
                        "mismatch_type": "MISSING_IN_TARGET", "mismatch_columns": [],
                        "source_hash": game_hash(e), "target_hash": None,
                        "source_sample": e, "target_sample": None,
                    })
                elif game_hash(e) != game_hash(t):
                    monetary_fields = [k for k in GAME_MONETARY_FIELDS
                                       if decimal_abs_diff(e.get(k), t.get(k)) > 0]
                    identity_fields = [k for k in GAME_IDENTITY_FIELDS
                                       if norm_text(e.get(k)) != norm_text(t.get(k))]
                    different_fields = identity_fields + monetary_fields
                    if game_rounding_only(e, t):
                        rounding_hash_rows += 1
                        rounding_external_ids.append(e["external_id"])
                        kind = "ROUNDING_ONLY"
                    else:
                        hash_mismatch += 1
                        failed_external_ids.append(e["external_id"])
                        kind = "HASH_MISMATCH"
                    mismatch_details.append({
                        "run_id": run_id, "brand": "inplay",
                        "entity": "gameTransaction", "business_key": e["external_id"],
                        "mismatch_type": kind, "mismatch_columns": different_fields,
                        "source_hash": game_hash(e), "target_hash": game_hash(t),
                        "source_sample": e, "target_sample": t,
                    })
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

        duration = time.perf_counter() - started
        status = DQ_STATUS_PASS
        notes = []
        hard_failures = []
        rounding_notes = []
        if missing:
            hard_failures.append(f"missing_in_target={missing}")
        if hash_mismatch:
            hard_failures.append(f"hash_mismatch_rows={hash_mismatch}")
        if rounding_hash_rows:
            rounding_notes.append(f"rounding_only_hash_rows={rounding_hash_rows} row_amount_tolerance={ROW_AMOUNT_TOLERANCE}")
        if src_sum.get("source_rows") != tgt_sum.get("target_rows"):
            hard_failures.append(f"row_count_variance source={src_sum.get('source_rows')} target={tgt_sum.get('target_rows')}")
        if src_sum.get("source_distinct_keys") != tgt_sum.get("target_distinct_keys"):
            hard_failures.append(f"distinct_key_variance source={src_sum.get('source_distinct_keys')} target={tgt_sum.get('target_distinct_keys')}")
        amount_pairs = [
            ("total_bet", "source_total_bet", "target_total_bet"),
            ("total_valid_bet", "source_total_valid_bet", "target_total_valid_bet"),
            ("total_payout", "source_total_payout", "target_total_payout"),
            ("total_jackpot_contribution", "source_total_jackpot_contribution", "target_total_jackpot_contribution"),
        ]
        for label, sk, tk in amount_pairs:
            diff = decimal_abs_diff(src_sum.get(sk), tgt_sum.get(tk))
            if exceeds_total_amount_tolerance(src_sum.get(sk), tgt_sum.get(tk)):
                hard_failures.append(
                    f"{label}_variance source={src_sum.get(sk)} target={tgt_sum.get(tk)} "
                    f"diff={diff} tolerance={TOTAL_AMOUNT_TOLERANCE}"
                )
            elif diff > 0:
                rounding_notes.append(
                    f"{label}_rounding_note source={src_sum.get(sk)} target={tgt_sum.get(tk)} "
                    f"diff={diff} tolerance={TOTAL_AMOUNT_TOLERANCE}"
                )
        notes.extend(hard_failures)
        notes.extend(rounding_notes)
        def add_id_note(label, ids):
            if not ids:
                return
            selected = ids[:notes_id_limit]
            suffix = f" (+{len(ids) - len(selected)} more; see etl_dq_mismatch_detail)" if len(ids) > len(selected) else ""
            notes.append(f"{label}=[{', '.join(selected)}]{suffix}")
        add_id_note("rounding_external_ids", rounding_external_ids)
        add_id_note("hash_mismatch_external_ids", failed_external_ids)
        add_id_note("missing_external_ids", missing_external_ids)
        if hard_failures:
            status = DQ_STATUS_FAIL
        elif rounding_notes:
            status = DQ_STATUS_PASS_WITH_ROUNDING_NOTE
        trust_result = dq_trust_result(status, config.BRAND_KEY)

        row: Dict[str, Any] = {
            "run_id": run_id,
            "brand": "inplay",
            "entity": "gameTransaction",
            "etl_stage": "DLAKE-NEXUS",
            "window_from": from_dt,
            "window_to": until_dt,
            **src_sum,
            **tgt_sum,
            "missing_in_target": missing,
            # Cross-database extra rows are intentionally not computed in this lightweight checker.
            # Use a database-side staging approach if extra-in-target detection is required.
            "extra_in_target": 0,
            "hash_mismatch_rows": hash_mismatch,
            "status": status,
            "trust_result": trust_result,
            "notes": "; ".join(notes) if notes else "All configured DQ checks passed for this window.",
            "duration_seconds": round(duration, 3),
            "duration_minutes": round(duration / 60.0, 3),
        }
        if not args.no_write_dq_tables:
            row.pop("source_total_valid_bet", None)
            row.pop("target_total_valid_bet", None)
            write_summary(tgt, args.dq_schema, row)
            if mismatch_details:
                log_current_query(f"write DQ mismatch details count={len(mismatch_details)}")
                write_mismatches(tgt, args.dq_schema, mismatch_details)
            send_dq_failure_email(row)
        print("[DQ RECON SUMMARY]")
        print(f"status: {status}")
        print(f"trust_result: {trust_result}")
        print(f"checked_source_rows_for_hash: {checked}")
        for k in sorted(row):
            print(f"{k}: {row[k]}")
        return 0 if status in (DQ_STATUS_PASS, DQ_STATUS_PASS_WITH_ROUNDING_NOTE) else 2
    finally:
        src.close(); tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())

