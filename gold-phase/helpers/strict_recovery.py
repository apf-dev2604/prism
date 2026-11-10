def backfill_missing_transactions(
    failed_dq_run_id,
    approved_manifest,
    dry_run=True
):
    """
    1. Read DQ evidence.
    2. Verify upstream Data Lake DQ PASS.
    3. Identify genuinely missing transaction IDs.
    4. Stage and validate only those IDs.
    5. Publish approved INSERTs atomically.
    6. Record the recovery outcome.
    """
    raise NotImplementedError(
        "Controlled backfill publisher must be implemented."
    )


def repair_existing_transactions(
    failed_dq_run_id,
    approved_manifest,
    dry_run=True
):
    """
    1. Read DQ mismatch evidence.
    2. Retrieve authoritative source values.
    3. Compare existing production values.
    4. Stage and validate proposed corrections.
    5. Apply approved UPDATEs atomically.
    6. Rerun DQ and record results.
    """
    raise NotImplementedError(
        "Controlled repair publisher must be implemented."
    )
