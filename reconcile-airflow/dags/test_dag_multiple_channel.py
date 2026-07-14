from airflow.decorators import dag, task, task_group
from pendulum import datetime


# Mock channels with scenarios
# True = insert_missing triggered (mismatch), False = counts match, skip
CHANNEL_SCENARIOS = {
    "VB": {"match": True, "mismatch": 5},
    "KTB": {"match": False, "mismatch": 0},
    "SCB": {"match": True, "mismatch": 12},
    "GSB": {"match": False, "mismatch": 0},
    "BBL": {"match": True, "mismatch": 3},
    "TMB": {"match": False, "mismatch": 0},
    "KBANK": {"match": True, "mismatch": 8},
    "BAY": {"match": False, "mismatch": 0},
    "CIMB": {"match": False, "mismatch": 0},
    "UOB": {"match": True, "mismatch": 1},
}


@task
def mock_copy_encrypt(channel: str, date: str) -> dict:
    print(f"[mock] copy_encrypt completed for {channel} on {date}")
    return {"channel": channel, "date": date, "status": "ok"}


@task
def mock_read_control_file(channel: str, date: str) -> dict:
    scenario = CHANNEL_SCENARIOS[channel]
    total = 100
    if scenario["match"]:
        total += scenario["mismatch"]
    return {"total_records": total, "channel": channel, "date": date}


@task
def mock_count_document(channel: str, date: str) -> dict:
    scenario = CHANNEL_SCENARIOS[channel]
    document_count = 100
    return {"document_count": document_count, "channel": channel, "date": date}


@task
def extract_field(data: dict, field: str) -> str:
    return str(data[field])


@task
def mock_compare(channel: str, date: str, control_count: str, document_count: str) -> dict:
    cc = int(control_count)
    dc = int(document_count)
    mismatch = cc - dc
    status = "fail" if mismatch > 0 else "pass"
    result = {
        "channel": channel,
        "date": date,
        "control_count": cc,
        "document_count": dc,
        "mismatch": mismatch,
        "status": status,
    }
    print(f"[mock] compare for {channel}: control={cc}, document={dc}, mismatch={mismatch}, status={status}")
    return result


@task
def skip_insert(channel: str):
    print(f"[mock] {channel}: counts match, nothing to insert")


@task
def mock_insert_missing(channel: str, date: str, missing_records: str):
    print(f"[mock] {channel}: inserting {missing_records} missing records on {date}")


@task(trigger_rule="all_done")
def collect_result(compare_result: dict) -> dict:
    return compare_result


@task_group(group_id="reconcile_channel")
def reconcile_channel(channel: str, date: str):
    copy = mock_copy_encrypt(channel, date)
    control = mock_read_control_file(channel, date)
    document = mock_count_document(channel, date)

    control_count = extract_field.override(task_id="extract_control_count")(
        control, "total_records"
    )
    document_count = extract_field.override(task_id="extract_document_count")(
        document, "document_count"
    )

    compare = mock_compare(
        channel, date,
        control_count, document_count,
    )

    mismatch_count = extract_field.override(task_id="extract_mismatch")(
        compare, "mismatch"
    )

    @task.branch(task_id="decide_insert_missing")
    def decide_insert_missing(compare_result: dict, insert_id: str, skip_id: str) -> str:
        if compare_result["status"] == "fail":
            print(f"  -> branch: insert_missing ({compare_result['mismatch']} mismatches)")
            return insert_id
        print("  -> branch: skip (counts match)")
        return skip_id

    insert = mock_insert_missing(channel, date, mismatch_count)
    skip = skip_insert(channel)

    branch = decide_insert_missing(compare, insert.operator.task_id, skip.operator.task_id)

    copy >> [control, document]
    [control_count, document_count] >> compare >> branch
    branch >> insert
    branch >> skip

    result = collect_result(compare)
    [insert, skip] >> result

    return result


@task(trigger_rule="all_done")
def send_email_report(results: list):
    print("\n========== RECONCILE SUMMARY ==========")
    for r in results:
        status = "PASS" if r["status"] == "pass" else f"FAIL (mismatch={r['mismatch']})"
        print(f"  {r['channel']}: {status}")
    print("=======================================\n")

    failures = [r for r in results if r["status"] == "fail"]
    if failures:
        print(f"ALERT: {len(failures)} channel(s) need insert_missing: {[f['channel'] for f in failures]}")
    else:
        print("All channels passed — no inserts needed.")


@dag(
    dag_id="test_reconcile_multi_channel",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["reconcile", "test"],
)
def test_reconcile_pipeline():
    date = "2026-07-14"

    results = [
        reconcile_channel.override(group_id=f"channel_{c.lower()}")(c, date)
        for c in sorted(CHANNEL_SCENARIOS.keys())
    ]
    send_email_report(results)


test_reconcile_pipeline()