from airflow.decorators import dag, task, task_group
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from pendulum import datetime


def make_pod_operator(task_id: str, command_args: list[str]) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace="reconcile",
        image="reconcile-cli:dev",
        image_pull_policy="Never",
        cmds=["/app/reconcile-cli"],
        arguments=command_args,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "256Mi"},
        ),
        get_logs=True,
        do_xcom_push=True,
        in_cluster=False,
        config_file="/usr/local/airflow/.kube/config",
    )


@task
def extract_field(data: dict, field: str) -> str:
    return str(data[field])


@task
def skip_insert():
    print("counts match, nothing to insert")


@task_group(group_id="reconcile_channel")
def reconcile_channel(channel: str, date: str):
    copy_encrypt = make_pod_operator(
        "copy_encrypt", ["copy-encrypt", "--channel", channel, "--date", date]
    )
    read_control = make_pod_operator(
        "read_control_file", ["read-control-file", "--channel", channel, "--date", date]
    )
    count_document = make_pod_operator(
        "count_document", ["count-document", "--channel", channel, "--date", date]
    )

    control_count = extract_field.override(task_id="extract_control_count")(
        read_control.output, "total_records"
    )
    document_count = extract_field.override(task_id="extract_document_count")(
        count_document.output, "document_count"
    )

    compare = make_pod_operator(
        "compare",
        [
            "compare",
            "--channel", channel,
            "--date", date,
            "--control-count", control_count,
            "--document-count", document_count,
        ],
    )

    mismatch_count = extract_field.override(task_id="extract_mismatch")(
        compare.output, "mismatch"
    )
    insert_missing = make_pod_operator(
        "insert_missing",
        [
            "insert-missing",
            "--channel", channel,
            "--date", date,
            "--missing-records", mismatch_count,
        ],
    )
    skip = skip_insert()

    # ใช้ .task_id ของ operator จริง แทนการ hardcode string เอง
    @task.branch(task_id="decide_insert_missing")
    def decide_insert_missing(compare_result: dict, insert_id: str, skip_id: str) -> str:
        if compare_result["status"] == "fail":
            return insert_id
        return skip_id

    branch = decide_insert_missing(compare.output, insert_missing.task_id, skip.operator.task_id)

    copy_encrypt >> [read_control, count_document]
    [control_count, document_count] >> compare >> branch
    branch >> insert_missing
    branch >> skip

    return compare.output


@task(trigger_rule="all_done")
def send_email_report(results: list):
    for r in results:
        print(f"reconcile result: {r}")


@dag(
    dag_id="reconcile_multi_channel",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["reconcile"],
)
def reconcile_pipeline():
    channels = ["VB", "KTB"]
    date = "2026-07-14"

    results = [
        reconcile_channel.override(group_id=f"channel_{c.lower()}")(c, date)
        for c in channels
    ]
    send_email_report(results)


reconcile_pipeline()
