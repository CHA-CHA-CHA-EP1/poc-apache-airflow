from airflow.decorators import dag, task
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
            requests={"cpu": "100m", "memory": "52Mi"},
        ),
        get_logs=True,
        do_xcom_push=True,
        in_cluster=False,
        config_file="/usr/local/airflow/.kube/config",
    )

@task
def extract_field(data: dict, field: str) -> str:
    return str(data[field])

@task.branch
def decide_insert_missing(compare_result: dict) -> str:
    if compare_result["status"] == "fail":
        return "insert_missing"
    return "skip_insert"

@task
def skip_insert():
    print("counts match, nothing to insert")

@dag(
    dag_id="reconcile",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
)

def reconcile_vb_dag():
    channel = "VB"
    date = "2026-07-14"

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

    branch = decide_insert_missing(compare.output)

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

    copy_encrypt >> [read_control, count_document]
    [control_count, document_count] >> compare >> branch
    branch >> insert_missing
    branch >> skip

reconcile_vb_dag()
