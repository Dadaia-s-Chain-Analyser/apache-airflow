import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args ={
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "marco_aurelio_reis@yahoo.com.br",
    "retries": 1,
    "retry_delay": timedelta(minutes=5) 
}


COMMON_PARMS = dict(
        image="marcoaureliomenezes/batch-contract-txs:latest",
        api_version='auto', 
        docker_url="unix:///var/run/docker.sock",
        network_mode='airflow-network',
        auto_remove=True,
        mount_tmp_dir=False)


def python_operator_test(data):
    print('HELLO DADAIA FROM PYTHON OPERATOR')
    print(data)


with DAG( 
            f"p_4_dag_test", 
            start_date=datetime(2023, 8, 26, 3), 
            schedule_interval="@daily", 
            default_args=default_args,
            max_active_runs=1,
            catchup=False
        ) as dag:


    test_bash_operator = BashOperator(
        task_id="test_bash_operator",
        bash_command="""echo {{ ds }} && echo {{ var.value.TESTE }}""",
        params={"file": "log.csv"}
    )

    test_bash_operator_2 = BashOperator(
        task_id="test_bash_operator_2",
        bash_command="""echo {{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d-%H-%M") }}"""
    )


    # test_python_operator = PythonOperator(
    #     task_id="test_python_operator",
    #     python_callable=python_operator_test,
    #     op_args=["{{ ts }}"]
    # )


    # test_docker_operator_1 = DockerOperator(
    #     task_id="test_docker_operator_1",
    #     container_name="test_docker_operator_1",
    #     entrypoint=["python", "-u", "4_testing_operator.py"],
    #     depends_on_past=True,
    #     environment=dict(
    #                     START_DATE = "{{ prev_ds }}",
    #                     END_DATE = "{{ ds }}",
    #                     NETWORK = "ENVIROMENT VARIABLE"),
    #     **COMMON_PARMS
    # )

    # test_docker_operator_2 = DockerOperator(
    #     task_id="test_docker_operator_2",
    #     container_name="test_docker_operator_2",
    #     entrypoint=["python", "-u", "4_testing_operator.py"],
    #     depends_on_past=True,
    #     environment=dict(
    #                     START_DATE = "{{ prev_ds }}",
    #                     END_DATE = "{{ ds }}",
    #                     NETWORK = "ENVIROMENT VARIABLE"),
    #     **COMMON_PARMS
    # )


    test_bash_operator >> test_bash_operator_2 #test_python_operator >> test_docker_operator_1 >> test_docker_operator_2
