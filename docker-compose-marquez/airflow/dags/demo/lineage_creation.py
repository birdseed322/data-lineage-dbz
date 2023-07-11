from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from kafka import KafkaProducer
import asyncio
import requests
import base64
import json

marquez_backend = "http://marquez:5000/api/v1/"
airflow_backend = "http://airflow:8080/api/v1/"
airflow_user = "airflow"
airflow_password = "airflow"
kafka_connect_url = "http://kafka-connect:8083/connectors"
kafka_server = "kafka:9094"
kafka_topic = "test"


def setup_kafka_connect():
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    payload = {
        "name": "Neo4jSinkConnectorJSONString",
        "config": {
            "topics": "test",
            "connector.class": "streams.kafka.connect.sink.Neo4jSinkConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": False,
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": False,
            "errors.retry.timeout": "-1",
            "errors.retry.delay.max.ms": "1000",
            "errors.tolerance": "all",
            "errors.log.enable": True,
            "errors.log.include.messages": True,
            "neo4j.server.uri": "bolt://neo4jServer:7687",
            "neo4j.authentication.basic.username": "neo4j",
            "neo4j.authentication.basic.password": "password",
            "neo4j.topic.cud": "test",
        },
    }

    try:
        response = requests.post(
            kafka_connect_url, headers=headers, data=json.dumps(payload)
        )
        print("Connector set!")
    except requests.exceptions.RequestException as err:
        print("Error when setting connector: " + str(err))


async def create_dag_node(dagId, producer):
    try:
        print("SENDING CREATEDAG MESSAGE TO Q")
        producer.send(
            kafka_topic,
            value={
                "op": "merge",
                "properties": {
                    "dagId": dagId,
                },
                "ids": {"dagId": dagId},
                "labels": ["Dag"],
                "type": "node",
                "detach": True,
            },
        )
        producer.flush()
    except Exception as err:
        print(f"Error when creating: {dagId} Error message: {err}")


async def create_dag_task_relationship(dag_id, task_id, producer):
    try:
        print("SENDING CREATEDAGTASKRS MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "is_parent_of",
            "from": {"ids": {"dagId": dag_id}, "labels": ["Dag"], "op": "merge"},
            "to": {
                "ids": {"taskId": f"{dag_id}.{task_id}"},
                "labels": ["Airflow Task"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
        producer.flush()
    except Exception as err:
        print(
            "Error when creating: "
            + dag_id
            + " to "
            + task_id
            + " Error message: "
            + err
        )


async def create_dag_dag_relationship(dag_id_1, dag_id_2, producer):
    try:
        print("SENDING CREATEDAGDAG MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "triggers",
            "from": {
                "ids": {"dagId": dag_id_1},
                "labels": ["Dag"],
                "op": "merge",
            },
            "to": {
                "ids": {"dagId": dag_id_2},
                "labels": ["Dag"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + dag_id_1
            + " to "
            + dag_id_2
            + " Error message: "
            + err
        )


async def create_task_task_relationship(task_id_1, task_id_2, producer):
    try:
        print("SENDING CREATEDAGDAG MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "activates",
            "from": {
                "ids": {"taskId": task_id_1},
                "labels": ["Airflow Task"],
                "op": "merge",
            },
            "to": {
                "ids": {"taskId": task_id_2},
                "labels": ["Airflow Task"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + task_id_1
            + " to "
            + task_id_2
            + " Error message: "
            + err
        )


async def create_spark_job_node(spark_job_id, producer):
    try:
        print("SENDING CREATESPARKJOB MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {
                "sparkJobId": spark_job_id,
            },
            "ids": {"sparkJobId": spark_job_id},
            "labels": ["Spark Job"],
            "type": "node",
            "detach": True,
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " + spark_job_id + " Error message: " + err)


async def create_task_spark_job_relationship(task_id, spark_job_id, producer):
    try:
        print("SENDING CREATETASKSPARKJOBRS MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "activates",
            "from": {
                "ids": {"taskId": task_id},
                "labels": ["Airflow Task"],
                "op": "merge",
            },
            "to": {
                "ids": {"sparkJobId": spark_job_id},
                "labels": ["Spark Job"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + task_id
            + " to "
            + spark_job_id
            + " Error message: "
            + err
        )


async def create_spark_job_spark_task_relationship(
    parent_spark_job_id, spark_task_id, producer
):
    try:
        print("SENDING CREATESPARKJOBSPARKTASKRS MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "parent_of",
            "from": {
                "ids": {"sparkJobId": parent_spark_job_id},
                "labels": ["Spark Job"],
                "op": "merge",
            },
            "to": {
                "ids": {"sparkTaskId": spark_task_id},
                "labels": ["Spark Task"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + parent_spark_job_id
            + " to "
            + spark_task_id
            + " Error message: "
            + err
        )


async def create_dataset_spark_task_relationship(dataset_id, spark_task_id, producer):
    try:
        print("SENDING CREATEDATASETTOSPARKTASKRS MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "used_in",
            "from": {
                "ids": {"datasetId": dataset_id},
                "labels": ["Dataset"],
                "op": "merge",
            },
            "to": {
                "ids": {"sparkTaskId": spark_task_id},
                "labels": ["Spark Task"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + dataset_id
            + " to "
            + spark_task_id
            + " Error message: "
            + err
        )


async def create_spark_task_dataset_relationship(spark_task_id, dataset_id, producer):
    try:
        print("SENDING CREATESPARKTASKDATASETRS MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "outputs_to",
            "from": {
                "ids": {"sparkTaskId": spark_task_id},
                "labels": ["Spark Task"],
                "op": "merge",
            },
            "to": {
                "ids": {"datasetId": dataset_id},
                "labels": ["Dataset"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + spark_task_id
            + " to "
            + dataset_id
            + " Error message: "
            + err
        )


async def create_dataset_task_relationship(dataset_id, task_id, producer):
    try:
        print("SENDING CREATEDATASETTOTASKRS MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "used_in",
            "from": {
                "ids": {"datasetId": dataset_id},
                "labels": ["Dataset"],
                "op": "merge",
            },
            "to": {
                "ids": {"taskId": task_id},
                "labels": ["Airflow Task"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + dataset_id
            + " to "
            + task_id
            + " Error message: "
            + err
        )


async def create_task_dataset_relationship(task_id, dataset_id, producer):
    try:
        print("SENDING CREATESPARKTASKDATASETRS MESSAGE TO Q")
        data = {
            "op": "merge",
            "properties": {},
            "rel_type": "outputs_to",
            "from": {
                "ids": {"taskId": task_id},
                "labels": ["Airflow Task"],
                "op": "merge",
            },
            "to": {
                "ids": {"datasetId": dataset_id},
                "labels": ["Dataset"],
                "op": "merge",
            },
            "type": "relationship",
        }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print(
            "Error when creating: "
            + task_id
            + " to "
            + dataset_id
            + " Error message: "
            + err
        )


def extract_job_name(input_string):
    parts = input_string.split(":")
    job_name = parts[-1]
    return job_name


def check_task_type(task_id):
    task = requests.get(marquez_backend + "namespaces/example/jobs/" + task_id).json()
    facets = task["latestRun"]["facets"]
    if "spark_version" in facets:
        return "Spark"
    elif "airflow_version" in facets:
        return "Airflow"
    else:
        return "Unresolved"


headers = {
    "Authorization": "Basic "
    + base64.b64encode(str.encode(f"{airflow_user}:{airflow_password}")).decode(
        "utf-8"
    ),
    "Content-type": "application/json",
}

default_args = {
    "owner": "dbz",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
    "provide_context": True,
}

with DAG(
    "lineage_creation",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args=default_args,
    description="DAG that generates lineage creation of all other DAGs in Neo4j.",
) as dag:

    def task_lineage(**context):
        try:
            setup_kafka_connect()
            producer = KafkaProducer(
                bootstrap_servers=[kafka_server],
                client_id="quickstart--shared-admin",
                value_serializer=lambda v: bytes(json.dumps(v).encode("utf-8")),
            )
            dictionary_task = {}
            response = requests.get(f"{airflow_backend}dags", headers=headers)
            data = response.json()
            for dag in data["dags"]:
                if not dag["dag_id"] == "lineage_creation":
                    key = dag["dag_id"]
                    dictionary_task[key] = False
            context["ti"].xcom_push(key="dictionary_task", value=dictionary_task)
            for key in dictionary_task:
                if dictionary_task[key] == False:
                    print("Go through function")
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(
                        lineage_creation_task(key, None, None, producer, **context)
                    )
                else:
                    print(f"skipped DAG {key}")
                dictionary_task = context["ti"].xcom_pull(key="dictionary_task")
                print(dictionary_task)
        except requests.exceptions.RequestException as err:
            print("Error occurred during DAGs retrieval:", err)

    def table_lineage(**context):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[kafka_server],
                client_id="quickstart--shared-admin",
                value_serializer=lambda v: bytes(json.dumps(v).encode("utf-8")),
            )
            dictionary_table = {}
            # Get all namespaces
            namespaces = requests.get(marquez_backend + "namespaces").json()
            for namespace in namespaces["namespaces"]:
                # Get all tasks
                try:
                    jobs = requests.get(
                        marquez_backend + "namespaces/" + namespace["name"] + "/jobs"
                    ).json()
                    for job in jobs["jobs"]:
                        node_id = "job:" + namespace["name"] + ":" + job["name"]
                        dictionary_table[node_id] = False
                except:
                    print("Can't reach " + namespace["name"])
                    continue

            context["ti"].xcom_push(key="dictionary_table", value=dictionary_table)
            for key in dictionary_table:
                if dictionary_table[key] == False:
                    print("Go through table creation function: " + key)
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(
                        lineage_creation_table(key, producer, **context)
                    )
                else:
                    print("Skipped node id: " + key)
                dictionary_table = context["ti"].xcom_pull(key="dictionary_table")
                print(dictionary_table)
        except:
            print("failed")

    async def lineage_creation_task(
        parent_dag_id, next_task_ids, wait_for_completion, producer, **context
    ):
        dictionary_task = context["ti"].xcom_pull(key="dictionary_task")
        dictionary_task[parent_dag_id] = True
        context["ti"].xcom_push(key="dictionary_task", value=dictionary_task)
        roots = []
        await create_dag_node(parent_dag_id, producer)
        dag = requests.get(
            airflow_backend + "dags/" + parent_dag_id + "/tasks",
            headers={
                "Authorization": "Basic "
                + base64.b64encode(
                    str.encode(f"{airflow_user}:{airflow_password}")
                ).decode("utf-8")
            },
        ).json()
        for task in dag["tasks"]:
            task["downstream_task_ids"] = [
                parent_dag_id + "." + x for x in task["downstream_task_ids"]
            ]
            await create_dag_task_relationship(parent_dag_id, task["task_id"], producer)
            if (
                wait_for_completion
                and len(task["downstream_task_ids"]) == 0
                and next_task_ids is not None
            ):
                for next_task_id in next_task_ids:
                    await create_task_task_relationship(
                        parent_dag_id + "." + task["task_id"], next_task_id, producer
                    )

        for task in dag["tasks"]:
            task_details = requests.get(
                marquez_backend
                + "namespaces/example/jobs/"
                + parent_dag_id
                + "."
                + task["task_id"]
            ).json()
            if (
                len(
                    task_details["latestRun"]["facets"]["airflow"]["task"][
                        "upstream_task_ids"
                    ]
                )
                == 2
            ):
                roots.append(parent_dag_id + "." + task["task_id"])
            # Handle TriggerDagRunOperator
            if task["operator_name"] == "TriggerDagRunOperator":
                wait_for_completion_downstream = task_details["latestRun"]["facets"][
                    "airflow"
                ]["task"]["args"]["wait_for_completion"]
                target_dag_id = task_details["latestRun"]["facets"]["airflow"]["task"][
                    "args"
                ]["trigger_dag_id"]
                await create_dag_dag_relationship(
                    parent_dag_id, target_dag_id, producer
                )
                if not wait_for_completion_downstream:
                    for downstream_task in task["downstream_task_ids"]:
                        await create_task_task_relationship(
                            parent_dag_id + "." + task["task_id"],
                            downstream_task,
                            producer,
                        )
                downstream_roots = await lineage_creation_task(
                    target_dag_id,
                    task["downstream_task_ids"],
                    wait_for_completion_downstream,
                    producer,
                    **context,
                )
                for root in downstream_roots:
                    # print(parent_dag_id + "." + task["task_id"] + " to " + root+ " created")
                    await create_task_task_relationship(
                        parent_dag_id + "." + task["task_id"], root, producer
                    )
            # Handle SparkSubmitOperator
            elif task["operator_name"] == "SparkSubmitOperator":
                spark_job_name = task_details["latestRun"]["facets"]["airflow"]["task"][
                    "_name"
                ]
                await create_spark_job_node(spark_job_name, producer)
                await create_task_spark_job_relationship(
                    parent_dag_id + "." + task["task_id"], spark_job_name, producer
                )
                spark_tasks = requests.get(
                    marquez_backend + "search?q=" + spark_job_name + ".%"
                ).json()
                for spark_task in spark_tasks["results"]:
                    await create_spark_job_spark_task_relationship(
                        spark_job_name, spark_task["name"], producer
                    )
                for downstream_task in task["downstream_task_ids"]:
                    await create_task_task_relationship(
                        parent_dag_id + "." + task["task_id"], downstream_task, producer
                    )
            else:
                for downstream_task in task["downstream_task_ids"]:
                    await create_task_task_relationship(
                        parent_dag_id + "." + task["task_id"], downstream_task, producer
                    )

        return roots

    async def lineage_creation_table(node_id, producer, **context):
        dictionary_table = context["ti"].xcom_pull(key="dictionary_table")
        dictionary_table[node_id] = True
        try:
            graph = requests.get(marquez_backend + "lineage?nodeId=" + node_id).json()[
                "graph"
            ]
            for node in graph:
                node_job_name = node["data"]["name"]
                if node["type"] == "DATASET":
                    for edge in node["inEdges"]:
                        origin = extract_job_name(edge["origin"])
                        node_type = check_task_type(origin)
                        if node_type == "Spark":
                            await create_spark_task_dataset_relationship(
                                origin, node_job_name, producer
                            )
                        if node_type == "Airflow":
                            await create_task_dataset_relationship(
                                origin, node_job_name, producer
                            )
                        else:
                            print("Unresolved")
                elif node["type"] == "JOB":
                    destination = extract_job_name(node_job_name)
                    node_type = check_task_type(destination)
                    dictionary_table[node["id"]] = True
                    print(node["id"] + " is now True")
                    for edge in node["inEdges"]:
                        origin = extract_job_name(edge["origin"])
                        if node_type == "Spark":
                            await create_dataset_spark_task_relationship(
                                origin, destination, producer
                            )
                        elif node_type == "Airflow":
                            await create_dataset_task_relationship(
                                origin, destination, producer
                            )
                        else:
                            print("Unresolved")
            context["ti"].xcom_push(key="dictionary_table", value=dictionary_table)

        except:
            print("Invalid node")

    task_lineage = PythonOperator(task_id="task_lineage", python_callable=task_lineage)
    table_lineage = PythonOperator(
        task_id="table_lineage", python_callable=table_lineage
    )

    task_lineage >> table_lineage
