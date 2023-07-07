import requests
import json
import asyncio
import base64
import helper_functions.kafka_helper_functions as kafka_helper
#Need to change backend when the script is executed by Airflow
marquez_backend = "http://localhost:5000/api/v1/"
airflow_backend = "http://localhost:8080/api/v1/"
airflow_user = "airflow"
airflow_password = "airflow"
dictionary = {}
headers =  {
  "Authorization": "Basic " + base64.b64encode(str.encode(f'{airflow_user}:{airflow_password}')).decode('utf-8'),
  "Content-type": "application/json"
}

kafka_helper.setup_kafka_connect()

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
    
async def lineage_creation_async_spark(parent_dag_id, next_task_ids, wait_for_completion):
    roots = []
    await kafka_helper.create_dag_node(parent_dag_id)
    dag = requests.get(airflow_backend + "dags/" + parent_dag_id + "/tasks", headers={"Authorization" : "Basic " + base64.b64encode(str.encode(f'{airflow_user}:{airflow_password}')).decode('utf-8')}).json()    
    for task in dag["tasks"]:
        task["downstream_task_ids"] = [parent_dag_id + "." + x for x in task["downstream_task_ids"]]
        await kafka_helper.create_dag_task_relationship(parent_dag_id, task["task_id"])
        if wait_for_completion and len(task["downstream_task_ids"]) == 0 and next_task_ids is not None:
            for next_task_id in next_task_ids:
                await kafka_helper.create_task_task_relationship(parent_dag_id + "." + task["task_id"], next_task_id)
        
    for task in dag["tasks"]:
        task_details = requests.get(marquez_backend + "namespaces/example/jobs/" + parent_dag_id + "." + task["task_id"]).json()
        if len(task_details["latestRun"]["facets"]["airflow"]["task"]["upstream_task_ids"]) == 2:
            roots.append(parent_dag_id + "." + task["task_id"])
        # Handle TriggerDagRunOperator
        if task["operator_name"] == "TriggerDagRunOperator":
            wait_for_completion_downstream = task_details["latestRun"]["facets"]["airflow"]["task"]["args"]["wait_for_completion"]
            target_dag_id = task_details["latestRun"]["facets"]["airflow"]["task"]["args"]["trigger_dag_id"]
            await kafka_helper.create_dag_dag_relationship(parent_dag_id, target_dag_id)
            if not wait_for_completion_downstream:
                for downstream_task in task["downstream_task_ids"]:
                    await kafka_helper.create_task_task_relationship(parent_dag_id + "." + task["task_id"], downstream_task)
            downstream_roots = await lineage_creation_async_spark(target_dag_id, task["downstream_task_ids"], wait_for_completion_downstream)
            for root in downstream_roots:
                # print(parent_dag_id + "." + task["task_id"] + " to " + root+ " created")
                await kafka_helper.create_task_task_relationship(parent_dag_id + "." + task["task_id"], root)
        # Handle SparkSubmitOperator
        elif task["operator_name"] == "SparkSubmitOperator":
            spark_job_name = task_details["latestRun"]["facets"]["airflow"]["task"]["_name"]
            await kafka_helper.create_spark_job_node(spark_job_name)
            await kafka_helper.create_task_spark_job_relationship(parent_dag_id + "." + task["task_id"], spark_job_name)
            spark_tasks = requests.get(marquez_backend + "search?q=" + spark_job_name + ".%").json()
            for spark_task in spark_tasks["results"]:
                await kafka_helper.create_spark_job_spark_task_relationship(spark_job_name, spark_task["name"])
            for downstream_task in task["downstream_task_ids"]:
                await kafka_helper.create_task_task_relationship(parent_dag_id + "." + task["task_id"], downstream_task)
        else:
            for downstream_task in task["downstream_task_ids"]:
                await kafka_helper.create_task_task_relationship(parent_dag_id + "." + task["task_id"], downstream_task)

    return roots

async def table_lineage_creation(node_id):
    graph = requests.get(marquez_backend + "lineage?nodeId=" + node_id).json()["graph"]
    for node in graph:
        node_job_name = node["data"]["name"]
        if node["type"] == "DATASET":
            for edge in node["inEdges"]:
                origin = extract_job_name(edge["origin"])
                node_type = check_task_type(origin)
                if node_type == "Spark":
                    await kafka_helper.create_spark_task_dataset_relationship(origin, node_job_name)
                if node_type == "Airflow":
                    await kafka_helper.create_task_dataset_relationship(origin, node_job_name)
                else:
                    print("Unresolved")
        elif node["type"] == "JOB":
            destination = extract_job_name(node_job_name)
            node_type = check_task_type(destination)
            for edge in node["inEdges"]:
                origin = extract_job_name(edge["origin"])
                if node_type == "Spark":
                    await kafka_helper.create_dataset_spark_task_relationship(origin, destination)
                elif node_type == "Airflow":
                    await kafka_helper.create_dataset_task_relationship(origin, destination)
                else:
                    print("Unresolved")
                
# Usage
asyncio.run(table_lineage_creation("job:example:test_four_layer.l2_task_2"))
# loop = asyncio.get_event_loop()
# roots = loop.run_until_complete(lineage_creation_async_spark("test_four_layer", None, None))
