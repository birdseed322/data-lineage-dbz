from kafka import KafkaProducer
import requests
import json
import asyncio

kafka_connect_url = "http://localhost:8083/connectors"
kafka_server = "localhost:9092"
kafka_topic = "test"

producer = KafkaProducer(bootstrap_servers=[kafka_server], client_id="quickstart--shared-admin", value_serializer= lambda v: bytes(json.dumps(v).encode('utf-8')))
def setup_kafka_connect():
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
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
            "neo4j.topic.cud": "test"
        }
    }

    try:
        response = requests.post(kafka_connect_url, headers=headers, data=json.dumps(payload))
        print("Connector set!")
    except requests.exceptions.RequestException as err:
        print("Error when setting connector: " + str(err))

async def create_dag_node(dagId):
    try:
        print("SENDING CREATEDAG MESSAGE TO Q")
        producer.send(
            kafka_topic,
            value = {
                "op": "merge",
                "properties": {
                    "dagId": dagId,
                },
                "ids": {"dagId": dagId},
                "labels": ["Dag"],
                "type": "node",
                "detach": True,
                }
        )
        producer.flush()
    except Exception as err:
        print(f"Error when creating: {dagId} Error message: {err}")

async def create_dag_task_relationship(dag_id, task_id):
    try:
        print("SENDING CREATEDAGTASKRS MESSAGE TO Q")
        data = {
                "op": "merge",
                "properties": {},
                "rel_type": "is_parent_of",
                "from": {
                    "ids": {"dagId": dag_id},
                    "labels": ["Dag"],
                    "op": "merge"
                },
                "to": {
                    "ids": {"taskId": f"{dag_id}.{task_id}"},
                    "labels": ["Airflow Task"],
                    "op": "merge"
                },
                "type": "relationship"
                }
        producer.send(kafka_topic, value=data)
        producer.flush()
    except Exception as err:
        print(
        "Error when creating: " +
          dag_id +
          " to " +
          task_id +
          " Error message: " +
          err
      )

async def create_dag_dag_relationship(dag_id_1, dag_id_2):
    try:
        print("SENDING CREATEDAGDAG MESSAGE TO Q")
        data = {
              "op": "merge",
              "properties": {},
              "rel_type": "triggers",
              "from": {
                "ids": { "dagId": dag_id_1 },
                "labels": ["Dag"],
                "op": "merge",
              },
              "to": {
                "ids": { "dagId": dag_id_2 },
                "labels": ["Dag"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          dag_id_1 +
          " to " +
          dag_id_2 +
          " Error message: " +
          err)

async def create_task_task_relationship(task_id_1, task_id_2):
    try:
        print("SENDING CREATEDAGDAG MESSAGE TO Q")
        data = {
              "op": "merge",
              "properties": {},
              "rel_type": "activates",
              "from": {
                "ids": { "taskId": task_id_1 },
                "labels": ["Airflow Task"],
                "op": "merge",
              },
              "to": {
                "ids": { "taskId": task_id_2 },
                "labels": ["Airflow Task"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          task_id_1 +
          " to " +
          task_id_2 +
          " Error message: " +
          err)

async def create_spark_job_node(spark_job_id):
    try:
        print("SENDING CREATESPARKJOB MESSAGE TO Q")
        data ={
              "op": "merge",
              "properties": {
                "sparkJobId" : spark_job_id,
              },
              "ids": { "sparkJobId": spark_job_id },
              "labels": ["Spark Job"],
              "type": "node",
              "detach": True,
            }
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          spark_job_id +
          " Error message: " +
          err)

async def create_task_spark_job_relationship(task_id, spark_job_id):
    try:
        print("SENDING CREATETASKSPARKJOBRS MESSAGE TO Q")
        data = {
              "op": "merge",
              "properties": {},
              "rel_type": "activates",
              "from": {
                "ids": { "taskId": task_id },
                "labels": ["Airflow Task"],
                "op": "merge",
              },
              "to": {
                "ids": { "sparkJobId": spark_job_id },
                "labels": ["Spark Job"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          task_id +
          " to " +
          spark_job_id +
          " Error message: " +
          err)

async def create_spark_job_spark_task_relationship(parent_spark_job_id, spark_task_id):
    try:
        print("SENDING CREATESPARKJOBSPARKTASKRS MESSAGE TO Q")
        data ={
              "op": "merge",
              "properties": {},
              "rel_type": "parent_of",
              "from": {
                "ids": { "sparkJobId": parent_spark_job_id },
                "labels": ["Spark Job"],
                "op": "merge",
              },
              "to": {
                "ids": { "sparkTaskId": spark_task_id },
                "labels": ["Spark Task"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          parent_spark_job_id +
          " to " +
          spark_task_id + 
          " Error message: " +
          err)

async def create_dataset_spark_task_relationship(dataset_id, spark_task_id):
    try:
        print("SENDING CREATEDATASETTOSPARKTASKRS MESSAGE TO Q")
        data ={
              "op": "merge",
              "properties": {},
              "rel_type": "used_in",
              "from": {
                "ids": { "datasetId": dataset_id },
                "labels": ["Dataset"],
                "op": "merge",
              },
              "to": {
                "ids": { "sparkTaskId": spark_task_id },
                "labels": ["Spark Task"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          dataset_id +
          " to " +
          spark_task_id + 
          " Error message: " +
          err)

async def create_spark_task_dataset_relationship(spark_task_id, dataset_id):
    try:
        print("SENDING CREATESPARKTASKDATASETRS MESSAGE TO Q")
        data ={
              "op": "merge",
              "properties": {},
              "rel_type": "outputs_to",
               "from": {
                "ids": { "sparkTaskId": spark_task_id },
                "labels": ["Spark Task"],
                "op": "merge",
              },
              "to": {
                "ids": { "datasetId": dataset_id },
                "labels": ["Dataset"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          spark_task_id +
          " to " +
          dataset_id + 
          " Error message: " +
          err)

async def create_dataset_task_relationship(dataset_id, task_id):
    try:
        print("SENDING CREATEDATASETTOTASKRS MESSAGE TO Q")
        data ={
              "op": "merge",
              "properties": {},
              "rel_type": "used_in",
               "from": {
                "ids": { "datasetId": dataset_id },
                "labels": ["Dataset"],
                "op": "merge",
              },
              "to": {
                "ids": { "taskId": task_id },
                "labels": ["Airflow Task"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          dataset_id +
          " to " +
          task_id + 
          " Error message: " +
          err)

async def create_task_dataset_relationship(task_id, dataset_id):
    try:
        print("SENDING CREATESPARKTASKDATASETRS MESSAGE TO Q")
        data ={
              "op": "merge",
              "properties": {},
              "rel_type": "outputs_to",
               "from": {
                "ids": { "taskId": task_id },
                "labels": ["Airflow Task"],
                "op": "merge",
              },
              "to": {
                "ids": { "datasetId": dataset_id },
                "labels": ["Dataset"],
                "op": "merge",
              },
              "type": "relationship",
            } 
        producer.send(kafka_topic, value=data)
    except Exception as err:
        print("Error when creating: " +
          task_id +
          " to " +
          dataset_id + 
          " Error message: " +
          err)


async def main():
    dag = "test"  # Replace with the desired dagId
    await create_dag_node(dag)
# Run the main function within an event loop
loop = asyncio.get_event_loop()
loop.run_until_complete(main())

producer.close()
