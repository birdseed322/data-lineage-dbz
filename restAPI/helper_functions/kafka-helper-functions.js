const { Kafka } = require("kafkajs");
// KAFKA TEST
// Kafka broker connection details
const kafkaBrokers = ["localhost:9092"];
const kafkaTopic = "test";

// Create a new Kafka producer
const kafka = new Kafka({
  clientId: "quickstart--shared-admin",
  brokers: kafkaBrokers,
});
const producer = kafka.producer({ allowAutoTopicCreation: true });

/**
 * Function to launch Kafka Connect and establish connection with Neo4j
 */
function setupKafkaConnect() {
  fetch("http://localhost:8083/connectors", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({
      name: "Neo4jSinkConnectorJSONString",
      config: {
        topics: "test",
        "connector.class": "streams.kafka.connect.sink.Neo4jSinkConnector",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": false,
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": false,
        "errors.retry.timeout": "-1",
        "errors.retry.delay.max.ms": "1000",
        "errors.tolerance": "all",
        "errors.log.enable": true,
        "errors.log.include.messages": true,
        "neo4j.server.uri": "bolt://neo4jServer:7687",
        "neo4j.authentication.basic.username": "neo4j",
        "neo4j.authentication.basic.password": "password",
        "neo4j.topic.cud": "test",
      },
    }),
  })
    .then((res) => {
      console.log("Connector set!");
    })
    .catch((err) => {
      console.log("Error when setting connector: " + err);
    });
}

/**
 * Function to create Dag node on Neo4j
 * @param {String} dagId - The ID of the Dag to be created
 */
async function createDagNode(dagId) {
  producer
    .connect()
    .then(() => {
      console.log("SENDING CREATEDAG MESSAGE TO Q");
      producer.send({
        topic: "test",
        messages: [
          {
            value: JSON.stringify({
              op: "merge",
              properties: {
                dagId,
              },
              ids: { dagId },
              labels: ["Dag"],
              type: "node",
              detach: true,
            }),
          },
        ],
      });
    })
    .catch((err) => {
      console.log("Error when creating: " + dagId + " Error message: " + err);
    });
}

/**
 * Function to persist "is_parent_of" relationship between Dag and Task on Neo4j
 * @param {String} dagId - The ID of the parent Dag 
 * @param {String} taskId - The ID of the task belonging to the parent Dag
 */
async function createDagTaskRelationship(dagId, taskId) {
  producer
    .connect()
    .then(() => {
      console.log("SENDING CREATEDAGTASKRS MESSAGE TO Q");
      producer.send({
        topic: "test",
        messages: [
          {
            value: JSON.stringify({
              op: "merge",
              properties: {},
              rel_type: "is_parent_of",
              from: {
                ids: { dagId },
                labels: ["Dag"],
                op: "merge",
              },
              to: {
                ids: { taskId: dagId + "." + taskId },
                labels: ["Job"],
                op: "merge",
              },
              type: "relationship",
            }),
          },
        ],
      });
    })
    .catch((err) => {
      console.log(
        "Error when creating: " +
          dagId +
          " to " +
          taskId +
          " Error message: " +
          err
      );
    });
}

/**
 * Function to persist "activates" relationship between 2 Tasks on Neo4j to illustrate dependency
 * @param {String} taskId1 - The ID of the upstream Task
 * @param {String} taskId2 - The ID of the downstream Task
 */
async function createTaskTaskRelationship(taskId1, taskId2) {
  producer
    .connect()
    .then(() => {
      console.log("SENDING CREATETASKTASKRS MESSAGE TO Q");
      producer.send({
        topic: "test",
        messages: [
          {
            value: JSON.stringify({
              op: "merge",
              properties: {},
              rel_type: "activates",
              from: {
                ids: { taskId: taskId1 },
                labels: ["Job"],
                op: "merge",
              },
              to: {
                ids: { taskId: taskId2 },
                labels: ["Job"],
                op: "merge",
              },
              type: "relationship",
            }),
          },
        ],
      });
    })
    .catch((err) => {
      console.log(
        "Error when creating: " +
          taskId1 +
          " to " +
          taskId2 +
          " Error message: " +
          err
      );
    });
}

module.exports = {
  setupKafkaConnect,
  createDagNode,
  createDagTaskRelationship,
  createTaskTaskRelationship
};
