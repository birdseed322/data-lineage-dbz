//Initialise const vars
const express = require("express");
const { Kafka } = require('kafkajs');
const app = express();
const marquez_backend = "http://localhost:5000/api/v1/";
const airflow_backend = "http://localhost:8080/api/v1/";
const airflow_user = "airflow";
const airflow_password = "airflow";
const neo4jUsername = "neo4j";
const neo4jPassword = "password";
var savedBookMarks = []

var neo4j = require("neo4j-driver");
var driver = neo4j.driver(
  "bolt://localhost",
  neo4j.auth.basic(neo4jUsername, neo4jPassword)
);

//Dependencies
app.use(express.json());

//KAFKA TEST
// Kafka broker connection details
const kafkaBrokers = ['localhost:9092'];
const kafkaTopic = 'test';
const setupKafkaConnect = fetch("http://localhost:8083/connectors", {
  method:"POST",
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  },
  body: JSON.stringify({
    "name": "Neo4jSinkConnectorJSONString",
    "config": {
      "topics": "test",
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
      "neo4j.topic.cud": "test"
    }
  }
  )
}).then((res) => {
  console.log("Connector set!");
}).catch((err) => {
  console.log("Error when setting connector: " + err);
})

// Create a new Kafka producer
const kafka = new Kafka({ clientId: 'quickstart--shared-admin',
  brokers: kafkaBrokers });
const producer = kafka.producer({allowAutoTopicCreation: true});


//-------------------------------------------------------------------------
//First draft of algo. Async not considered
// function lineageCreation(parentDagId, nextTaskIds, waitForCompletion) {
//   var roots = [];
//   //Call tasks of new parent
//   fetch(airflow_backend + "dags/" + parentDagId + "/tasks", {
//     headers: {
//       Authorization: "Basic " + btoa(airflow_user + ":" + airflow_password),
//     },
//   }).then((result) => {
//     //Query parent dag
//     result.json().then((dag) => {
//       //loop through each task
//       dag.tasks.forEach((task) => {
//         //Create all nodes
//         console.log(task.task_id + " Created");
//         //Call to marquez
//         // fetch(marquez_backend +
//         //   "namespaces/example/jobs/" + //"example" to be replaced
//         //   parentDagId +
//         //   "." +
//         //   task.task_id).then((marquez_task_result) => {
//         //     marquez_task_result.json().then((marquez_task) => {
//         //       if (marquez_task.latestRun.facets.airflow.task.upstream_task_ids.length == 2) {
//         //         console.log(task.task_id + " is a root")
//         //         roots.push(task.task_id);
//         //       }
//         //     })
//         //   })
//         //Attain upstream (airflow.task.upstream_task_ids). If upstream empty push to root
//         if (
//           waitForCompletion &&
//           !task.downstream_task_ids[0] &&
//           nextTaskIds != null
//         ) {
//           //Create link in neo4j with nextTaskIds (Might need to loop)
//           console.log(task.task_id + " -> " + nextTaskIds);
//         }
//       });

//       dag.tasks.forEach((task) => {
//         // to create downstream links
//         fetch(
//           marquez_backend +
//             "namespaces/example/jobs/" + //"example" to be replaced
//             parentDagId +
//             "." +
//             task.task_id
//         ).then((marquez_task_result) => {
//           marquez_task_result.json().then((marquez_task) => {
//             if (
//               marquez_task.latestRun.facets.airflow.task.upstream_task_ids
//                 .length == 2
//             ) {
//               console.log(task.task_id + " is a root");
//               roots.push(task.task_id);
//             }

//             if (task.operator_name == "TriggerDagRunOperator") {
//               const wait_for_completion =
//                 marquez_task.latestRun.facets.airflow.task.args
//                   .wait_for_completion;
//               const target_dag_id =
//                 marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
//               console.log("Entering " + target_dag_id);
//               var downstream_roots = lineageCreation(
//                 target_dag_id,
//                 task.downstream_task_ids,
//                 waitForCompletion
//               );
//               console.log("DOWNSTREAM ROOTS " + downstream_roots);
//               downstream_roots.then((downstream_roots_res) => {
//                 downstream_roots_res.forEach((root_task_id) => {
//                   //Create neo4j link with task.task_id (The TriggerDagRunOperator)
//                   console.log(task.task_id + " -> " + root_task_id);
//                 })
//                 return
//               })
//             } else {
//               task.downstream_task_ids.forEach((downStreamTask) => {
//                 //create link with downstream task
//                 console.log(task.task_id + " -> " + downStreamTask);
//               });
//             }
//           });
//         });

//         // if (task.operator_name == "TriggerDagRunOperator") {
//         //   fetch(
//         //     marquez_backend +
//         //       "namespaces/example/jobs/" + //"example" to be replaced
//         //       parentDagId +
//         //       "." +
//         //       task.task_id
//         //   ).then((marquez_task_result) => {
//         //     marquez_task_result.json().then((marquez_task) => {
//         //       const wait_for_completion =
//         //         marquez_task.latestRun.facets.airflow.task.args
//         //           .wait_for_completion;
//         //       const target_dag_id =
//         //         marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
//         //       console.log("Entering " + target_dag_id);
//         //       const downstream_roots = lineageCreation(
//         //         target_dag_id,
//         //         task.downstream_task_ids,
//         //         waitForCompletion
//         //       );
//         //       console.log("DOWNSTREAM ROOTS " + downstream_roots);
//         //       downstream_roots.forEach((root_task_id) => {
//         //         //Create neo4j link with task.task_id (The TriggerDagRunOperator)
//         //         console.log(task.task_id + " -> " + root_task_id);
//         //       });
//         //     });
//         //   });
//         // } else {
//         //   task.downstream_task_ids.forEach((downStreamTask) => {
//         //     //create link with downstream task
//         //     console.log(task.task_id + " -> " + downStreamTask);
//         //   });
//         // }
//       });
//       console.log("Exiting " + parentDagId);
//     });
//   });
//   console.log("-------------------------------------------------------");
//   return roots;
// }

// function runCypher(query, params) {
//   console.log(query + " FIRED OFF! PARAMS:" + JSON.stringify(params));
//   var session = driver.session();
//   var writeTrx = session.executeWrite(async trx => {
//     var res = await trx.run(query, params);
//     return res;
//   })

//   writeTrx.catch((err) => {console.log(err)}).then(() => {session.close()})
// }

async function runCypherV2(session, query, params) {
  await session.executeWrite(async trx => {
    await trx.run(query, params);
  })

}

const run = async () => {
  await producer.connect().then(() => {
    console.log("Connected to Kafka broker");
    producer.send({
      topic: 'test',
      messages: [{
        value: JSON.stringify({
          "op": "merge",
          "properties": {
            "foo": "value",
            "key": 1
          },
          "ids": {"key": 1, "otherKey":  "foo"},
          "labels": ["Foo","Bar"],
          "type": "node",
          "detach": true
        }),
    }]}
      )
  })

}

async function createDagNode(dagId) {
  producer.connect().then(() => {
    console.log("SENDING CREATEDAG MESSAGE TO Q");
    producer.send({
      topic: "test",
      messages: [{
        value : JSON.stringify({
          "op": "merge",
          "properties": {
            dagId,
          },
          "ids": {dagId},
          "labels": ["Dag"],
          "type": "node",
          "detach": true
        })
      }]
    })
  }).catch((err) => {
    console.log("Error when creating: " + dagId + " Error message: " + err);
  });
}

async function createDagTaskRelationship(dagId, taskId) {
  producer.connect().then(() => {
    console.log("SENDING CREATEDAGTASKRS MESSAGE TO Q");
    producer.send({
      topic: "test",
      messages: [{
        value : JSON.stringify({
          "op": "merge",
          "properties": {},
          "rel_type":"is_parent_of",
          "from": {
            "ids":{dagId},
            "labels":["Dag"],
            "op" : "merge"
          },
          "to":{
            "ids":{"taskId":dagId + "." + taskId},
            "labels":["Job"],
            "op":"merge"
          },
          "type": "relationship",
        })
      }]
    })
  }).catch((err) => {
    console.log("Error when creating: " + dagId + " to " + taskId + " Error message: " + err);
  });
}

async function createTaskTaskRelationship(taskId1, taskId2) {
  producer.connect().then(() => {
    console.log("SENDING CREATETASKTASKRS MESSAGE TO Q");
    producer.send({
      topic: "test",
      messages: [{
        value : JSON.stringify({
          "op": "merge",
          "properties": {},
          "rel_type":"activates",
          "from": {
            "ids":{"taskId":taskId1},
            "labels":["Job"],
            "op" : "merge"
          },
          "to":{
            "ids":{"taskId":taskId2},
            "labels":["Job"],
            "op":"merge"
          },
          "type": "relationship",
        })
      }]
    })
  }).catch((err) => {
    console.log("Error when creating: " + taskId1 + " to " + taskId2 + " Error message: " + err);
  });
}

//Second draft of algo. Async considered
async function lineageCreation(parentDagId, nextTaskIds, waitForCompletion) {
  var roots = [];
  // runCypher("MERGE(:Dag {dagId: $dagIdParam})", {
  //   dagIdParam: parentDagId,
  // });
  await createDagNode(parentDagId);   
  return fetch(airflow_backend + "dags/" + parentDagId + "/tasks", {
    headers: {
      Authorization: "Basic " + btoa(airflow_user + ":" + airflow_password),
    },
  })
    .then((result) => {
      return result.json()
    })
    .then((dag) => {
      const fetchPromises = [];
      dag.tasks.forEach(async (task) => {
        //rename
        task.downstream_task_ids.forEach(
          (x, index) =>
            (task.downstream_task_ids[index] = parentDagId + "." + x)
        );
        //Create nodes
        // runCypher(
        //   "MATCH (parentDag:Dag{dagId:$dagIdParam}) MERGE (parentDag)-[:is_parent_of]->(job:Job{taskId:$taskIdParam})",
        //   {
        //     taskIdParam: parentDagId + "." + task.task_id,
        //     dagIdParam: parentDagId,
        //   }
        // );
        await createDagTaskRelationship(parentDagId, task.task_id);

        if (
          waitForCompletion &&
          task.downstream_task_ids.length == 0 &&
          nextTaskIds != null
        ) {
          //Create link in neo4j with nextTaskIds
          nextTaskIds.map(async (nextTaskId) => {
            // runCypher(
            //   "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
            //   {
            //     downstreamTaskIdParam: parentDagId + "." + task.task_id,
            //     nextTaskIdParam: nextTaskId,
            //   }
            // );
            await createTaskTaskRelationship(parentDagId + "." + task.task_id, nextTaskId)
          });
        }
      });

      dag.tasks.forEach(async (task) => {
        const fetchPromise = fetch(
          marquez_backend +
            "namespaces/example/jobs/" + //"example" to be replaced
            parentDagId +
            "." +
            task.task_id
        )
          .then((marquez_task_result) => marquez_task_result.json())
          .then(async (marquez_task) => {
            //Since Marquez API returns a string, 2 represents an empty list
            if (
              marquez_task.latestRun.facets.airflow.task.upstream_task_ids
                .length == 2
            ) {
              roots.push(parentDagId + "." + task.task_id);
            }
            const wait_for_completion =
              marquez_task.latestRun.facets.airflow.task.args
                .wait_for_completion;

            if (task.operator_name == "TriggerDagRunOperator") {
              if (!wait_for_completion) {
                task.downstream_task_ids.map(async (downStreamTask) => {
                  // runCypher(
                  //   "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
                  //   {
                  //     downstreamTaskIdParam: parentDagId + "." + task.task_id,
                  //     nextTaskIdParam: downStreamTask,
                  //   }
                  // );
                  await createTaskTaskRelationship(parentDagId + "." + task.task_id, downStreamTask)
                });
              }
              const target_dag_id =
                marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
              fetchPromises.push(
                await lineageCreation(
                  target_dag_id,
                  task.downstream_task_ids,
                  wait_for_completion
                ).then((downstream_roots) => {
                  downstream_roots.map(async (root_task_id) => {
                    // runCypher(
                    //   "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
                    //   {
                    //     downstreamTaskIdParam: parentDagId + "." + task.task_id,
                    //     nextTaskIdParam: root_task_id,
                    //   }
                    // );
                    await createTaskTaskRelationship(parentDagId + "." + task.task_id, root_task_id);
                  });
                })
              );
            } else {
              task.downstream_task_ids.map(async (downStreamTask) => {
                // runCypher(
                //   "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
                //   {
                //     downstreamTaskIdParam: parentDagId + "." + task.task_id,
                //     nextTaskIdParam: downStreamTask
                //   }
                // );
                await createTaskTaskRelationship(parentDagId + "." + task.task_id, downStreamTask)
              });
            }
          });

        await fetchPromises.push(fetchPromise);
      });
      return Promise.all(fetchPromises).then(() => roots);
    })
    .catch((err) => {console.log("ERROR: " + err + " PARAMS: " + parentDagId)});
}





function createAirflowJob() {
  app.post("/test", function (req, res) {
    console.log("post request working");
  });
}

app.get("/test", function (req, res) {
  createAirflowJob(); //not running, need a curl function?
  res.send("created airflow job");
});

app.post("/test", function (req, res) {
  console.log("post request working");
});

app.get("/", function (req, res) {
  res.send("Hello World");
});

//Traffic redirected to leverage Marquez lineage tech
app.get("/airflow/lineage", function (req, res) {
  console.log(req.query.nodeId);
  fetch(marquez_backend + "lineage?nodeId=" + req.query.nodeId).then(
    (result) => {
      result.json().then((lin) => {
        console.log(lin);
      });
    }
  );
});

app.get("/airflow/lineage/:dagId", function (req, res) {
  console.log("-------------NEW QUERY -----------------------");
  lineageCreation(req.params.dagId, null);
  res.send("ok");
});

app.listen(3001, function () {
  console.log("Server is now running on port 3001");
});
