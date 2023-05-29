//Initialise const vars
const express = require("express");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const app = express();
const marquez_backend = "http://localhost:5000/api/v1/";
const airflow_backend = "http://localhost:8080/api/v1/";
const airflow_user = "airflow";
const airflow_password = "airflow";
const neo4jUsername = "neo4j";
const neo4jPassword = "password";

var neo4j = require("neo4j-driver");
var driver = neo4j.driver(
  "bolt://localhost",
  neo4j.auth.basic(neo4jUsername, neo4jPassword)
);

//Dependencies
app.use(express.json());

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

const dagNodesWriter = createCsvWriter({
  path: "./csv/dags.csv",
  header: [{ id: "dag_id", title: "DAG_ID" }],
});
const taskNodesWriter = createCsvWriter({
  path: "./csv/tasks.csv",
  header: [{ id: "task_id", title: "TASK_ID" }],
});
const dagTasksRelationsWriter = createCsvWriter({
  path: "./csv/dagTasksRelations.csv",
  header: [
    { id: "dag_id", title: "DAG_ID" },
    { id: "task_id", title: "TASK_ID" },
  ],
});
const interTasksRelationsWriter = createCsvWriter({
  path: "./csv/interTaskRelations.csv",
  header: [
    { id: "source_id", title: "SOURCE_ID" },
    { id: "dest_id", title: "DEST_ID" },
  ],
});

async function fetchMarquez(parentDagId, task) {
  const result = await fetch(
    marquez_backend +
      //"example to be replaced"
      "namespaces/example/jobs/" +
      parentDagId +
      "." +
      task.task_id
  ).then((fetch) => fetch.json());
  return result;
}

async function fetchAirflowDag(parentDagId) {
  const result = await fetch(
    airflow_backend + "dags/" + parentDagId + "/tasks",
    {
      headers: {
        Authorization: "Basic " + btoa(airflow_user + ":" + airflow_password),
      },
    }
  ).then((result) => result.json());
  return result;
}
//lineageCreation function implemented with CSV
async function lineageCreation(parentDagId, nextParentTaskIds) {
  //root nodes of DAG to be returned
  const rootsArr = [];
  const dagArr = [{ dag_id: parentDagId }];
  const tasksArr = [];
  const interTasksRelationsArr = [];
  const dagTasksRelationsArr = [];

  //Fetch tasks of the DAG from Airflow API call
  const airflowDag = await fetchAirflowDag(parentDagId);
  airflowDag.tasks.forEach((task) => {
    //Compile tasks in the DAG to be written to jobs.csv
    tasksArr.push({ task_id: task.task_id });
    //Compile relations between tasks and the dag
    dagTasksRelationsArr.push({
      dag_id: parentDagId,
      task_id: task.task_id,
    });
    //Links relations from leaf nodes to nodes from master dag
    if (task.downstream_task_ids.length == 0 && nextParentTaskIds != null) {
      nextParentTaskIds.forEach((nextParentTaskId) => {
        interTasksRelationsArr.push({
          source_id: task.task_id,
          dest_id: nextParentTaskId,
        });
      });
    }
  });

  //Push root nodes of a DAG to an array to return
  await Promise.all(
    airflowDag.tasks.map(async (task) => {
      const taskMarquez = await fetchMarquez(parentDagId, task);
      console.log(taskMarquez.operator_name);
      if (
        taskMarquez.latestRun.facets.airflow.task.upstream_task_ids.length == 2
        //equivalent to "[]", an empty list
      ) {
        rootsArr.push(parentDagId + "." + task.task_id);
      }

      // Check whether it waits for triggered dag to complete
      if (task.operator_name == "TriggerDagRunOperator") {
        const wait_for_completion =
          taskMarquez.latestRun.facets.airflow.task.args.wait_for_completion;

        if (!wait_for_completion) {
          //Creates link to downstream tasks id
          task.downstream_task_ids.forEach((downStreamTaskId) => {
            interTasksRelationsArr.push({
              source_id: task.task_id,
              dest_id: downStreamTaskId,
            });
          });
        }

        const target_dag_id =
          taskMarquez.latestRun.facets.airflow.task.args.trigger_dag_id;

        //Link trigger task to root tasks of triggered dag
        await lineageCreation(target_dag_id, task.downstream_task_ids).then(
          (downstreamRootsIds) => {
            downstreamRootsIds.forEach((downstreamRootsId) => {
              interTasksRelationsArr.push({
                source_id: task.task_id,
                dest_id: downstreamRootsId,
              });
            });
          }
        );
      } else {
        //Creates link to downstream tasks id
        task.downstream_task_ids.forEach((downStreamTaskId) => {
          interTasksRelationsArr.push({
            source_id: task.task_id,
            dest_id: downStreamTaskId,
          });
        });
      }
    })
  );

  //Write DAG id to dags.csv
  await dagNodesWriter.writeRecords(dagArr);
  //Write all tasks in the DAG to tasks.csv
  await taskNodesWriter.writeRecords(tasksArr);
  //Write all relations between tasks and DAG to dagTasksRelations csv
  await dagTasksRelationsWriter.writeRecords(dagTasksRelationsArr);
  //Write relations between a task and its downstream tasks to interTasksRelations.csv
  await interTasksRelationsWriter.writeRecords(interTasksRelationsArr);

  if (nextParentTaskIds == null) {
    //If it is master DAG, import commands
  }
  return rootsArr;
}

// //Second draft of algo. Async considered
// function lineageCreation(parentDagId, nextTaskIds, waitForCompletion) {
//   var roots = [];
//   runCypher("MERGE(:Dag {dagId: $dagIdParam})", {
//     dagIdParam: parentDagId,
//   });
//   return fetch(airflow_backend + "dags/" + parentDagId + "/tasks", {
//     headers: {
//       Authorization: "Basic " + btoa(airflow_user + ":" + airflow_password),
//     },
//   })
//     .then((result) => result.json())
//     .then((dag) => {
//       const fetchPromises = [];
//       dag.tasks.forEach((task) => {
//         //rename
//         task.downstream_task_ids.forEach(
//           (x, index) =>
//             (task.downstream_task_ids[index] = parentDagId + "." + x)
//         );
//         //Create nodes
//         runCypher(
//           "MATCH (parentDag:Dag{dagId:$dagIdParam}) MERGE (parentDag)-[:is_parent_of]->(job:Job{taskId:$taskIdParam})",
//           {
//             taskIdParam: parentDagId + "." + task.task_id,
//             dagIdParam: parentDagId,
//           }
//         );
//         jobWriter.writeRecords([{task_id: task.task_id}])
//           .then(() => {
//             console.log("Wrote to job csv");
//           })
//         if (
//           waitForCompletion &&
//           task.downstream_task_ids.length == 0 &&
//           nextTaskIds != null
//         ) {
//           //Create link in neo4j with nextTaskIds
//           nextTaskIds.map((nextTaskId) => {
//             runCypher(
//               "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
//               {
//                 downstreamTaskIdParam: parentDagId + "." + task.task_id,
//                 nextTaskIdParam: nextTaskId,
//               }
//             );
//           });
//         }
//       });

//       dag.tasks.forEach((task) => {
//         const fetchPromise = fetch(
//           marquez_backend +
//             "namespaces/example/jobs/" + //"example" to be replaced
//             parentDagId +
//             "." +
//             task.task_id
//         )
//           .then((marquez_task_result) => marquez_task_result.json())
//           .then(async (marquez_task) => {
//             //Since Marquez API returns a string, 2 represents an empty list
//             if (
//               marquez_task.latestRun.facets.airflow.task.upstream_task_ids
//                 .length == 2
//             ) {
//               roots.push(parentDagId + "." + task.task_id);
//             }
//             const wait_for_completion =
//               marquez_task.latestRun.facets.airflow.task.args
//                 .wait_for_completion;

//             if (task.operator_name == "TriggerDagRunOperator") {
//               if (!wait_for_completion) {
//                 task.downstream_task_ids.map((downStreamTask) => {
//                   runCypher(
//                     "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
//                     {
//                       downstreamTaskIdParam: parentDagId + "." + task.task_id,
//                       nextTaskIdParam: downStreamTask,
//                     }
//                   );
//                 });
//               }
//               const target_dag_id =
//                 marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
//               fetchPromises.push(
//                 lineageCreation(
//                   target_dag_id,
//                   task.downstream_task_ids,
//                   wait_for_completion
//                 ).then((downstream_roots) => {
//                   downstream_roots.map((root_task_id) => {
//                     runCypher(
//                       "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
//                       {
//                         downstreamTaskIdParam: parentDagId + "." + task.task_id,
//                         nextTaskIdParam: root_task_id,
//                       }
//                     );
//                   });
//                 })
//               );
//             } else {
//               task.downstream_task_ids.map((downStreamTask) => {
//                 runCypher(
//                   "MATCH (downstream_task:Job{taskId:$downstreamTaskIdParam}), (next_task:Job{taskId:$nextTaskIdParam}) MERGE (downstream_task)-[:activates]->(next_task)",
//                   {
//                     downstreamTaskIdParam: parentDagId + "." + task.task_id,
//                     nextTaskIdParam: downStreamTask
//                   }
//                 );
//               });
//             }
//           });

//         fetchPromises.push(fetchPromise);
//       });
//       return Promise.all(fetchPromises).then(() => roots);
//     });
// }

app.get("/test", function (req, res) {
  createAirflowJob(); //not running, need a curl function?
  res.send("created airflow job");
});

// No longer needed, since all nodes will be created in the initial req for the parent dag
// async function checkNode(taskId) {
//   //fetch to backend to check if taskId exists
// };

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

// Old assumption. Replace Marquez as main consumer of Airflow Openlineage data
// app.post('/test', function(req, res){
//   console.log("Received at /test endpoint");
//   console.log(req.body);
//   res.send('ok');
// })

// app.post('/lineage', function(req, res){
//   console.log("Received at /lineage endpoint");
//   console.log(req.body);
//   res.send('ok');
// })

// app.post('/api/v1/lineage', function(req, res){
//   console.log("Received at /api/v1/lineage endpoint");
//   console.log(req.body);
//   res.send('ok');
// })
