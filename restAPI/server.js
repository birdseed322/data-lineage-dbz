//Initialise const vars
const express = require("express");
const app = express();
const marquez_backend = "http://localhost:5000/api/v1/";
const airflow_backend = "http://localhost:8080/api/v1/";
const airflow_user = "airflow";
const airflow_password = "airflow";
const neo4jUsername = "neo4j";
const neo4jPassword = "password"

var neo4j = require("neo4j-driver");
var driver = neo4j.driver("bolt://localhost", neo4j.auth.basic(neo4jUsername, neo4jPassword));

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

function runCypher(query, params) {
  var session = driver.session();     
  session
    .run(query, params)
    .catch(function(err) {
      console.log(err);
    }).finally(() => {
      session.close();
    });
}

//Second draft of algo. Async considered
function lineageCreation(parentDagId, nextTaskIds, waitForCompletion) {
  var roots = [];

  return fetch(airflow_backend + "dags/" + parentDagId + "/tasks", {
    headers: {
      Authorization: "Basic " + btoa(airflow_user + ":" + airflow_password),
    },
  })
    .then((result) => result.json())
    .then((dag) => {
      const fetchPromises = [];
      dag.tasks.forEach((task) => {   
        //Create nodes
        console.log(parentDagId + "." + task.task_id + " Created");
        runCypher('MERGE(n:Job {taskId: $taskIdParam})', {taskIdParam: task.task_id});
        
        if (
          waitForCompletion &&
          task.downstream_task_ids.length == 0 &&
          nextTaskIds != null
        ) {
          //Create link in neo4j with nextTaskIds (Might need to loop)
          nextTaskIds.forEach((nextTaskId) => {
            console.log(parentDagId + "." + task.task_id + " -> " + nextTaskId);
          });
        }
      });

      dag.tasks.forEach((task) => {
        //rename
        task.downstream_task_ids.forEach((x, index) => task.downstream_task_ids[index] = parentDagId + "." + x);
        const fetchPromise = fetch(
          marquez_backend +
            "namespaces/example/jobs/" + //"example" to be replaced
            parentDagId +
            "." +
            task.task_id
        )
          .then((marquez_task_result) => marquez_task_result.json())
          .then((marquez_task) => {
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
                task.downstream_task_ids.forEach((downStreamTask) => {
                  console.log(parentDagId + "." + task.task_id + " -> " + downStreamTask);
                });
              }
              const target_dag_id =
                marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
              console.log("Entering " + target_dag_id);
              fetchPromises.push(
                lineageCreation(
                  target_dag_id,
                  task.downstream_task_ids,
                  wait_for_completion
                ).then((downstream_roots) => {
                  downstream_roots.forEach((root_task_id) => {
                    console.log(parentDagId + "." + task.task_id + " -> " + root_task_id);
                  });
                })
              );
            } else {
              task.downstream_task_ids.forEach((downStreamTask) => {
                console.log(parentDagId + "." + task.task_id + " -> " + downStreamTask);
              });
            }
          });

        fetchPromises.push(fetchPromise);
      });

      console.log("Exiting " + parentDagId);
      return Promise.all(fetchPromises).then(() => roots);
    });
    
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
