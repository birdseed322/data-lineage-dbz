//Initialise const vars
const express = require("express");
const app = express();
const marquez_backend = "http://localhost:5000/api/v1/";
const airflow_backend = "http://localhost:8080/api/v1/";
const airflow_user = "airflow";
const airflow_password = "airflow";

//Dependencies
app.use(express.json());

function lineageCreation(parentDagId, nextTaskIds, waitForCompletion) {
  var roots = [];
  //Call tasks of new parent
  fetch(airflow_backend + "dags/" + parentDagId + "/tasks", {
    headers: {
      Authorization: "Basic " + btoa(airflow_user + ":" + airflow_password),
    },
  }).then((result) => {
    result.json().then((tasks) => {
      //loop through each task
      tasks.tasks.forEach((task) => {
        //Create all nodes
        //Call to marquez
        //Attain upstream (airflow.task.upstream_task_ids). If upstream empty push to root
        if (waitForCompletion && !task.downstream_task_ids[0]) {
          //Create link in neo4j with nextTaskIds
        }
      });

      tasks.tasks.forEach((task) => {
        if (task.operator_name == "TriggerDagRunOperator") {
          console.log(task);
          fetch(
            marquez_backend +
              "namespaces/example/jobs/" +
              parentDagId +
              "." +
              task.task_id
          ).then((marquez_task_result) => {
            marquez_task_result.json().then((marquez_task) => {
              const wait_for_completion =
                marquez_task.latestRun.facets.airflow.task.args
                  .wait_for_completion;
              const target_dag_id =
                marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
              const downstream_roots = lineageCreation(target_dag_id, task.downstream_task_ids, waitForCompletion);
              downstream_roots.forEach((root_task_id) => {
                //Create neo4j link with task.task_id (The TriggerDagRunOperator)
              })

            });
          });
        }
        // if (task.downstream_task_ids[0]) {
        //   //Create link to downstream node

        //   console.log(task.task_id + " has something");
        // } else {

        //   console.log("Theres nothing");
        // }
      });
    });
  });
}

async function checkNode(taskId) {
  //fetch to backend to check if taskId exists
}

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
