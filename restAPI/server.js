//Initialise const vars
const {
  createCsvDirectory,
  fetchAirflowDag,
  fetchMarquez,
  createCsvWriters,
  writeRecords,
  importCSVs,
} = require("./helper_functions/csv-helper-functions");
const {
  setupKafkaConnect,
  createDagNode,
  createDagTaskRelationship,
  createTaskTaskRelationship
} = require("./helper_functions/kafka-helper-functions")
const express = require("express");
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
var session = driver.session();

var kafkaConnect = setupKafkaConnect();

//Dependencies
app.use(express.json());


//lineageCreation function implemented with Kafka for immediate graph creation
async function lineageCreationAsync(
  parentDagId,
  nextTaskIds,
  waitForCompletion
) {
  var roots = [];
  await createDagNode(parentDagId);
  return fetch(airflow_backend + "dags/" + parentDagId + "/tasks", {
    headers: {
      Authorization: "Basic " + btoa(airflow_user + ":" + airflow_password),
    },
  })
    .then((result) => {
      return result.json();
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
        await createDagTaskRelationship(parentDagId, task.task_id);

        if (
          waitForCompletion &&
          task.downstream_task_ids.length == 0 &&
          nextTaskIds != null
        ) {
          //Create link in neo4j with nextTaskIds
          nextTaskIds.map(async (nextTaskId) => {
            await createTaskTaskRelationship(
              parentDagId + "." + task.task_id,
              nextTaskId
            );
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
                  await createTaskTaskRelationship(
                    parentDagId + "." + task.task_id,
                    downStreamTask
                  );
                });
              }
              const target_dag_id =
                marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
              fetchPromises.push(
                await lineageCreationAsync(
                  target_dag_id,
                  task.downstream_task_ids,
                  wait_for_completion
                ).then((downstream_roots) => {
                  downstream_roots.map(async (root_task_id) => {
                    await createTaskTaskRelationship(
                      parentDagId + "." + task.task_id,
                      root_task_id
                    );
                  });
                })
              );
            } else {
              task.downstream_task_ids.map(async (downStreamTask) => {
                await createTaskTaskRelationship(
                  parentDagId + "." + task.task_id,
                  downStreamTask
                );
              });
            }
          })
          .catch(err => console.log("Marquez API call failed: " + err));
        fetchPromises.push(fetchPromise);
      });
      return Promise.all(fetchPromises).then(() => roots);
    })
    .catch(err => console.log("Airflow API call failed"));
}

//lineageCreation function implemented with CSV
async function lineageCreationCSV(parentDagId, nextParentTaskIds) {
  const rootsArr = [];
  const dagArr = [{ dag_id: parentDagId }];
  const tasksArr = [];
  const interTasksRelationsArr = [];
  const dagTasksRelationsArr = [];

  const csvDir = createCsvDirectory(parentDagId);
  const csvWriters = createCsvWriters(csvDir);

  //Fetch tasks of the DAG from Airflow API call
  const airflowDag = await fetchAirflowDag(
    airflow_backend,
    airflow_user,
    airflow_password,
    parentDagId
  );

  airflowDag.tasks.forEach((task) => {
    const taskName = `${parentDagId}.${task.task_id}`;
    //Compile tasks in the DAG to be written to jobs.csv
    tasksArr.push({ task_id: taskName });
    //Compile relations between tasks and the dag
    dagTasksRelationsArr.push({
      dag_id: parentDagId,
      task_id: taskName,
    });
    //Links relations from leaf nodes to nodes from master dag
    if (task.downstream_task_ids.length == 0 && nextParentTaskIds != null) {
      nextParentTaskIds.forEach((nextParentTaskId) => {
        interTasksRelationsArr.push({
          source_id: taskName,
          dest_id: nextParentTaskId,
        });
      });
    }
  });

  //Push root nodes of a DAG to an array to return
  await Promise.all(
    airflowDag.tasks.map(async (task) => {
      const taskName = `${parentDagId}.${task.task_id}`;
      const taskMarquez = await fetchMarquez(
        marquez_backend,
        parentDagId,
        task
      );
      try {
        const noUpstreamTask =
          marquez_task.latestRun.facets.airflow.task.upstream_task_ids.length ==
          2
            ? true
            : false;
        if (noUpstreamTask) {
          roots.push(taskName);
        }
      } catch (err) {
        console.log("Please run it on Airflow and Marquez first");
      }

      // Check whether it waits for triggered dag to complete
      if (task.operator_name == "TriggerDagRunOperator") {
        const wait_for_completion =
          taskMarquez.latestRun.facets.airflow.task.args.wait_for_completion;

        if (!wait_for_completion) {
          //Creates link to downstream tasks id
          task.downstream_task_ids.forEach((downStreamTaskId) => {
            interTasksRelationsArr.push({
              source_id: taskName,
              dest_id: `${parentDagId}.${downStreamTaskId}`,
            });
          });
        }

        const target_dag_id =
          taskMarquez.latestRun.facets.airflow.task.args.trigger_dag_id;

        //Link trigger task to root tasks of triggered dag
        await lineageCreationCSV(target_dag_id, task.downstream_task_ids).then(
          (childRootsIds) => {
            childRootsIds.forEach((childRootsId) => {
              interTasksRelationsArr.push({
                source_id: taskName,
                dest_id: childRootsId,
              });
            });
          }
        );
      } else {
        //Creates link to downstream tasks id
        task.downstream_task_ids.forEach((downStreamTaskId) => {
          interTasksRelationsArr.push({
            source_id: taskName,
            dest_id: `${parentDagId}.${downStreamTaskId}`,
          });
        });
      }
    })
  );

  await writeRecords(csvWriters, [
    dagArr,
    tasksArr,
    dagTasksRelationsArr,
    interTasksRelationsArr,
  ]);

  //If master DAG, import into Neo4j
  if (nextParentTaskIds == null) {
    await importCSVs(parentDagId, session);
  }
  return rootsArr;
}


app.get("/", function (req, res) {
  res.send("Landing Page");
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
  lineageCreationAsync(req.params.dagId, null);
  res.send("ok");
});

app.get("/airflow/lineagecsv/:dagId", function (req, res) {
  console.log("-------------NEW QUERY -----------------------");
  try {
    lineageCreationCSV(req.params.dagId, null);
    res.send("Called lineageCreationCsv");
  } catch (err) {
    console.log(err);
  }
});

app.listen(3001, function () {
  console.log("Server is now running on port 3001");
});
