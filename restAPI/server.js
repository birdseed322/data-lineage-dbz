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
  createDagDagRelationship,
  createTaskTaskRelationship,
  createSparkJobNode,
  createTaskSparkJobRelationship,
  createSparkJobSparkTaskRelationship,
  createDatasetToSparkTaskRelationship,
  createSparkTaskToDatasetRelationship,
  createDatasetToTaskRelationship,
  createTaskToDatasetRelationship,
} = require("./helper_functions/kafka-helper-functions");
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

//ExtractJobName from nodeId
function extractJobName(inputString) {
  const parts = inputString.split(":");
  const jobName = parts[parts.length - 1];
  return jobName;
}

/**
 * Function that will construct and persist the lineage of a given Dag on Neo4j through a Kafka queue
 * @param {String} parentDagId - The ID of the parentDag
 * @param {string[]} nextTaskIds - The IDs of the immediate downstream tasks of the TriggerDagOperator task
 * @param {Boolean} waitForCompletion - Boolean to indicate if the TriggerDagOperator waits for the completion of triggered Dags
 * @returns {string[]} - Array of Task IDs belonging to the roots of the current Dag
 */
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
          //Create link in with nextTaskIds
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
          .catch((err) => console.log("Marquez API call failed: " + err));
        fetchPromises.push(fetchPromise);
      });
      return Promise.all(fetchPromises).then(() => roots);
    })
    .catch((err) => console.log("Airflow API call failed"));
}

/**
 * Creates CSV files describing nodes and relaltionships which are directly imported to Neo4j.
 * @param {String} parentDagId - The ID of the parent DAG.
 * @param {string[]} nextParentTaskIds - Array of IDs of the next immediate tasks of a node that triggers a downstream DAG.
 * @returns {string[]} Array of root nodes in a DAG.
 */
async function lineageCreationCSV(parentDagId, nextParentTaskIds) {
  //Arrays that will be written to the CSV files.
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

/**
 * Function that will construct and persist the lineage of a given Dag and its called Spark tasks on Neo4j through a Kafka queue
 * @param {String} parentDagId - The ID of the parent DAG.
 * @param {string[]} nextTaskIds - The IDs of the immediate downstream tasks of the TriggerDagOperator task
 * @param {Boolean} waitForCompletion - Boolean to indicate if the TriggerDagOperator waits for the completion of triggered Dags
 * @returns {string[]} - Array of Task IDs belonging to the roots of the current Dag
 */
async function lineageCreationAsyncSpark(
  parentDagId,
  nextTaskIds,
  waitForCompletion
) {
  var roots = [];
  console.log("Now in " + parentDagId);
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
              const target_dag_id =
                marquez_task.latestRun.facets.airflow.task.args.trigger_dag_id;
              //Creates "trigger" relationship with downstream dag. Job level dependency
              await createDagDagRelationship(parentDagId, target_dag_id);
              console.log(
                "Parent = " + parentDagId + " Child = " + target_dag_id
              );
              if (!wait_for_completion) {
                task.downstream_task_ids.map(async (downStreamTask) => {
                  await createTaskTaskRelationship(
                    parentDagId + "." + task.task_id,
                    downStreamTask
                  );
                });
              }
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
            } else if (task.operator_name == "SparkSubmitOperator") {
              const spark_job_name =
                marquez_task.latestRun.facets.airflow.task._name;
              //May need to insert parser to convert name into spark name format (all lowercase and underscores)
              //Insert function to create spark job node here on Neo4j
              await createSparkJobNode(spark_job_name);
              await createTaskSparkJobRelationship(
                parentDagId + "." + task.task_id,
                spark_job_name
              );
              fetch(marquez_backend + "search?q=" + spark_job_name + ".%").then(
                (spark_tasks_res) => {
                  spark_tasks_res.json().then((spark_tasks) => {
                    //Algo does not consider multiple seperate lineage spawned from single spark job!
                    var a_spark_task_nodeId = spark_tasks.results[0].nodeId;
                    spark_tasks.results.forEach(async (spark_task) => {
                      //Create Node for spark_task
                      //Create association with spark_job_name node (parent)
                      await createSparkJobSparkTaskRelationship(
                        spark_job_name,
                        spark_task.name
                      );
                    });
                  });
                }
              );
              task.downstream_task_ids.map(async (downStreamTask) => {
                await createTaskTaskRelationship(
                  parentDagId + "." + task.task_id,
                  downStreamTask
                );
              });
            } else {
              task.downstream_task_ids.map(async (downStreamTask) => {
                await createTaskTaskRelationship(
                  parentDagId + "." + task.task_id,
                  downStreamTask
                );
              });
            }
          })
          .catch((err) => console.log("Marquez API call failed: " + err));
        fetchPromises.push(fetchPromise);
      });
      return Promise.all(fetchPromises).then(() => roots);
    })
    .catch((err) => console.log("Airflow API call failed"));
}

//Function that makes lineage out of ANY datasets. Need to traverse to check if destination / origin is Spark task or AF task? No real seperation between Dataset, AF and Spark node.
async function tableLineageCreation(nodeId) {
  fetch(`${marquez_backend}lineage?nodeId=${nodeId}`).then(async (result) => {
    result.json().then((json) => {
      json.graph.forEach(async (node) => {
        const nodeJobName = node.data.name;
        if (node.type == "DATASET") {
          node.inEdges.forEach(async (edge) => {
            const origin = extractJobName(edge.origin);
            const nodeType = await checkTaskType(origin);
            if (nodeType == "Spark") {
              await createSparkTaskToDatasetRelationship(origin, nodeJobName);
            } else if (nodeType == "Airflow") {
              await createTaskToDatasetRelationship(origin, nodeJobName);
            } else {
              console.log("Buggin");
            }
          });
        } else if (node.type == "JOB") {
          const destination = extractJobName(nodeJobName);
          const nodeType = await checkTaskType(destination);
          node.inEdges.forEach(async (edge) => {
            const origin = extractJobName(edge.origin);
            if (nodeType == "Spark") {
              await createDatasetToSparkTaskRelationship(origin, destination);
            } else if (nodeType == "Airflow") {
              await createDatasetToTaskRelationship(origin, destination);
            } else {
              console.log("Buggin");
            }
          });
        }
      });
    });
  });
}

async function lineageCreationAirflowTables(taskId) {
  fetch(`${marquez_backend}lineage?nodeId=job:example:${taskId}`).then(
    (result) => {
      result.json().then((json) => {
        json.graph.forEach((node) => {
          if (node.type == "DATASET") {
            node.inEdges.forEach(async (edge) => {
              await createTaskToDatasetRelationship(
                extractJobName(edge.origin),
                extractJobName(edge.destination)
              );
            });
          } else if (node.type == "JOB") {
            node.inEdges.forEach(async (edge) => {
              //Create RS between origin and destination
              await createDatasetToTaskRelationship(
                extractJobName(edge.origin),
                extractJobName(edge.destination)
              );
            });
          }
        });
      });
    }
  );
}

async function checkTaskType(taskId) {
  return await fetch(
    `${marquez_backend}namespaces/example/jobs/${taskId}`
  ).then((response) => {
    return response.json().then((task) => {
      if (task.latestRun.facets.spark_version) {
        return "Spark";
      } else if (task.latestRun.facets.airflow_version) {
        return "Airflow";
      } else {
        return "Unresolved";
      }
    });
  });
}

//END POINTS
app.get("/", function (req, res) {
  res.send("Landing Page");
});

app.get("/airflow/lineageasync/:dagId", function (req, res) {
  console.log("-------------NEW QUERY (ASYNC) -----------------------");
  lineageCreationAsync(req.params.dagId, null);
  res.send("Called lineageCreationAsync");
});

app.get("/airflow/lineagecsv/:dagId", function (req, res) {
  console.log("-------------NEW QUERY (CSV) -----------------------");
  try {
    lineageCreationCSV(req.params.dagId, null);
    res.send("Called lineageCreationCsv");
  } catch (err) {
    console.log(err);
  }
});

app.get("/airflow/lineageasyncspark/:dagId", function (req, res) {
  console.log("-------------NEW QUERY (ASYNC SPARK) -----------------------");
  lineageCreationAsyncSpark(req.params.dagId, null);
  res.send("Called lineageCreationAsyncSpark");
});

app.get("/tablelineage", function (req, res) {
  console.log(
    "-------------NEW QUERY (TABLE LINEAGE VIA NODEID) -----------------------"
  );
  tableLineageCreation(req.query.nodeId);
  res.send("Called tableLineageCreation");
});

app.listen(3001, function () {
  console.log("Server is now running on port 3001");
});
