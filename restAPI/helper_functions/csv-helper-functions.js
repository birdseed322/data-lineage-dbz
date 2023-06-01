const createCsvWriter = require("csv-writer").createObjectCsvWriter;
var fs = require("fs");

function createCsvDirectory(parentDagId) {
  const csvDir = `../docker-compose-marquez/import/${parentDagId}_csv`;
  if (!fs.existsSync(csvDir)) {
    fs.mkdirSync(csvDir, { recursive: true });
  }
  return csvDir;
}

async function fetchAirflowDag(
  airflow_backend,
  airflow_user,
  airflow_password,
  parentDagId
) {
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

async function fetchMarquez(marquez_backend, parentDagId, task) {
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

function createCsvWriters(csvDir) {
  const dagNodesWriter = createCsvWriter({
    path: csvDir + "/dags.csv",
    header: [{ id: "dag_id", title: "DAG_ID" }],
  });

  const taskNodesWriter = createCsvWriter({
    path: csvDir + "/tasks.csv",
    header: [{ id: "task_id", title: "TASK_ID" }],
  });

  const dagTasksRelationsWriter = createCsvWriter({
    path: csvDir + "/dagTasksRelations.csv",
    header: [
      { id: "dag_id", title: "DAG_ID" },
      { id: "task_id", title: "TASK_ID" },
    ],
  });

  const interTasksRelationsWriter = createCsvWriter({
    path: csvDir + "/interTasksRelations.csv",
    header: [
      { id: "source_id", title: "SOURCE_ID" },
      { id: "dest_id", title: "DEST_ID" },
    ],
  });

  return [
    dagNodesWriter,
    taskNodesWriter,
    dagTasksRelationsWriter,
    interTasksRelationsWriter,
  ];
}

async function writeRecords(writerArr, dataArr) {
  for (i = 0; i < writerArr.length; i++) {
    await writerArr[i].writeRecords(dataArr[i]);
  }
}

async function importCSVs(parentDagId, session) {
  const queries = [
    `LOAD CSV WITH HEADERS FROM 'file:///${parentDagId}_csv/dags.csv' AS row
        MERGE (:Dag {dagId: row.DAG_ID})`,

    `LOAD CSV WITH HEADERS FROM 'file:///${parentDagId}_csv/tasks.csv' AS row
        MERGE (:Task {taskId: row.TASK_ID})`,

    `LOAD CSV WITH HEADERS FROM 'file:///${parentDagId}_csv/dagTasksRelations.csv' AS row
        MATCH (dag:Dag) WHERE dag.dagId = row.DAG_ID
        MATCH (task:Task) WHERE task.taskId = row.TASK_ID
        MERGE (dag)-[:Parent]->(task)`,

    `LOAD CSV WITH HEADERS FROM 'file:///${parentDagId}_csv/interTasksRelations.csv' AS row
        MATCH (source:Task) WHERE source.taskId = row.SOURCE_ID
        MATCH (dest:Task) WHERE dest.taskId = row.DEST_ID
        MERGE (source)-[:Activates]->(dest)`,
  ];

  for (query of queries) {
    await session.run(query).catch((err) => {
      console.log(err);
    });
  }
}

module.exports = {
  createCsvDirectory,
  fetchAirflowDag,
  fetchMarquez,
  createCsvWriters,
  writeRecords,
  importCSVs,
};
