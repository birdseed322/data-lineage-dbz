const express = require('express');
const neo4j = require('neo4j-driver');

var driver = neo4j.driver(
  'bolt://localhost',
  neo4j.auth.basic('neo4j', 'neo4jneo4j')
);

var session = driver.session();

const app = express();
app.use(express.json());

app.get('/', function (req, res) {
  session.run('MATCH(n:Movie) RETURN n LIMIT 25')
    .then(function(result) {
      result.records.forEach(function(record){
        console.log(record._fields[0].properties);
      })
    })
    .catch(function(err){
      console.log(err);
    })
  res.send('Entry page');
})


app.post('/test', function(req, res){
  console.log("Received at /test endpoint");
  console.log(req.body);
  res.send('ok');
}) 

app.post('/lineage', function(req, res){
  console.log("Received at /lineage endpoint");
  console.log(req.body);
  res.send('ok');
}) 

app.post('/api/v1/lineage', function(req, res){
  console.log("Received at /api/v1/lineage endpoint");
  console.log(req.body);
  res.send('ok');
}) 

app.listen(3001, function() {
  console.log("Server is now running on port 3001");
});