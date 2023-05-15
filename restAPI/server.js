const express = require('express');
const app = express();
app.use(express.json())

app.get('/', function (req, res) {
  res.send('Hello World');
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

app.listen(3000, function() {
  console.log("Server is now running on port 3000");
});