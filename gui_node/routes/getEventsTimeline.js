var express = require('express');
var router = express.Router();

var MongoClient = require('mongodb').MongoClient;
// Connection url
var url = 'mongodb://localhost:3000/db_sense'
/* GET users listing. */
router.get('/', function(req, res, next) {
  var resp_obj = null;
  MongoClient.connect(url, function(err, db) {
    // Create a collection we want to drop later
    var col = db.collection('coll_event_timeline');
    // Show that duplicate records got dropped
    col.find({}).toArray(function(err, items) {
      if(err != null){
        db.close();
        res.json({"status":0,"message":"Could not retrieve Events timeline from database."});
      } else {
        resp_obj = {}
        resp_obj.data = items;
        resp_obj.status = 1;
        resp_obj.message = "Successfully retrieved Events timeline from database.";
        db.close();
        res.json(resp_obj);
      }
    });
  });
});

module.exports = router;
