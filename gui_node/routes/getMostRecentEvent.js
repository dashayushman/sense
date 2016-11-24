var express = require('express');
var router = express.Router();

var MongoClient = require('mongodb').MongoClient;
// Connection url
var url = 'mongodb://localhost:3000/db_sense'
/* GET users listing. */
router.get('/', function (req, res, next) {
        var resp_obj = {};

        try {
            MongoClient.connect(url, function (err, db) {
                // Create a collection we want to drop later
                var col = db.collection('cameo_events');

                col.find({},{"limit":1,"skip": 0,"sort": [['DATEADDED','desc']]}).toArray(function(err, items) {
                    if (err != null) {
                        db.close();
                        res.json({"status": 0, "message": "Could not retrieve Most recent event from database."});
                    } else {
                        resp_obj.eventid = items[0].GLOBALEVENTID;
                        resp_obj.status = 1;
                        resp_obj.message = "Successfully retrieved most recent event from database."
                        db.close();
                        res.json(resp_obj)
                        return;
                    }
                });
            });
        } catch (ex) {
            console.error(ex.toString());
            res.json({"status": -1, "message": "Exception occurred while retrieving data."});
            res.json(resp_obj);
        }
    }
    );

module.exports = router;
