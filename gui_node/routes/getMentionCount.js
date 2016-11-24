var express = require('express');
var router = express.Router();

var MongoClient = require('mongodb').MongoClient;
// Connection url
var url = 'mongodb://localhost:3000/db_sense'
/* GET users listing. */
router.get('/', function (req, res, next) {
        var resp_obj = {};
        var eventId = req.query.id;
        var startdt = req.query.startdt;
        var enddt = req.query.enddt;
        var all = req.query.all;
        try {
            MongoClient.connect(url, function (err, db) {
                // Create a collection we want to drop later
                var col = db.collection('cameo_mentions');
                var query = {'$and': [{'GLOBALEVENTID': parseInt(eventId)}]};
                all = parseInt(all);
                if (all != 1) {
                    query['$and'].push({'MentionTimeDate': {$lte: parseInt(enddt), $gte: parseInt(startdt)}});
                }
                col.find(query).count(function (err, count) {
                    if (err != null) {
                        db.close();
                        res.json({"status": 0, "message": "Could not retrieve Mentions count from database."});
                    } else {
                        console.log(count)
                        resp_obj.count = count;
                        resp_obj.status = 1;
                        resp_obj.message = "Successfully retrieved Mentions count from database."
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
