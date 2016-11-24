var express = require('express');
var router = express.Router();
var fs = require('fs');

var MongoClient = require('mongodb').MongoClient;
// Connection url
var url = 'mongodb://localhost:3000/db_sense';
/* GET users listing. */
router.get('/', function(req, res, next) {
    resp_obj = {};
    try {
    if(isNaN(parseInt(req.query.sdt)) || isNaN(parseInt(req.query.edt))){

        MongoClient.connect(url, function (err, db) {
            // Create a collection we want to drop later
            var col = db.collection('coll_impact_map');
            // Show that duplicate records got dropped
            col.find({}).sort({'timestamp': -1}).toArray(function (err, items) {
                if (err != null) {
                    db.close();
                    res.json({"status": 0, "message": "Could not retrieve Global Impact Map data from database."});
                } else {
                    if (items.length != 0) {
                        resp_obj = items[0];
                        resp_obj.status = 1;
                        resp_obj.message = "Successfully retrieved Global Impact Map data from database."
                        db.close();
                        res.json(resp_obj)
                        return;
                    } else {
                        resp_obj = {};
                        resp_obj.status = -1;
                        resp_obj.message = "No data found for the selected date."
                        db.close();
                        res.json(resp_obj)
                        return;
                    }
                }
            });
        });
    }else{
        sdt = parseInt(req.query.sdt);
        edt = parseInt(req.query.edt);
        MongoClient.connect(url, function (err, db) {
            // Create a collection we want to drop later
            var col = db.collection('coll_impact_map');
            // Show that duplicate records got dropped
            col.find({'$or': [{'startDate': sdt}, {'endDate': edt}]}).toArray(function (err, items) {
                if (err != null) {
                    db.close();
                    res.json({"status": 0, "message": "Could not retrieve Global Impact Map data from database."});
                } else {
                    if (items.length != 0) {
                        resp_obj = items[0];
                        resp_obj.status = 1;
                        resp_obj.message = "Successfully retrieved Global Impact Map data from database."
                        db.close();
                        res.json(resp_obj)
                        return;
                    } else {
                        resp_obj = {};
                        resp_obj.status = -1;
                        resp_obj.message = "No data found for the selected date."
                        db.close();
                        res.json(resp_obj)
                        return;
                    }
                }
            });
        });
    }
    } catch(ex){
        console.error(ex.toString());
        res.json({"status": -1, "message": "Exception Occurred while extracting impact map data."});
        return;
    }
});
module.exports = router;
