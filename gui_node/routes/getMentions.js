var express = require('express');
var mongoose = require('mongoose');
var sleep = require('sleep');
var async = require('async');
var router = express.Router();

var MongoClient = require('mongodb').MongoClient;
// Connection url
var url = 'mongodb://localhost:3000/db_sense';

/* GET users listing. */
router.post('/', function(req, res, next) {
    var resp_obj = {};
    var params = req.body;
    var eventId = parseInt(req.query.id);
    var _outstanding = 3;
    var excp = false;
    try {

        MongoClient.connect(url, {server: {socketOptions: {connectTimeoutMS: 2000}}},  function(err, db) {
            var col = db.collection('cameo_mentions');
            var queries =  getQuery(params, eventId);
            function resultsCallback(err, type, data){
                if(err != null){
                    db.close();
                    resp_obj['draw'] = params.draw;
                    resp_obj['recordsTotal'] = 0;
                    resp_obj['recordsFiltered'] = 0;
                    resp_obj['data'] = [];
                    resp_obj['message'] = "";
                    resp_obj['error'] = "Error occurred while retrieving data.";
                    resp_obj['status'] = 0;
                    _outstanding = 0;
                    excp = true;
                    if (res.headersSent) {
                        return;
                    }
                    res.json(resp_obj);
                    return;
                }
                if(type === 0){
                    resp_obj['recordsTotal'] = data;
                    _outstanding -= 1;
                }else if(type === 1){
                    resp_obj['recordsFiltered'] = data;
                    _outstanding -= 1;
                }else if(type === 2){
                    resp_obj['data'] = data;
                    _outstanding -= 1;
                }

                if(_outstanding == 0){
                    if(!excp){
                        resp_obj['draw'] = params.draw;
                        resp_obj['message'] = "Successfully retrieved events from database.";
                        resp_obj['error'] = "";
                        resp_obj['status'] = 1;
                    }
                    if (res.headersSent) {
                        return;
                    }
                    res.json(resp_obj);
                }
            }
            function getTotalRecords(col, callback){
                col.count(function(err, count) {
                    //n_records =  count;
                    callback(err, 0, count);
                });
            }
            function getFilteredRecords(col, callback){
                col.find(queries[0]).count(function(err, count) {
                    //n_filtered =  count;
                    callback(err, 1, count);
                });
            }
            function getPaginatedItems(col, callback){
                col.find(queries[0],queries[1]).toArray(function(err, items) {
                    callback(err, 2, items);

                });
            }
            getTotalRecords(col, resultsCallback);
            getFilteredRecords(col, resultsCallback);
            getPaginatedItems(col, resultsCallback);
        });
    } catch (ex) {
        console.error(ex.toString());
        resp_obj['draw'] = params.draw;
        resp_obj['recordsTotal'] = 0;
        resp_obj['recordsFiltered'] = 0;
        resp_obj['data'] = [];
        resp_obj['message'] = "";
        resp_obj['error'] = "Exception occurred while retrieving data.";
        resp_obj['status'] = -1;
        res.json(resp_obj);
    }
});

function getQuery(params, eventId) {
    var options = {
        "limit": parseInt(params.length),
        "skip": parseInt(params.start),
        "sort": []
    }
    qFilter = {};
    var order_cnt;
    var date_added = false;
    for(order_i = 0; order_i< params.order.length; order_i++){
        ord_obj = params.order[order_i];
        col_obj = params.columns[parseInt(ord_obj.column)];
        options.sort.push([col_obj.name, ord_obj.dir]);
        if(col_obj.name.trim() === 'MentionTimeDate' ){date_added = true;}
    }
    if(!date_added){options.sort.push(['MentionTimeDate', 'desc'])}
    qFilter['$and'] = [{'GLOBALEVENTID':eventId}]
    var dtfil = {'MentionTimeDate' : {}}
    if(params.startDate.trim().toLowerCase() != ''){
        var std = parseInt(params.startDate);
        dtfil['MentionTimeDate']['$gte'] = std;
    }
    if(params.endDate.trim().toLowerCase() != ''){
        var ltd = parseInt(params.endDate);
        dtfil['MentionTimeDate']['$lte'] = ltd;
    }
    if(Object.keys(dtfil['MentionTimeDate']).length != 0){
        qFilter['$and'].push(dtfil);
    }

    atll = parseInt(params.atRange[0]);
    atul = parseInt(params.atRange[1]);

    qFilter['$and'].push({'MentionDocTone':{'$gte' : atll, '$lte' : atul}});


    if(params.search.value.trim().toLowerCase() != ''){

        qFilter['$and'].push({'$or': [
            {'MentionSourceName' : new RegExp('.*'+params.search.value.trim().toLowerCase() + '.*')}
        ]})
        //qFilter['$text'] = {'$search': params.search.value.trim(), '$caseSensitive': false}
    }
    //add other filters as well
    return [qFilter, options]
}


module.exports = router;
