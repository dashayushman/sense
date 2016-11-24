var express = require('express');
var mongoose = require('mongoose');
var sleep = require('sleep');
var async = require('async')
var moment = require('moment');
var router = express.Router();

var MongoClient = require('mongodb').MongoClient;
// Connection url
var url = 'mongodb://localhost:3000/db_sense'

/* GET users listing. */
router.get('/', function (req, res, next) {
    var resp_obj = {};
    var _outstanding = 7;
    var excp = false;
    var eventId = req.query.id;

    var countries = null;
    var eventCodes = null;
    var ethnicity = null;
    var religion = null;
    var knownGroup = null;
    var cameo_type = null;
    var event = null;


    if (eventId == null || eventId == '') {
        resp_obj['data'] = {};
        resp_obj['message'] = "Please provide an Event id to get data.";
        resp_obj['status'] = 0;
        res.json(resp_obj);
        return;
    }
    eventId = parseInt(req.query.id);
    try {
        MongoClient.connect(url, {server: {socketOptions: {connectTimeoutMS: 2000}}}, function (err, db) {
            var col_cameo_events = db.collection('cameo_events');
            var col_cameo_event_codes = db.collection('cameo_eventcodes');
            var col_cameo_countries = db.collection('fips_country');
            var col_cameo_ethinicity = db.collection('cameo_ethnic');
            var col_cameo_known_group = db.collection('cameo_knowngroup');
            var col_cameo_relegion = db.collection('cameo_religion');
            var col_cameo_type = db.collection('cameo_type');
            //var queries =  getQuery(params);
            function resultsCallback(err, type, data) {
                if (err != null) {
                    db.close();
                    resp_obj['data'] = {};
                    resp_obj['message'] = "Error occurred while retrieving data.";
                    resp_obj['status'] = -2;
                    res.json(resp_obj);
                    return;
                }
                if (type === 0) {
                    eventCodes = data;
                    _outstanding -= 1;
                } else if (type === 1) {
                    countries = data;
                    _outstanding -= 1;
                }  else if (type === 2) {
                    event = data;
                    _outstanding -= 1;
                } else if (type === 3) {
                    ethnicity = data;
                    _outstanding -= 1;
                } else if (type === 4) {
                    knownGroup = data;
                    _outstanding -= 1;
                } else if (type === 5) {
                    religion = data;
                    _outstanding -= 1;
                } else if (type === 6) {
                    cameo_type = data;
                    _outstanding -= 1;
                }

                if (_outstanding == 0) {
                    if(event == null){
                        resp_obj['data'] = {};
                        resp_obj['message'] = "Event could not be found.";
                        resp_obj['status'] = -2;
                        res.json(resp_obj);
                        return;
                    }
                    event.Actor1Geo_Type_Name = '';
                    event.Actor2Geo_Type_Name = '';
                    event.ActionGeo_Type_Name = '';

                    if(event.Actor1Geo_Type != null){
                        event.Actor1Geo_Type_Name = getGeotypeFromCode(event.Actor1Geo_Type);
                    }
                    if(event.Actor2Geo_Type != null){
                        event.Actor2Geo_Type_Name = getGeotypeFromCode(event.Actor2Geo_Type);
                    }
                    if(event.ActionGeo_Type != null){
                        event.ActionGeo_Type_Name = getGeotypeFromCode(event.ActionGeo_Type);
                    }

                    //.......................................................................................

                    event.Actor1Geo_CountryCode_name = '';
                    event.Actor2Geo_CountryCode_name = '';
                    event.ActionGeo_CountryCode_name = '';

                    if(event.Actor1Geo_CountryCode != null){
                        event.ActionGeo_Type_Name = getGeoNameFromFipsType(event.Actor1Geo_CountryCode, countries);
                    }
                    if(event.Actor2Geo_CountryCode != null){
                        event.ActionGeo_Type_Name = getGeoNameFromFipsType(event.Actor2Geo_CountryCode, countries);
                    }
                    if(event.ActionGeo_CountryCode != null){
                        event.ActionGeo_Type_Name = getGeoNameFromFipsType(event.ActionGeo_CountryCode, countries);
                    }

                    //.......................................................................................

                    event.Actor1KnownGroupCode_Name = '';
                    event.Actor2KnownGroupCode_Name = '';

                    if(event.Actor1Geo_CountryCode != null){
                        event.Actor1KnownGroupCode_Name = getKnownGroupFromCode(event.Actor1KnownGroupCode, knownGroup);
                    }
                    if(event.Actor2Geo_CountryCode != null){
                        event.Actor2KnownGroupCode_Name = getKnownGroupFromCode(event.Actor2KnownGroupCode, knownGroup);
                    }

                    //.......................................................................................

                    event.Actor1EthnicCode_Name = '';
                    event.Actor2EthnicCode_Name = '';

                    if(event.Actor1EthnicCode != null){
                        event.Actor1EthnicCode_Name = getEthnicityFromCode(event.Actor1EthnicCode, ethnicity);
                    }
                    if(event.Actor2EthnicCode != null){
                        event.Actor2EthnicCode_Name = getEthnicityFromCode(event.Actor2EthnicCode, ethnicity);
                    }

                    //.......................................................................................

                    event.Actor1Religion1Code_Name = '';
                    event.Actor2Religion1Code_Name = '';

                    if(event.Actor1Religion1Code != null){
                        event.Actor1Religion1Code_Name = getReligionFromCode(event.Actor1Religion1Code, religion);
                    }
                    if(event.Actor2Religion1Code != null){
                        event.Actor2Religion1Code_Name = getReligionFromCode(event.Actor2Religion1Code, religion);
                    }

                    //.......................................................................................

                    event.Actor1Type1Code_Name = '';
                    event.Actor2Type1Code_Name = '';

                    if(event.Actor1Type1Code != null){
                        event.Actor1Type1Code_Name = getTypeFromCode(event.Actor1Type1Code, cameo_type);
                    }
                    if(event.Actor2Type1Code != null){
                        event.Actor2Type1Code_Name = getTypeFromCode(event.Actor2Type1Code, cameo_type);
                    }

                    //.......................................................................................

                    event.QuadClass_Name = '';
                    if(event.QuadClass != null){
                        event.QuadClass_Name = getEventClassFromCode(event.QuadClass);
                    }


                    event.EventRootCode_Name = '';
                    if(event.EventRootCode != null){
                        event.EventRootCode_Name = getEvenFromCode(event.EventRootCode, eventCodes);
                    }

                    final_event = prepareTableData(event);

                    resp_obj['data'] = final_event;
                    resp_obj['message'] = "Successfully retrieved events from database.";
                    resp_obj['error'] = "";
                    resp_obj['status'] = 1;
                    res.json(resp_obj);
                }
            }


            function getEventCodes(col_cameo_event_codes, callback) {
                col_cameo_event_codes.find({}).toArray(function (err, items) {
                    callback(err, 0, items);
                });
            }

            function getCountries(col_cameo_countries, callback) {
                col_cameo_countries.find({}).toArray(function (err, items) {
                    callback(err, 1, items);
                });
            }

            function getEvent(col_cameo_events, eventId, callback) {
                col_cameo_events.findOne({GLOBALEVENTID: eventId},function (err, items) {
                    callback(err, 2, items);
                });
            }
            function getEthnicity(col_cameo_ethinicity, callback) {
                col_cameo_ethinicity.find({}).toArray(function (err, items) {
                    callback(err, 3, items);
                });
            }
            function getKnownGroup(col_cameo_known_group, callback) {
                col_cameo_known_group.find({}).toArray(function (err, items) {
                    callback(err, 4, items);
                });
            }
            function getRelegion(col_cameo_relegion, callback) {
                col_cameo_relegion.find({}).toArray(function (err, items) {
                    callback(err, 5, items);
                });
            }
            function getType(col_cameo_type, callback) {
                col_cameo_type.find({}).toArray(function (err, items) {
                    callback(err, 6, items);
                });
            }

            getEventCodes(col_cameo_event_codes, resultsCallback);
            getCountries(col_cameo_countries, resultsCallback);

            getEvent(col_cameo_events, eventId, resultsCallback);
            getEthnicity(col_cameo_ethinicity, resultsCallback);
            getKnownGroup(col_cameo_known_group, resultsCallback);
            getRelegion(col_cameo_relegion, resultsCallback);
            getType(col_cameo_type, resultsCallback);
        });
    } catch (ex) {
        console.error(ex.toString());

        resp_obj['data'] = {};
        resp_obj['message'] = "Exception occurred while retrieving data.";
        resp_obj['status'] = -1;
        res.json(resp_obj);
    }
});

function prepareTableData(event_buf){
    event_buf.act1Table = [];
    event_buf.act2Table = [];
    event_buf.actionTable = [];
    event_buf.eventTable = [];

    event_buf.act1Table.push(['Name' , event_buf.Actor1Name]);
    event_buf.act1Table.push(['Geography Type' , event_buf.Actor1Geo_Type_Name]);
    event_buf.act1Table.push(['Geography Name' , event_buf.Actor1Geo_CountryCode_name]);
    event_buf.act1Table.push(['Known Group' , event_buf.Actor1KnownGroupCode_Name]);
    event_buf.act1Table.push(['Ethenicity' , event_buf.Actor1EthnicCode_Name]);
    event_buf.act1Table.push(['Relegion' , event_buf.Actor1Religion1Code_Name]);
    event_buf.act1Table.push(['Type/Role' , event_buf.Actor1Type1Code_Name]);
    event_buf.act1Table.push(['Full Geographical Name' , event_buf.Actor1Geo_FullName]);
    event_buf.act1Table.push(['Lat, Long' , event_buf.Actor1Geo_Lat.toString() + ' , ' + event_buf.Actor1Geo_Long.toString()]);

    event_buf.act2Table.push(['Name' , event_buf.Actor2Name]);
    event_buf.act2Table.push(['Geography Type' , event_buf.Actor2Geo_Type_Name]);
    event_buf.act2Table.push(['Geography Name' , event_buf.Actor2Geo_CountryCode_name]);
    event_buf.act2Table.push(['Known Group' , event_buf.Actor2KnownGroupCode_Name]);
    event_buf.act2Table.push(['Ethenicity' , event_buf.Actor2EthnicCode_Name]);
    event_buf.act2Table.push(['Relegion' , event_buf.Actor2Religion1Code_Name]);
    event_buf.act2Table.push(['Type/Role' , event_buf.Actor2Type1Code_Name]);
    event_buf.act2Table.push(['Full Geographical Name' , event_buf.Actor2Geo_FullName]);
    event_buf.act2Table.push(['Lat, Long' , event_buf.Actor2Geo_Lat.toString() + ' , ' + event_buf.Actor2Geo_Long.toString()]);

    event_buf.actionTable.push(['Full Geographical Name' , event_buf.ActionGeo_FullName]);
    event_buf.actionTable.push(['Geography Type' , event_buf.ActionGeo_Type_Name]);
    event_buf.actionTable.push(['Geography Name' , event_buf.ActionGeo_CountryCode_name]);
    event_buf.actionTable.push(['Lat, Long' , event_buf.ActionGeo_Lat.toString() + ' , ' + event_buf.ActionGeo_Long.toString()]);

    event_buf.eventTable.push(['ID' , event_buf.GLOBALEVENTID]);
    event_buf.eventTable.push(['Date-Time' , moment(event_buf.DATEADDED.toString(), "YYYYMMDDHHmmss").format('LLL')]);
    event_buf.eventTable.push(['Event Type' , event_buf.EventRootCode_Name]);
    event_buf.eventTable.push(['Event Class' , event_buf.QuadClass_Name]);
    event_buf.eventTable.push(['Number of Articles' , event_buf.NumArticles]);
    event_buf.eventTable.push(['Source URL' , '<a href="' + event_buf.SOURCEURL + '" class="btn btn-primary btn-sm btn-small" target="_blank">Click To View Source</a>' ]);

    return event_buf;

}

function getEvenFromCode(code, eth){
    name = '';
    for(var i = 0; i<eth.length; i++){
        if(code == eth[i].CAMEOEVENTCODE){
            return eth[i].EVENTDESCRIPTION;
        }
    }
    return name;
}

function getTypeFromCode(code, eth){
    name = '';
    for(var i = 0; i<eth.length; i++){
        if(code == eth[i].CODE){
            return eth[i].LABEL;
        }
    }
    return name;
}

function getReligionFromCode(code, eth){
    name = '';
    for(var i = 0; i<eth.length; i++){
        if(code == eth[i].CODE){
            return eth[i].LABEL;
        }
    }
    return name;
}

function getEthnicityFromCode(code, eth){
    name = '';
    for(var i = 0; i<eth.length; i++){
        if(code == eth[i].CODE){
            return eth[i].LABEL;
        }
    }
    return name;
}

function getKnownGroupFromCode(code, kg){
    name = '';
    for(var i = 0; i<kg.length; i++){
        if(code == kg[i].CODE){
            return kg[i].LABEL;
        }
    }
    return name;
}

function getGeoNameFromFipsType(code, countries){
    name = '';
    for(var i = 0; i<countries.length; i++){
        if(code == countries[i].CountryCode){
            return countries[i].CountryName;
        }
    }
    return name;
}

function getGeotypeFromCode(code){
    switch(code) {
        case 1:
            return 'Country';
        case 2:
            return 'US State';
            break;
        case 3:
            return 'US City';
            break;
        case 4:
            return 'World City';
            break;
        case 5:
            return 'World STate';
            break;
        default:
            return '';

    }
}

function getEventClassFromCode(code){
    switch(code) {
        case 1:
            return 'Verbal Cooperation';
        case 2:
            return 'Material Cooperation';
            break;
        case 3:
            return 'Verbal Conflict';
            break;
        case 4:
            return 'Material Conflict';
            break;
        default:
            return '';

    }
}

function getQuery(params) {
    var options = {
        "limit": parseInt(params.length),
        "skip": parseInt(params.start),
        "sort": []
    }
    qFilter = {};
    var order_cnt;
    var date_added = false;
    for (order_i = 0; order_i < params.order.length; order_i++) {
        ord_obj = params.order[order_i];
        col_obj = params.columns[parseInt(ord_obj.column)];
        options.sort.push([col_obj.name, ord_obj.dir]);
        if (col_obj.name.trim() === 'DATEADDED') {
            date_added = true;
        }
    }
    if (!date_added) {
        options.sort.push(['DATEADDED', 'desc'])
    }
    qFilter['$and'] = [
        {'Actor1Name': {'$ne': ''}},
        {'Actor2Name': {'$ne': ''}},
        {'Actor1Geo_FullName': {'$ne': ''}},
        {'Actor2Geo_FullName': {'$ne': ''}},
        {'ActionGeo_FullName': {'$ne': ''}},
    ]
    var dtfil = {'DATEADDED': {}}
    if (params.startDate.trim().toLowerCase() != '') {
        var std = parseInt(params.startDate);
        dtfil['DATEADDED']['$gte'] = std;
    }
    if (params.endDate.trim().toLowerCase() != '') {
        var ltd = parseInt(params.endDate);
        dtfil['DATEADDED']['$lte'] = ltd;
    }
    if (Object.keys(dtfil['DATEADDED']).length != 0) {
        qFilter['$and'].push(dtfil);
    }

    gsll = parseInt(params.gsRange[0]);
    gsul = parseInt(params.gsRange[1]);

    atll = parseInt(params.atRange[0]);
    atul = parseInt(params.atRange[1]);

    qFilter['$and'].push({'GoldsteinScale': {'$gte': gsll, '$lte': gsul}});
    qFilter['$and'].push({'AvgTone': {'$gte': atll, '$lte': atul}});


    if (params.search.value.trim().toLowerCase() != '') {

        qFilter['$and'].push({
            '$or': [
                {'Actor1Name': new RegExp(params.search.value.trim().toLowerCase())},
                {'Actor2Name': new RegExp(params.search.value.trim().toLowerCase())},
                {'Actor1Geo_FullName': new RegExp(params.search.value.trim().toLowerCase())},
                {'Actor2Geo_FullName': new RegExp(params.search.value.trim().toLowerCase())},
                {'ActionGeo_FullName': new RegExp(params.search.value.trim().toLowerCase())},
                {'SOURCEURL': new RegExp(params.search.value.trim().toLowerCase())}
            ]
        })
        //qFilter['$text'] = {'$search': params.search.value.trim(), '$caseSensitive': false}
    }
    //add other filters as well
    return [qFilter, options]
}


module.exports = router;
