var table;
statcount = 3;
var headerSearchQuery = '';
var tableLoaded = false;
var loc = ['bottom', 'right'];
var style = 'flat';
var classes = 'messenger-fixed';
for (var i=0; i < loc.length; i++)
    classes += ' messenger-on-' + loc[i];
$.globalMessenger({ extraClasses: classes, theme: style });
Messenger.options = { extraClasses: classes, theme: style };

var tableRows = '<tr><td class="bold text-white">MYPROP</td><td class="bold" style="color: #D5B9F1">MYVAL</td></tr>'


$(document).ready(function () {
    initFilter(eventId);
    loadMetadata();
    var eventId = getParameterByName('id', 0);
    if(eventId == null){
        getMostRecentEvent();
        return;
    }
    if(eventId == ''){
        getMostRecentEvent();
        return;
    }
    if(!isANumber(eventId)){
        $( "#id_loader_eo" ).toggleClass( "hide" );
        $( "#id_loader_ao" ).toggleClass( "hide" );
        $( "#id_loader_impact" ).toggleClass( "hide" );
        $( "#id_loader_mo" ).toggleClass( "hide" );
        Messenger().post({
            message: "Invalid Event Id.",
            type: 'error',
            showCloseButton: true
        });
        return;
    }


    loadEventDetails(eventId);


});

function isANumber(str){
    var isnum = /^\d+$/.test(str);
    return isnum;
}

function getMostRecentEvent(){
    $.ajax({
        type : "GET",
        async : true,
        url : "getMostRecentEvent",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            eventId = res.eventid
            loadMetadata(eventId);
            initFilter(eventId);
            loadEventDetails(eventId);

        }else{
            $( "#id_loader_eo" ).toggleClass( "hide" );
            $( "#id_loader_ao" ).toggleClass( "hide" );
            $( "#id_loader_impact" ).toggleClass( "hide" );
            $( "#id_loader_mo" ).toggleClass( "hide" );
            Messenger().post({
                message: "Could Not load the most recent event.",
                type: 'error',
                showCloseButton: true
            });
            return;
        }
    }).error(function(msg) {
        $( "#id_loader_eo" ).toggleClass( "hide" );
        $( "#id_loader_ao" ).toggleClass( "hide" );
        $( "#id_loader_impact" ).toggleClass( "hide" );
        $( "#id_loader_mo" ).toggleClass( "hide" );
        Messenger().post({
            message: "Could Not load the most recent event.",
            type: 'error',
            showCloseButton: true
        });
        return;
    });
}

function loadMentionsStats(eventId, stdt, enddt, all, bindid){


    $.ajax({
        type : "GET",
        async : true,
        url : "getMentionCount?id=" + eventId + '&startdt=' + stdt + '&enddt='+enddt + '&all='+all,
        dataType: "json"
    }).done(function(res) {
            if(res.status == 1){
                $( "#"+ bindid ).html( res.count );
                statcount -= 1;
                if(statcount == 0){
                    $( "#id_loader_mo" ).toggleClass( "hide" );
                    $( "#id_cont_mo" ).toggleClass( "hide" );
                }
            }else{
                $( "#id_loader_mo" ).toggleClass( "hide" );
                Messenger().post({
                    message: "Unfortunately, Mentions data could not be loaded.",
                    type: 'error',
                    showCloseButton: true
                });
            }
        }).error(function(msg) {
        $( "#id_loader_eo" ).toggleClass( "hide" );
        $( "#id_loader_ao" ).toggleClass( "hide" );
        $( "#id_loader_impact" ).toggleClass( "hide" );
        $( "#id_loader_mo" ).toggleClass( "hide" );
        Messenger().post({
            message: "Unfortunately, Event Data couldnot be loaded.",
            type: 'error',
            showCloseButton: true
        });
    });

}

function loadEventDetails(eventId){

    $.ajax({
        type : "GET",
        async : true,
        url : "getEvent?id=" + eventId,
        dataType: "json"
    })
        .done(function(res) {
            if(res.status == 1){
                bindEventData(res.data);
                bindImpactData(res.data);
                today_enddt = moment((new Date()).toString()).format('YYYYMMDDHHmmss');
                today_stdt = moment((new Date()).toString()).subtract(1, 'day').format('YYYYMMDDHHmmss');

                month_enddt = moment((new Date()).toString()).format('YYYYMMDDHHmmss');
                month_stdt = moment((new Date()).toString()).subtract(1, 'months').format('YYYYMMDDHHmmss');

                all = 1;
                loadMentionsStats(eventId, today_stdt, today_enddt, 1, "id_om_total");
                loadMentionsStats(eventId, today_stdt, today_enddt, 0, "id_om_today");
                loadMentionsStats(eventId, month_stdt, month_enddt, 0, "id_om_month");
                loadMentionsTable(eventId);

            }else if(res.status == -2){
                $( "#id_loader_eo" ).toggleClass( "hide" );
                $( "#id_loader_ao" ).toggleClass( "hide" );
                $( "#id_loader_impact" ).toggleClass( "hide" );
                $( "#id_loader_mo" ).toggleClass( "hide" );
                Messenger().post({
                    message: "Event id does not exist.",
                    type: 'error',
                    showCloseButton: true
                });
            } else {
                $( "#id_loader_eo" ).toggleClass( "hide" );
                $( "#id_loader_ao" ).toggleClass( "hide" );
                $( "#id_loader_impact" ).toggleClass( "hide" );
                $( "#id_loader_mo" ).toggleClass( "hide" );
                Messenger().post({
                    message: "Unfortunately, Event data could not be loaded.",
                    type: 'error',
                    showCloseButton: true
                });
            }
        }).error(function(msg) {
            $( "#id_loader_eo" ).toggleClass( "hide" );
            $( "#id_loader_ao" ).toggleClass( "hide" );
            $( "#id_loader_impact" ).toggleClass( "hide" );
            $( "#id_loader_mo" ).toggleClass( "hide" );
            Messenger().post({
                message: "Unfortunately, Event Data couldnot be loaded.",
                type: 'error',
                showCloseButton: true
            });
    });

}


function bindImpactData(data) {
    //red #f65854
    //green #0AA699

    var gs = data.GoldsteinScale;
    var at = data.AvgTone;
    $( "#idgsvalue" ).html(gs.toFixed(2))
    $( "#idatvalue" ).html(at.toFixed(2))
    if(gs > 0){
        $( "#idgstile" ).addClass( "green");
    }else if(gs == 0){
        $( "#idgstile" ).addClass( "blue");
    }else{
        $( "#idgstile" ).addClass( "red");
    }

    if(at > 0){
        $( "#idattile" ).addClass( "green");
    }else if(at == 0){
        $( "#idattile" ).addClass( "blue");
    }else{
        $( "#idattile" ).addClass( "red");
    }

}

function bindEventData(data){
    for(var i = 0; i < data.act1Table.length; i++){
        row = tableRows;
        row = row.replace(new RegExp("MYPROP", 'g'), data.act1Table[i][0]);
        row = row.replace(new RegExp("MYVAL", 'g'), data.act1Table[i][1]);
        $( "#id_act1_body" ).append( row );
    }

    for(var i = 0; i < data.act2Table.length; i++){
        row = tableRows;
        row = row.replace(new RegExp("MYPROP", 'g'), data.act2Table[i][0]);
        row = row.replace(new RegExp("MYVAL", 'g'), data.act2Table[i][1]);
        $( "#id_act2_body" ).append( row );
    }
    for(var i = 0; i < data.actionTable.length; i++){
        row = tableRows;
        row = row.replace(new RegExp("MYPROP", 'g'), data.actionTable[i][0]);
        row = row.replace(new RegExp("MYVAL", 'g'), data.actionTable[i][1]);
        $( "#id_action_body" ).append( row );
    }
    for(var i = 0; i < data.eventTable.length; i++){
        row = tableRows;
        row = row.replace(new RegExp("MYPROP", 'g'), data.eventTable[i][0]);
        row = row.replace(new RegExp("MYVAL", 'g'), data.eventTable[i][1]);
        $( "#id_eo_body" ).append( row );
    }
    $( "#id_loader_eo" ).toggleClass( "hide" );
    $( "#id_loader_ao" ).toggleClass( "hide" );
    $( "#id_loader_impact" ).toggleClass( "hide" );
    //$( "#id_loader_mo" ).toggleClass( "hide" );

    $( "#id_cont_eo" ).toggleClass( "hide" );
    $( "#id_cont_ao" ).toggleClass( "hide" );
    $( "#id_cont_impact" ).toggleClass( "hide" );
    //$( "#id_cont_mo" ).toggleClass( "hide" );
}



function initFilter(hello){
    var today = new Date();
    var buff = new Date();
    var lastMonth = new Date(buff.setMonth(buff.getMonth() - 5));
    $('#idStartDate').datepicker('setDate', lastMonth);
    $('#idEndDate').datepicker('setDate', today);
    $('#idatslider').slider();
}

function loadMentionsTable(eventId){

    table = $('#example').DataTable({
        serverSide: true,
        processing: true,
        oLanguage: {
            "sProcessing": '<div class="pull-right"> <i class="fa fa-spinner fa fa-6x fa-spin"></i> </div>'
        },
        ajax: {
            url: 'getMentions?id='+eventId,
            type: 'POST',
            data: function (d) {
                var sdt = moment($('#idStartDate').find("input").val(), 'MM/DD/YYYY').format('YYYYMMDDHHmmss');
                var edt = moment($('#idEndDate').find("input").val(), 'MM/DD/YYYY').format('YYYYMMDDHHmmss');
                d.startDate = sdt;
                d.endDate = edt;
                d.atRange = $('#idatslider').data('slider').getValue();

                var searchquery = getParameterByName('q');
                if(searchquery != null){
                    d.search.value = searchquery;
                }
                if(headerSearchQuery != ''){
                    d.search.value = headerSearchQuery;
                }
            }
        },
        columns: [
            {"data": "MentionTimeDate"},
            {"data": "MentionType"},
            {"data": "MentionSourceName"},
            {"data": "MentionIdentifier"},
            {"data": "Confidence"},
            {"data": "MentionDocTone"}
        ],
        columnDefs: [
            {"name": "MentionTimeDate", "targets": 0},
            {"name": "MentionType", "targets": 1},
            {"name": "MentionSourceName", "targets": 2},
            {"name": "MentionIdentifier", "targets": 3, "orderable": false},
            {"name": "Confidence", "targets": 4},
            {"name": "MentionDocTone", "targets": 5}
        ],
        fnCreatedRow: function (nRow, aData, iDataIndex) {
            $('td:eq(3)', nRow).html('<a href="' + aData.MentionIdentifier + '" class="btn btn-primary btn-sm btn-small" style="background-color: #9900FF;" target="_blank">Source URI</a>');
            if (aData.MentionDocTone >= 0) {
                $('td:eq(5)', nRow).html('<span class="bold" style="color: greenyellow;">' + aData.MentionDocTone.toFixed(2) + '</span>');
            } else {
                $('td:eq(5)', nRow).html('<span class="bold" style="color: orangered;">' + aData.MentionDocTone.toFixed(2) + '</span>');
            }

            $('td:eq(0)', nRow).html('<span class="bold" style="color: #D5B9F1">' + moment(aData.MentionTimeDate.toString(), "YYYYMMDDHHmmss").format('LLL') + '</span>');
            $('td:eq(1)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.MentionType + '</span>');
            $('td:eq(2)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.MentionSourceName + '</span>');
            $('td:eq(4)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.Confidence + '</span>');
            tableLoaded = true;
        }
    });
}

$("#idFilterButton").click(function() {

    $( "#idFilterLoader" ).toggleClass( "hide" );

    table.ajax.reload( function ( json ) {
        $( "#idFilterLoader" ).toggleClass( "hide" );

    });
});


function loadMetadata(){
    $.ajax({
        type : "GET",
        async : true,
        url : "getMetadata",
        dataType: "json"
    })
        .done(function(res) {
            if(res.status == 1){
                //id_lst_upd_date
                $( "#id_lst_upd_date" ).html( res.last_update_date_str );
            }else{
                Messenger().post({
                    message: "Unfortunately, Metadata could not be loaded.",
                    type: 'error',
                    showCloseButton: true
                });
            }
        }).error(function(msg) {
        Messenger().post({
            message: "Unfortunately, Metadata could not be loaded.",
            type: 'error',
            showCloseButton: true
        });
    });
}

function getParameterByName(name, url) {
    if (!url) {
        url = window.location.href;
    }
    name = name.replace(/[\[\]]/g, "\\$&");
    var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}

$( "#idHeaderSearchBar" ).submit(function( event ) {

    event.preventDefault();
    headerSearchQuery =  $("#idHeaderSearch").val();
    window.location = "/search?q="+headerSearchQuery;
});