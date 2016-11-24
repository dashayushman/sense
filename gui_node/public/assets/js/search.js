var table;
var headerSearchQuery = '';
var loc = ['bottom', 'right'];
var style = 'flat';
var classes = 'messenger-fixed';
for (var i=0; i < loc.length; i++)
    classes += ' messenger-on-' + loc[i];
$.globalMessenger({ extraClasses: classes, theme: style });
Messenger.options = { extraClasses: classes, theme: style };

$(document).ready(function () {
    loadMetadata();
    var today = new Date();
    var buff = new Date()
    var lastMonth = new Date(buff.setMonth(buff.getMonth() - 1));
    $('#idStartDate').datepicker('setDate', lastMonth);

    $('#idEndDate').datepicker('setDate', today);
    $('#idgsslider').slider();
    $('#idatslider').slider();
    Messenger().post({
        message: "Loading events since the last one month. Please use the filter option to extract more relevant data.",
        type: 'info',
        showCloseButton: true
    });
    table = $('#example').DataTable({
        serverSide: true,
        processing: true,
        oLanguage: {
            "sProcessing": '<div class="pull-right"> <i class="fa fa-spinner fa fa-6x fa-spin"></i> </div>'
        },
        ajax: {
            url: 'getEvents',
            type: 'POST',
            data: function (d) {
                var sdt = moment($('#idStartDate').find("input").val(), 'MM/DD/YYYY').format('YYYYMMDDHHmmss');
                var edt = moment($('#idEndDate').find("input").val(), 'MM/DD/YYYY').format('YYYYMMDDHHmmss');
                d.startDate = sdt;
                d.endDate = edt;
                d.gsRange = $('#idgsslider').data('slider').getValue();
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
            {"data": "GLOBALEVENTID"},
            {"data": "DATEADDED"},
            {"data": "Actor1Name"},
            {"data": "Actor1Geo_FullName"},
            {"data": "Actor2Name"},
            {"data": "Actor2Geo_FullName"},
            {"data": "ActionGeo_FullName"},
            {"data": "GoldsteinScale"},
            {"data": "NumMentions"},
            {"data": "AvgTone"},
            {"data": "SOURCEURL"},
            {"data": "details"}
        ],
        columnDefs: [
            {"name": "GLOBALEVENTID", "targets": 0},
            {"name": "DATEADDED", "targets": 1},
            {"name": "Actor1Name", "targets": 2},
            {"name": "Actor1Geo_FullName", "targets": 3},
            {"name": "Actor2Name", "targets": 4},
            {"name": "Actor2Geo_FullName", "targets": 5},
            {"name": "ActionGeo_FullName", "targets": 6},
            {"name": "GoldsteinScale", "targets": 7},
            {"name": "NumMentions", "targets": 8},
            {"name": "AvgTone", "targets": 9},
            {"name": "SOURCEURL", "targets": 10, "orderable": false},
            {"name": "details", "targets": 11, "orderable": false}
        ],
        fnCreatedRow: function (nRow, aData, iDataIndex) {
            $('td:eq(10)', nRow).html('<a href="' + aData.SOURCEURL + '" class="btn btn-primary btn-sm btn-small" style="background-color: #9900FF;" target="_blank">source</a>');
            $('td:eq(11)', nRow).html('<a href="' + aData.details + '" class="btn btn-primary btn-sm btn-small" style="background-color: #FF00FF;" target="_blank">View Details</a>');
            if (aData.AvgTone >= 0) {
                $('td:eq(9)', nRow).html('<span class="bold" style="color: greenyellow;">' + aData.AvgTone.toFixed(2) + '</span>');
            } else {
                $('td:eq(9)', nRow).html('<span class="bold" style="color: orangered;">' + aData.AvgTone.toFixed(2) + '</span>');
            }
            if (aData.GoldsteinScale >= 0) {
                $('td:eq(7)', nRow).html('<span class="bold" style="color: greenyellow;">' + aData.GoldsteinScale.toFixed(2) + '</span>');
            } else {
                $('td:eq(7)', nRow).html('<span class="bold" style="color: orangered;">' + aData.GoldsteinScale.toFixed(2) + '</span>');
            }
            $('td:eq(1)', nRow).html('<span class="bold" style="color: #D5B9F1">' + moment(aData.DATEADDED.toString(), "YYYYMMDDHHmmss").format('LLL') + '</span>');
            $('td:eq(0)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.GLOBALEVENTID + '</span>');
            $('td:eq(2)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.Actor1Name + '</span>');
            $('td:eq(3)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.Actor1Geo_FullName + '</span>');
            $('td:eq(4)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.Actor2Name + '</span>');
            $('td:eq(5)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.Actor2Geo_FullName + '</span>');
            $('td:eq(6)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.ActionGeo_FullName + '</span>');
            $('td:eq(8)', nRow).html('<span class="bold" style="color: #D5B9F1">' + aData.NumMentions + '</span>');

        }
    });

});

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

    $( "#idHeaderSearchLoader" ).toggleClass( "hide" );
    Messenger().post({
        message: "Searching Events.",
        type: 'info',
        showCloseButton: true
    });
    table.ajax.reload( function ( json ) {
        $( "#idHeaderSearchLoader" ).toggleClass( "hide" );

    });
});