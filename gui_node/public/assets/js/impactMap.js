
//Loading states of various blocks of the UI
var global_impact_map = 0;


var hie_row = '<tr><td class="v-align-middle bold" style="color: #FF2EFF">EVENTTYPE</td><td class="v-align-middle"><span class="bold text-info" >ACTOR1</span> </td><td><span class="bold text-info">GEOACT1</span> </td><td class="v-align-middle"><span class="bold" style="color: #D5B9F1">ACTOR2</span> </td><td><span class="bold" style="color: #D5B9F1">GEOACT2</span> </td><td class="v-align-middle bold" style="color: #6DFFFA">IMPORTANCE</td><td class="v-align-middle"><a href="SOURCELINK" class="btn btn-primary btn-sm btn-small">source</a></td></tr>'

var loc = ['bottom', 'right'];
var style = 'flat';
var classes = 'messenger-fixed';
for (var i=0; i < loc.length; i++)
    classes += ' messenger-on-' + loc[i];
$.globalMessenger({ extraClasses: classes, theme: style });
Messenger.options = { extraClasses: classes, theme: style };


var waypoint_gim = new Waypoint({
    element: document.getElementById('id_wp_gim'),
    handler: function(direction) {
        if(global_impact_map == 0){
            loadMetadata();

        }
    },
    offset: '75%'
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
                sdt = moment(res.last_update_date.toString(), "YYYYMMDDHHmmss");
                d_sdt = sdt;
                sdt.second(0);
                sdt.minute(0);
                sdt.hour(0);
                sdt = parseInt(sdt.format('YYYYMMDDHHmmss'));

                edt = moment(res.last_update_date.toString(), "YYYYMMDDHHmmss");
                d_edt = edt;
                edt.second(59);
                edt.minute(59);
                edt.hour(23);
                edt = parseInt(edt.format('YYYYMMDDHHmmss'));

                $('#idStartDate').datepicker('setDate', d_sdt.toDate());
                $( "#id_lst_upd_date" ).html( res.last_update_date_str );
                loadGim(sdt, edt);
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


function loadGim(sdt, edt){
    if(sdt == null || edt == null){
        sdt = '';
        edt = '';
    }
    global_impact_map = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getGlobalImpactMapData?sdt=" + sdt + '&edt=' + edt,
        dataType: "json"
        }).done(function(res) {
            if(res.status == 1){
                hide("id_loader_gim");
                hide('idFilterLoader');
                status = loadBubbleMap(res.latlong,res.mapData);
                if(status){
                    unHide("id_cont_gim");
                    Messenger().post({
                        message: "Global Impact Map loaded with data for the selected date. Please use the filter to select the date you want to visualize the data for.",
                        type: 'info',
                        showCloseButton: true
                    });
                    global_impact_map = 2; //status loaded

                }else{
                    Messenger().post({
                        message: "Unfortunately, the Global Impact Map object could not be initialized.",
                        type: 'error',
                        showCloseButton: true
                    });
                    global_impact_map = 0; //not loaded

                }

            }else if(res.status == -1){
                Messenger().post({
                    message: "Unfortunately, No data was found for the selected date.",
                    type: 'error',
                    showCloseButton: true
                });
                global_impact_map = 0; //not loaded
                hide('idFilterLoader');
            }else{
                Messenger().post({
                    message: "Unfortunately, the Global Impact Map could not be loaded from the database. Please try again later.",
                    type: 'error',
                    showCloseButton: true
                });
                global_impact_map = 0; //not loaded
                hide('idFilterLoader');
            }
        }).error(function(res) {
            Messenger().post({
                message: "Unfortunately, the Global Impact Map could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            global_impact_map = 0; //not loaded
            hide('idFilterLoader');
        });
}




function loadBubbleMap(latlong,mapData){
    // build map
    var map;
    var minBulletSize = 1;
    var maxBulletSize = 5;
    var min = Infinity;
    var max = -Infinity;

    AmCharts.theme = AmCharts.themes.dark;

// get min and max values
    for (var i = 0; i < mapData.length; i++) {
        var value = mapData[i].value;
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
    }

        map = new AmCharts.AmMap();
        map.projection = "miller";

        map.addTitle("", 14);
        map.areasSettings = {
            unlistedAreasColor: "#FFFFFF",
            unlistedAreasAlpha: 0.1
        };
        map.imagesSettings = {
            balloonText: "<span style='font-size:14px;'><b>[[title]]</b></span>",
            alpha: 0.6
        }

        map.zoomControl = {
            gridHeight: 100,
            draggerAlpha: 5,
            gridAlpha: 0.2
        }

        var dataProvider = {
            mapVar: AmCharts.maps.worldLow,
            images: []
        }

        // create circle for each country

        // it's better to use circle square to show difference between values, not a radius
        var maxSquare = maxBulletSize * maxBulletSize * 2 * Math.PI;
        var minSquare = minBulletSize * minBulletSize * 2 * Math.PI;

        // create circle for each country
        for (var i = 0; i < mapData.length; i++) {
            var dataItem = mapData[i];
            var value = dataItem.value;
            // calculate size of a bubble
            var square = (value - min) / (max - min) * (maxSquare - minSquare) + minSquare;
            if (square < minSquare) {
                square = minSquare;
            }
            var size = Math.sqrt(square / (Math.PI * 2));
            var id = dataItem.code;
            title_html = '<span style="font-size:14px;"><b>'+ dataItem.name +'</b></span><br>'+ '<span style="font-size:14px;"><b>Goldstein Impact: </b></span>' + dataItem.gs.toFixed(3) + '<br><span style="font-size:14px;"><b>Number of Occurences: </b></span>' + dataItem.count;
            dataProvider.images.push({
                type: "circle",
                theme: "black",
                width: size,
                height: size,
                color: dataItem.color,
                longitude: latlong[id].longitude,
                latitude: latlong[id].latitude,
                title: title_html,
                value: value
            });
        }
        map.dataProvider = dataProvider;

        map.write("mapdiv");
        return true;
}

$("#idFilterButton").click(function() {

    $( "#idFilterLoader" ).toggleClass( "hide" );
    var edt = moment($('#idStartDate').find("input").val(), 'MM/DD/YYYY');
    edt.second(59);
    edt.minute(59);
    edt.hour(23);
    edt = edt.format('YYYYMMDDHHmmss')
    var sdt = moment($('#idStartDate').find("input").val(), 'MM/DD/YYYY');
    sdt.second(0);
    sdt.minute(0);
    sdt.hour(0);
    sdt = sdt.format('YYYYMMDDHHmmss')
    loadGim(sdt, edt)
});

$( "#idHeaderSearchBar" ).submit(function( event ) {

    event.preventDefault();
    url = window.location.href;
    headerSearchQuery =  $("#idHeaderSearch").val();

    $( "#idHeaderSearchLoader" ).toggleClass( "hide" );

    location.href ='/search?q=' + headerSearchQuery;
});


function hide(id){
    //$( "#"+id ).toggleClass( "hide" );
    var el = document.getElementById(id);

    if(!el.classList.contains("hide")){
        el.classList.add("hide")
    }


}

function unHide(id){
    $("#"+id).removeClass("hide");
    //$( "#"+id ).toggleClass( "hide" );
}