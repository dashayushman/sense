
//Loading states of various blocks of the UI
var global_impact_map = 0;
var overall_stats = 0;
var linked_geo = 0;
var hir = 0;
var hie =0;
var articles_mentioned_per_cat = 0;
var mentions_trend = 0;
var network_of_actors = 0;

var obj_hir = null;
var obj_hie = null;
var hir_row = '<tr><td class="v-align-middle bold" style="color: #FF2EFF">GEOCODE</td><td class="v-align-middle"><span class="muted">CONNAME</span> </td><td><span class="muted bold" style="color: #FF2EFF">LOCATION</span> </td><td class="v-align-middle"><span class="bold HALOOCOL">IMPACT</span></td></tr>'
var hir_col_green = "text-success"
var hir_col_red = "text-error"

var hie_row = '<tr><td class="v-align-middle bold" style="color: #FF2EFF">EVENTTYPE</td><td class="v-align-middle"><span class="bold text-info" >ACTOR1</span> </td><td><span class="bold text-info">GEOACT1</span> </td><td class="v-align-middle"><span class="bold" style="color: #D5B9F1">ACTOR2</span> </td><td><span class="bold" style="color: #D5B9F1">GEOACT2</span> </td><td class="v-align-middle bold" style="color: #6DFFFA">IMPORTANCE</td></tr>'

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
            loadGim();
        }
    },
    offset: '75%'
});

var waypoint_os = new Waypoint({
    element: document.getElementById('id_wp_os'),
    handler: function(direction) {
        if(overall_stats == 0){
            loadOs();
        }
    },
    offset: '40%'
});



var waypoint_hir = new Waypoint({
    element: document.getElementById('id_wp_hir'),
    handler: function(direction) {
        if(hir == 0){
            loadHir();
        }
    },
    offset: '20%'
});

var waypoint_hie = new Waypoint({
    element: document.getElementById('id_wp_hie'),
    handler: function(direction) {
        if(hie == 0){
            loadHie();
        }
    },
    offset: '20%'
});


var waypoint_mt = new Waypoint({
    element: document.getElementById('id_wp_mt'),
    handler: function(direction) {
        if(mentions_trend == 0){
            loadMt();
        }
    },
    offset: '20%'
});

$( ".btn-small" ).click(function() {
    if(hir == 0){
        Messenger().post({
            message: "Top 10 list not loaded yet. Please wait for it to gets loaded.",
            type: 'info',
            showCloseButton: true
        });
        return;
    }else if(hir == 1){
        Messenger().post({
            message: "Top 10 list is loading. Please wait for it to gets loaded.",
            type: 'info',
            showCloseButton: true
        });
        return;
    }
    hide("id_cont_hir");
    unHide("id_loader_hir")
    val = this.value;
    if(val == null){
        Messenger().post({
            message: "Unknown button pressed. Unable to load Top 10 High Impact Regions.",
            type: 'error',
            showCloseButton: true
        });
        return;
    }
    if(val != "none" && val.length != 0){
        top10List = null;
        if(val.includes("hir")){
            top10List = obj_hir[val];
        }
        if(val.includes("hie")){
            top10List = obj_hie[val];
        }

        if(top10List != null){
            status = false;
            if(val.includes("hie")){
                status = updateHieTable(top10List.data);
            }
            if(val.includes("hir")){
                status = updateHirTable(top10List.data);
            }
            if(status){
                hide("id_loader_hir");
                unHide("id_cont_hir");
            }
        }else{
            Messenger().post({
                message: "Some error occurred. Try again later or refresh the page.",
                type: 'error',
                showCloseButton: true
            });
        }

    }else{
        hide("id_loader_hir");
        unHide("id_cont_hir");
        Messenger().post({
            message: "View all feature will be added very soon.",
            type: 'error',
            showCloseButton: true
        });
        return;
    }

});

function loadLinkedLocation() {
    linked_geo = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getLinkedLocations",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_lgeo");

            status = loadLinkedLocationMap(res.lines,res.images);
            if(status){
                unHide("id_cont_lgeo");
                Messenger().post({
                    message: "Linked locations Map loaded with 15 minutes time resolution.",
                    type: 'info',
                    showCloseButton: true
                });
                linked_geo = 2; //status loaded
            }else{
                Messenger().post({
                    message: "Unfortunately, Linked locations object could not be initialized.",
                    type: 'error',
                    showCloseButton: true
                });
                linked_geo = 0; //not loaded
            }

        }else{
            Messenger().post({
                message: "Unfortunately, Linked locations could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            linked_geo = 0; //not loaded
        }
    }).error(function(res) {
        Messenger().post({
            message: "Unfortunately, the Linked locations could not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        linked_geo = 0; //not loaded
    });
}

function loadOs(){
    overall_stats = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getOverallStats",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_os");
            status = loadStats(res.n_events_now,
                res.n_events_today,
                res.n_events_this_month,
                res.e_percent_higher_last_month,
                res.n_mentions_now,
                res.n_mentions_today,
                res.n_mentions_this_month,
                res.m_percent_higher_last_month,
                res.n_countries_now,
                res.n_countries_today,
                res.n_countries_this_month,
                res.c_percent_higher_last_month);
            if(status){
                unHide("id_cont_os");
                Messenger().post({
                    message: "Overall Stats loaded with data.",
                    type: 'info',
                    showCloseButton: true
                });
                overall_stats = 2; //status loaded
            }else{
                Messenger().post({
                    message: "Unfortunately, Overall Stats object could not be initialized.",
                    type: 'error',
                    showCloseButton: true
                });
                overall_stats = 0; //not loaded
            }

        }else{
            Messenger().post({
                message: "Unfortunately, Overall Stats could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            overall_stats = 0; //not loaded
        }
    }).error(function(res) {
        Messenger().post({
            message: "Unfortunately, Overall Stats could not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        overall_stats = 0; //not loaded
    });
}

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

function loadNoa() {
    network_of_actors = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getActorNetwork",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_noa");
            status = updateActorNetwork(res.nodes,res.edges);
            if(status){
                unHide("id_cont_noa");
                Messenger().post({
                    type: 'info',
                    message: "Actor Network loaded.",
                    showCloseButton: true
                });
                network_of_actors = 2; //status loaded
            }else{
                Messenger().post({
                    message: "Unfortunately,Actor Network object could not be initialized.",
                    type: 'error',
                    showCloseButton: true
                });
                network_of_actors = 0; //not loaded
            }

        }else{
            Messenger().post({
                message: "Unfortunately, Actor Network could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            network_of_actors = 0; //not loaded
        }
    }).error(function(res) {
        Messenger().post({
            message: "Unfortunately,Actor Network could not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        network_of_actors = 0; //not loaded
    });
}

function loadMt() {
    mentions_trend = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getMentionsTimeline",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_mt");
            status = updateMtLineChart(res.data,res.sources);
            if(status){
                unHide("id_cont_mt");
                Messenger().post({
                    type: 'info',
                    message: "Mentions Timeline loaded.",
                    showCloseButton: true
                });
                mentions_trend = 2; //status loaded
            }else{
                Messenger().post({
                    message: "Unfortunately, Mentions Timeline object could not be initialized.",
                    type: 'error',
                    showCloseButton: true
                });
                mentions_trend = 0; //not loaded
            }

        }else{
            Messenger().post({
                message: "Unfortunately, Mentions Timeline could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            mentions_trend = 0; //not loaded
        }
    }).error(function(res) {
        Messenger().post({
            message: "Unfortunately, Mentions Timelinecould not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        mentions_trend = 0; //not loaded
    });
}

function loadAmpc() {
    articles_mentioned_per_cat = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getArticlesPerCategory",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_ampc");
            status = updateAmpcPie(res.data);
            if(status){
                unHide("id_cont_ampc");
                Messenger().post({
                    type: 'info',
                    message: "Articles Mentioned Per Category loaded.",
                    showCloseButton: true
                });
                articles_mentioned_per_cat = 2; //status loaded
            }else{
                Messenger().post({
                    message: "Unfortunately, Articles Mentioned Per Category object could not be initialized.",
                    type: 'error',
                    showCloseButton: true
                });
                articles_mentioned_per_cat = 0; //not loaded
            }

        }else{
            Messenger().post({
                message: "Unfortunately, Articles Mentioned Per Category could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            articles_mentioned_per_cat = 0; //not loaded
        }
    }).error(function(res) {
        Messenger().post({
            message: "Unfortunately, Articles Mentioned Per Category could not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        articles_mentioned_per_cat = 0; //not loaded
    });
}

function loadHie() {
    hie = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getHighImpactEvents",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_hie");
            obj_hie = res;
            status = updateHieTable(res.hie_15.data);
            if(status){
                unHide("id_cont_hie");
                Messenger().post({
                    type: 'info',
                    message: "High Impact events loaded.",
                    showCloseButton: true
                });
                hie = 2; //status loaded
            }else{
                Messenger().post({
                    message: "Unfortunately, High Impact events object could not be initialized.",
                    type: 'error',
                    showCloseButton: true
                });
                hie = 0; //not loaded
            }

        }else{
            Messenger().post({
                message: "Unfortunately, High Impact events could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            hie = 0; //not loaded
        }
    }).error(function(res) {
        Messenger().post({
            message: "Unfortunately, High Impact events could not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        hie = 0; //not loaded
    });
}

function loadHir() {
    hir = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getHighImpactRegions",
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_hir");
            obj_hir = res;
            status = updateHirTable(res.hir_15.data);
            if(status){
                unHide("id_cont_hir");
                Messenger().post({
                    type: 'info',
                    message: "High Impact Regions loaded.",
                    showCloseButton: true
                });
                hir = 2; //status loaded
            }else{
                Messenger().post({
                    message: "Unfortunately, High Impact Regions object could not be initialized.",
                    type: 'error',
                    showCloseButton: true
                });
                hir = 0; //not loaded
            }

        }else{
            Messenger().post({
                message: "Unfortunately, High Impact Regions could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            hir = 0; //not loaded
        }
    }).error(function(res) {
        Messenger().post({
            message: "Unfortunately, High Impact Regions could not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        hir = 0; //not loaded
    });
}

function loadGim(){
    global_impact_map = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getGlobalImpactMapData",
        dataType: "json"
        }).done(function(res) {
            if(res.status == 1){
                hide("id_loader_gim");

                status = loadBubbleMap(res.latlong,res.mapData);
                if(status){
                    unHide("id_cont_gim");
                    Messenger().post({
                        message: "Global Impact Map loaded with data with 15 minutes time resolution.",
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

            }else{
                Messenger().post({
                    message: "Unfortunately, the Global Impact Map data could not be loaded from the database. Please try again later.",
                    type: 'error',
                    showCloseButton: true
                });
                global_impact_map = 0; //not loaded
            }
        }).error(function(res) {
            Messenger().post({
                message: "Unfortunately, the Global Impact Map data could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            global_impact_map = 0; //not loaded
        });
}

function loadStats(n_events_now,n_events_today,n_events_this_month,e_percent_higher_last_month,n_mentions_now,n_mentions_today,n_mentions_this_month,m_percent_higher_last_month,n_countries_now,n_countries_today,n_countries_this_month,c_percent_higher_last_month) {

    $( "#id_oe_total" ).html( n_events_now );
    $( "#id_oe_today" ).html( n_events_today );
    $( "#id_oe_month" ).html( n_events_this_month );
    $( "#id_oe_higher" ).html( e_percent_higher_last_month.toFixed(2) );

    $( "#id_om_total" ).html( n_mentions_now );
    $( "#id_om_today" ).html( n_mentions_today );
    $( "#id_om_month" ).html( n_mentions_this_month);
    $( "#id_om_higher" ).html( m_percent_higher_last_month.toFixed(2) );

    $( "#id_ol_total" ).html( n_countries_now );
    $( "#id_ol_today" ).html( n_countries_today );
    $( "#id_ol_month" ).html( n_countries_this_month );
    $( "#id_ol_higher" ).html( c_percent_higher_last_month.toFixed(2) );
    return true;
}

function updateHirTable(top10List) {
//GEOCODE   CONNAME LOCATION    IMPACT  IMPACTCOL
    $( "#id_hir_body" ).html("")
    var i;

    for (i = 0; i < top10List.length; i++) {
        obj = top10List[i];
        row = hir_row;
        row = row.replace(new RegExp("GEOCODE", 'g'), obj.ActionGeo_CountryCode);
        row = row.replace(new RegExp("CONNAME", 'g'), obj.FIPS_Country_Name);
        row = row.replace(new RegExp("LOCATION", 'g'), obj.ActionGeo_FullName);
        row = row.replace(new RegExp("IMPACT", 'g'), obj.GoldsteinScale);
        if(obj.GoldsteinScale >= 0){
            row = row.replace(new RegExp("HALOOCOL", 'g'), hir_col_green);
        }else{
            row = row.replace(new RegExp("HALOOCOL", 'g'), hir_col_red);
        }
        $( "#id_hir_body" ).append( row );

    }
    return true
}

function updateHieTable(top10List) {
//EVENTTYPE   ACTOR1 GEOACT1    ACTOR2  GEOACT2 IMPORTANCE
    $( "#id_hie_body" ).html("")
    var i;

    for (i = 0; i < top10List.length; i++) {
        obj = top10List[i];
        row = hie_row;
        row = row.replace(new RegExp("EVENTTYPE", 'g'), obj.EventType);
        row = row.replace(new RegExp("ACTOR1", 'g'), obj.Actor1Name);
        row = row.replace(new RegExp("GEOACT1", 'g'), obj.Actor1Geo_FullName);
        row = row.replace(new RegExp("ACTOR2", 'g'), obj.Actor2Name);
        row = row.replace(new RegExp("GEOACT2", 'g'), obj.Actor2Geo_FullName);
        row = row.replace(new RegExp("IMPORTANCE", 'g'), obj.score.toFixed(3));


        $( "#id_hie_body" ).append( row );

    }
    return true
}

function updateMtLineChart(data,sources) {
    AmCharts.makeChart("trendchartdiv", {
        type: "serial",
        theme: "black",
        export: {
            "enabled": true
        },
        dataProvider: data,
        marginTop: 10,
        categoryField: "year",
        categoryAxis: {
            gridAlpha: 0.07,
            axisColor: "#DADADA",
            startOnAxis: true,
            guides: []
        },
        valueAxes: [{
            stackType: "regular",
            gridAlpha: 0.07,
            title: "Articles from different channels"
        }],

        graphs: [{
            id:"gid_web",
            type: "line",
            title: "Web",
            valueField: "web",
            lineAlpha: 0,
            fillAlphas: 0.6,
            balloonText: "<span style='font-size:14px; color:#F05D5E;'>Web <b>[[value]]</b></span>"
        }, {
            type: "line",
            title: "Citation Only",
            valueField: "citation_only",
            lineAlpha: 0,
            fillAlphas: 0.6,
            balloonText: "<span style='font-size:14px; color:#0F7173;'>Citation Only <b>[[value]]</b></span>"
        }],
        legend: {
            position: "bottom",
            color:"white",
            valueText: "[[value]]",
            valueWidth: 100,
            valueAlign: "left",
            equalWidths: false,
            periodValueText: "total: [[value.sum]]"
        },
        chartCursor: {
            cursorAlpha: 0
        },
        chartScrollbar: {
            "autoGridCount": true,
            "graph": "gid_web",
            "scrollbarHeight": 40
        }

    });
    return true;
}

function updateActorNetwork(nodes,edges) {
    var network;
    var allNodes;
    var highlightActive = true;

    var nodesDataset = nodes; // these come from WorldCup2014.js
    var edgesDataset = edges; // these come from WorldCup2014.js

    function redrawAll() {


        var container = document.getElementById('mynetwork');
        var data = {
            nodes: nodes,
            edges: edges
        };
        var options = {
            nodes: {
                shape: 'dot',
                scaling: {
                    min: 10,
                    max: 30,
                    label: {
                        min: 8,
                        max: 30,
                        drawThreshold: 12,
                        maxVisible: 20
                    }
                },
                font: {
                    size: 12,
                    face: 'Tahoma'
                }
            },
            edges: {
                width: 0.5,
                color: {inherit: 'from'},
                smooth: {
                    "type": "continuous",
                    "forceDirection": "none",
                    "roundness": 0.35
                }
            },
            physics: {
                stabilization: false,
                "forceAtlas2Based": {
                    "gravitationalConstant": -232,
                    "centralGravity": 0.03,
                    "springLength": 100,
                    "avoidOverlap": 0.58
                },
                "maxVelocity": 46,
                "minVelocity": 0.67,
                "solver": "forceAtlas2Based",
                "timestep": 0.46
            },
            interaction: {
                tooltipDelay: 200,
                hideEdgesOnDrag: false
            }
        };
        network = new vis.Network(container, data, options);


        allNodes = Object(nodesDataset);

        network.on("click",neighbourhoodHighlight);
    }

    function neighbourhoodHighlight(params) {
        // if something is selected:
        if (params.nodes.length > 0) {
            highlightActive = true;
            var i,j;
            var selectedNode = params.nodes[0];
            var degrees = 2;

            // mark all nodes as hard to read.
            for (var nodeId in allNodes) {
                allNodes[nodeId].color = 'rgba(200,200,200,0.5)';
                if (allNodes[nodeId].hiddenLabel === undefined) {
                    allNodes[nodeId].hiddenLabel = allNodes[nodeId].label;
                    allNodes[nodeId].label = undefined;
                }
            }
            var connectedNodes = network.getConnectedNodes(selectedNode);
            var allConnectedNodes = [];

            // get the second degree nodes
            for (i = 1; i < degrees; i++) {
                for (j = 0; j < connectedNodes.length; j++) {
                    allConnectedNodes = allConnectedNodes.concat(network.getConnectedNodes(connectedNodes[j]));
                }
            }

            // all second degree nodes get a different color and their label back
            for (i = 0; i < allConnectedNodes.length; i++) {
                allNodes[allConnectedNodes[i]].color = 'rgba(150,150,150,0.75)';
                if (allNodes[allConnectedNodes[i]].hiddenLabel !== undefined) {
                    allNodes[allConnectedNodes[i]].label = allNodes[allConnectedNodes[i]].hiddenLabel;
                    allNodes[allConnectedNodes[i]].hiddenLabel = undefined;
                }
            }

            // all first degree nodes get their own color and their label back
            for (i = 0; i < connectedNodes.length; i++) {
                allNodes[connectedNodes[i]].color = undefined;
                if (allNodes[connectedNodes[i]].hiddenLabel !== undefined) {
                    allNodes[connectedNodes[i]].label = allNodes[connectedNodes[i]].hiddenLabel;
                    allNodes[connectedNodes[i]].hiddenLabel = undefined;
                }
            }

            // the main node gets its own color and its label back.
            allNodes[selectedNode].color = undefined;
            if (allNodes[selectedNode].hiddenLabel !== undefined) {
                allNodes[selectedNode].label = allNodes[selectedNode].hiddenLabel;
                allNodes[selectedNode].hiddenLabel = undefined;
            }
        }
        else if (highlightActive === true) {
            // reset all nodes
            for (var nodeId in allNodes) {
                allNodes[nodeId].color = undefined;
                if (allNodes[nodeId].hiddenLabel !== undefined) {
                    allNodes[nodeId].label = allNodes[nodeId].hiddenLabel;
                    allNodes[nodeId].hiddenLabel = undefined;
                }
            }
            highlightActive = false
        }

// transform the object into an array
        var updateArray = [];
        for (nodeId in allNodes) {
            if (allNodes.hasOwnProperty(nodeId)) {
                updateArray.push(allNodes[nodeId]);
            }
        }
        nodesDataset.update(updateArray);
    }
    redrawAll()
}

function updateAmpcPie(data) {
    AmCharts.makeChart("chartdiv", {
        "type": "pie",
        "color":"black",
        "export": {
            "enabled": true
        },
        "dataProvider": data,
        "titleField": "source",
        "valueField": "n_mentions",
        "balloonText": "[[title]]<br><span style='font-size:14px'><b>[[value]]</b> ([[percents]]%)</span>",
        "legend": {
            "align": "center",
            "markerType": "circle",
            "color":"black"
        }

    });
    return true;
}

function loadLinkedLocationMap(lines,images){
    AmCharts.makeChart("mapdiv2", {
        type: "map",
        export: {
            "enabled": true
        },
        projection:"miller",
        dataProvider: {
            map: "worldLow",

            lines: lines,
            images: images
        },

        areasSettings: {
            unlistedAreasColor: "#FFCC00"
        },

        imagesSettings: {
            color: "#CC0000",
            rollOverColor: "#CC0000",
            selectedColor: "#000000"
        },

        linesSettings: {
            arc: -0.7, // this makes lines curved. Use value from -1 to 1
            arrow: "middle",
            color: "#CC0000",
            alpha: 3,
            arrowAlpha: 1,
            arrowSize: 4,
            dashLength:3
        },
        zoomControl: {
            gridHeight: 100,
            draggerAlpha: 5,
            gridAlpha: 0.2
        },

        backgroundZoomsToTop: false,
        linesAboveImages: true
    });
}

function loadBubbleMap(latlong,mapData){
    // build map
    var map;
    var minBulletSize = 3;
    var maxBulletSize = 70;
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
        map["export"] = {
            "enabled": true
        };
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

            dataProvider.images.push({
                type: "circle",
                theme: "black",
                width: size,
                height: size,
                color: dataItem.color,
                longitude: latlong[id].longitude,
                latitude: latlong[id].latitude,
                title: dataItem.name + ": " + dataItem.avg.toFixed(3),
                value: value,
                gval:dataItem["avg"]
            });
        }
        map.dataProvider = dataProvider;

        map.write("mapdiv");
        return true;
}



function hide(id){
    var el = document.getElementById(id);
    if(!el.classList.contains("hide")){
        el.classList.add("hide")
    }

}

function unHide(id){
    $("#"+id).removeClass("hide");
}


