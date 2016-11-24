
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
                loadNoa(sdt, edt);
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


function loadNoa(sdt, edt) {
    if(sdt == null || edt == null){
        sdt = '';
        edt = '';
    }
    network_of_actors = 1; // status loading
    $.ajax({
        type : "GET",
        async : true,
        url : "getActorNetwork?sdt=" + sdt + '&edt=' + edt,
        dataType: "json"
    }).done(function(res) {
        if(res.status == 1){
            hide("id_loader_noa");
            hide('idFilterLoader');
            status = updateActorNetwork(res.nodes,res.edges);
            if(status){
                unHide("id_cont_noa");
                Messenger().post({
                    type: 'info',
                    message: "Network of Actors loaded with data for the selected date. Please use the filter to select the date you want to visualize data for.",
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

        }else if(res.status == -1){
            Messenger().post({
                message: "Unfortunately, No data was found for the selected date.",
                type: 'error',
                showCloseButton: true
            });
            global_impact_map = 0; //not loaded
            hide('idFilterLoader');
        }else{
            hide('idFilterLoader');
            Messenger().post({
                message: "Unfortunately, Actor Network could not be loaded from the database. Please try again later.",
                type: 'error',
                showCloseButton: true
            });
            network_of_actors = 0; //not loaded
        }
    }).error(function(res) {
        hide('idFilterLoader');
        Messenger().post({
            message: "Unfortunately,Actor Network could not be loaded from the database. Please try again later.",
            type: 'error',
            showCloseButton: true
        });
        network_of_actors = 0; //not loaded
    });
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
                    max: 70,
                    label: {
                        min: 8,
                        max: 50,
                        drawThreshold: 12,
                        maxVisible: 20
                    }
                },
                font: {
                    size: 20,
                    face: 'Tahoma',
                    color:'white'
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
                    "gravitationalConstant": -300,
                    "centralGravity": 0.02,
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
    loadNoa(sdt, edt)
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