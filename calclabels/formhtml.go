package calclabels

// String representing html for simple service access
const formHTML=`
<html>                                                                                                   
<head>
<script src="http://code.jquery.com/jquery-1.11.0.min.js"></script>                                      
<script type="text/javascript" src="//www.google.com/jsapi"></script>

<script>
var result_location = "";

function segSelect(){
    var segspec = document.getElementById("segspecific");
    if (document.forms[0].algorithm.options[document.forms[0].algorithm.selectedIndex].value == "segment") {
        segspec.style.visibility = "visible";
    }
    else {
        segspec.style.visibility = "hidden";
    }
}
</script>
</head>

<H2>Create DVID Datatype Instance Using Cluster</H2>
<i>Segmentation works on the 'grayscale' datatype instance and stand-alone graph computation works on the 'bodies' datatype instance</i>
<form id="calclabels" method="post">
DVID server (e.g., emdata1:80): <input type="text" id="dvidserver" value="DEFAULT"><br>
DVID uuid: <input type="text" id="uuid"><br>
Job size: <input type="text" id="jobsize" value="512"><br>
Name of DVID segmentation or graph: <input type="text" id="labelname"><br>
Bounding box coordinate 1 (e.g., "x,y,z"): <input type="text" id="bbox1"><br>
Bounding box coordinate 2 (e.g., "x,y,z"): <input type="text" id="bbox2"><br>
or <br>
ROI name: <input type="text" id="roi"><br>

<div id="segspecific" style="visibility: hidden">
Boundary classifier name (stored on DVID at classifiers/): <input type="text" id="classifier"><br>
Agglomeration classifier name (XML stored on DVID at classifiers/): <input type="text" id="agglomclassifier"><br>
Agglomeration feature file [OPTIONAL] (txt stored on DVID at classifiers/): <input type="text" id="agglomfeaturefile"><br>
Graph classifier name [OPTIONAL] (H5 stored on DVID at classifiers/): <input type="text" id="graphclassifier"><br>
Synapse file [OPTIONAL] (JSON stored on DVID at annotations/): <input type="text" id="synapses"><br>

Stitch mode (0 means no stitch, 3 is most aggressive stitch):  <input type="text" id="stitchmode" value="2"><br>
Seed size: <input type="text" id="seedsize" value="5"><br>
Agglom threshold: <input type="text" id="agglomthreshold" value="0.2"><br>
</div>

Algorithm name: <select id="algorithm" onchange="segSelect()"><option value="compute-graph" selected="selected">Compute Graph Only</option><option value="segment">Segment</option></select><br>
<input type="submit" value="Submit"/>
</form>

<hr>
<br>
<div id="status"></div><br>
<div id="results"></div><br>
</div>

<script>
    setInterval(loadUpdate, 5000);

    $("#calclabels").submit(function(event) {                                                           
      event.preventDefault();
      $('#status').html("")

      $.ajax({
        type: "POST",
        url: "/formhandler/",
        data: {uuid: $('#uuid').val(), bbox1: $('#bbox1').val(), bbox2: $('#bbox2').val(), classifier: $('#classifier').val(), agglomclassifier: $('#agglomclassifier').val(), agglomfeaturefile: $('#agglomfeaturefile').val(), labelname: $('#labelname').val(), dvidserver: $('#dvidserver').val(), algorithm: $('#algorithm').val(), jobsize: $('#jobsize').val(), stitchmode: $('#stitchmode').val(), seedsize: $('#seedsize').val(), agglomthreshold: $('#agglomthreshold').val(), graphclassifier: $('#graphclassifier').val(), synapses: $('#synapses').val(), roi: $('#roi').val()},
        success: function(data){
            result_location = data["result-callback"];
            $('#status').html("Location of result on DVID: " + result_location);
        },

        error: function(msg) {
                $('#status').html("Error Processing Results: " + msg.responseText);
          }
        });
    });
    function loadUpdate() {
        if (result_location != "") {
            $.ajax({
                type: "GET",
                url: "/jobstatus/" + result_location,
                success: function(data){
                    $('#results').html(data);
                },
                error: function(msg) {
                    $('#results').html("No response for callback");
                }
            });
        }      
    }                                                                                                
</script>                                                                                                
</html>                                  
`
