package calclabels

// String representing html for simple service access
const formHTML=`
<html>                                                                                                   
<head>
<script src="http://code.jquery.com/jquery-1.11.0.min.js"></script>                                      
<script type="text/javascript" src="//www.google.com/jsapi"></script>

</head>
<H2>Create DVID Datatype Instance Using Cluster</H2>
<form id="calclabels" method="post">
DVID server (e.g., emdata1:80): <input type="text" id="dvidserver" value="DEFAULT"><br>
DVID uuid: <input type="text" id="uuid"><br>
Name of label space: <input type="text" id="labelname"><br>
Bounding box coordinate 1 (e.g., "x,y,z"): <input type="text" id="bbox1"><br>
Bounding box coordinate 2 (e.g., "x,y,z"): <input type="text" id="bbox2"><br>
Classifier name (stored on DVID at classifiers/): <input type="text" id="classifier"><br>
Agglomeration classifier name (stored on DVID at classifiers/): <input type="text" id="agglomclassifier"><br>
Algorithm name: <select id="algorithm"><option value="compute-graph" selected="selected">Compute Graph</option><option value="segment">Segment</option></select><br>
<input type="submit" value="Submit"/>
</form>

<hr>
<br>
<div id="status"></div><br>
</div>

<script>
    $("#calclabels").submit(function(event) {                                                           
      event.preventDefault();
      $('#status').html("")

      $.ajax({
        type: "POST",
        url: "/formhandler/",
        data: {uuid: $('#uuid').val(), bbox1: $('#bbox1').val(), bbox2: $('#bbox2').val(), classifier: $('#classifier').val(), agglomclassifier: $('#agglomclassifier').val(), labelname: $('#labelname').val(), dvidserver: $('#dvidserver').val(), algorithm: $('#algorithm').val()},
        success: function(data){
            var result_location = data["result-callback"];
            $('#status').html("Location of result on DVID: " + result_location)
        },

        error: function(msg) {
                $('#status').html("Error Processing Results: " + msg.responseText)
          }
        });
    });                                                                                                  
</script>                                                                                                
</html>                                  
`
