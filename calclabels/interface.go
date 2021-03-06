package calclabels

// String representing interface for adder example
const ramlInterface = `#%%RAML 0.8
title: Cluster Calculate Labels
/calculation:
  post:
    description: "Call service to calculate labels over a region on the cluster"
    body:
      application/json:
        schema: |
          { "$schema": "http://json-schema.org/schema#",
            "title": "Provide region where labels will be computed",
            "type": "object",
            "properties": {
              "dvid-server": { 
                "description": "location of DVID server (will try to find on service proxy if not provided)",
                "type": "string" 
              },
              "label-name": {
                "description": "name of the datatype to store the labels",
                "type": "string" 
              },
              "uuid": { "type": "string" },
              "bbox1": {
                "description": "Bottom-left bounding box coordinate (3D coordinates only for now)",
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {"type": "integer"}
              },
              "bbox2": {
                "description": "Top-right bounding box coordinate (3D coordinates only for now)",
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {"type": "integer"}
              },
              "job-size": {
                "description": "Size of sub-regions used in cluster jobs (default: 500)",
                "type": "integer",
                "default": 500
              },
              "seed-size": {
                "description": "Size of the watershed seed size threshold used (default: 5)",
                "type": "integer",
                "default": 5
              },
              "agglom-threshold": {
                "description": "Threshold for agglomeration (default: 0.2)",
                "type": "number",
                "default": 0.2
              },
              "stitch-mode": {
                "description": "Aggressive for segmentation stitching (0 is no stitch, 3 is most aggressive stitching) (default: 2)",
                "type": "integer",
                "default": 2
              },
              "overlap-size": {
                "description": "Pixel overlap between adjacent sub-regions used in stitching (default: 40)",
                "type": "integer",
                "default": 40
              },
              "synapses": {
                "description": "Name of synapse file in DVID",
                "type": "string",
                "default": ""
              },
              "roi": {
                "description": "Name of roi in DVID (specified if bounds are not specified)",
                "type": "string",
                "default": ""
              },
              "classifier": {
                "description": "Name of pixel classifier",
                "type": "string",
                "default": ""
              },
              "agglomclassifier": {
                "description": "Name of agglomeration classifier",
                "type": "string",
                "default": ""
              },
              "agglomfeaturefile": {
                "description": "Name of feature text file for agglomeration",
                "type": "string",
                "default": ""
              },
              "graphclassifier": {
                "description": "Name of graph classifier to compute edge uncertainty",
                "type": "string",
                "default": ""
              },
              "algorithm": {
                "description": "Type of algorithm used",
                "enum": [ "segment", "compute-graph" ]
              }
            },
            "required" : ["label-name", "uuid", "algorithm"]
          }
    responses:
      200:
        body:
          application/json:
            schema: |
              { "$schema": "http://json-schema.org/schema#",
                "title": "Provide callback for clustered labeling status",
                "type": "object",
                "properties": {
                  "result-callback": {
                    "description" : "DVID URL to cluster label status (returns JSON)",
                    "type": "string"
                  }
                },
                "required" : ["result-callback"]
              }
/interface/interface.raml:
  get:
    description: "Get the interface for the cluster calculate label service"
    responses:
      200:
        body:
          application/raml+yaml:
`
