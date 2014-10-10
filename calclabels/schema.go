package calclabels

// String representing the JSON schema for the service call
const calclabelsSchema = `
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
`
