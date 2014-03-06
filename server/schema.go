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
      "type": "integer"
      "default": 40
    },
    "classifier": {
      "description": "Name of pixel classifier",
      "type": "string"
    },
    "label-engine": {
      "description": "Type of algorithm used (currently not used)",
      "enum": [ "simp-watershed" ],
      "default": "simp-watershed"
    }
  },
  "required" : ["label-name", "uuid", "bbox1", "bbox2", "classifier", "label-engine"]
}
`
