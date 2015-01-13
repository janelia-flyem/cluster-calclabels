import argparse
import h5py
import numpy
import struct
import json
import requests
import time

def execute(argv):
    parser = argparse.ArgumentParser(description="Remaps h5")
    parser.add_argument('config_file', type=str, help="Location of configuration json")
    args = parser.parse_args()

    json_data = json.load(open(args.config_file))
    json_data2 = json.load(open(json_data["remapjson"]))

    hfile = h5py.File(json_data["labels"], 'r')
    labels = numpy.array(hfile['stack']).astype(numpy.uint64)

    # crop labels
    bufsz = json_data["border"]
    labels = labels[bufsz:-1*bufsz, bufsz:-1*bufsz, bufsz:-1*bufsz]

    roi = json_data["roi"]

    # remapping is based off of an adjusted set of labels (if necessary)
    if json_data["offset"] != 0:
        labels = labels + json_data["offset"]
        # make sure 0 is 0
        labels[labels == json_data["offset"]] = 0

    # json mapping should exist
    if len(json_data2["remap"]) != 0:
        mapping_col = numpy.unique(labels)
        label_mappings = dict(zip(mapping_col, mapping_col))
        
        remap_data = json_data2["remap"]
        for mapping in remap_data:
            label_mappings[mapping[0]] = mapping[1]

        vectorized_relabel = numpy.frompyfunc(label_mappings.__getitem__, 1, 1)
        labels = vectorized_relabel(labels).astype(numpy.uint32)
   
    fout = h5py.File(json_data["labelsout"], 'a')
    fout.create_dataset("stack", data=labels)
    fout.close()

