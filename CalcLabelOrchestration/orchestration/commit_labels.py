import argparse
import h5py
import numpy
import struct
import json
import requests
import time

def execute(argv):
    parser = argparse.ArgumentParser(description="Writes h5 to DVID")
    parser.add_argument('config_file', type=str, help="Location of configuration json")
    args = parser.parse_args()

    json_data = json.load(open(args.config_file))

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
    if len(json_data["remap"]) != 0:
        mapping_col = numpy.unique(labels)
        label_mappings = dict(zip(mapping_col, mapping_col))
        
        remap_data = json_data["remap"]
        for mapping in remap_data:
            label_mappings[mapping[0]] = mapping[1]

        vectorized_relabel = numpy.frompyfunc(label_mappings.__getitem__, 1, 1)
        labels = vectorized_relabel(labels).astype(numpy.uint64)
    
    # write dvid volume
    # <server>/api/node/<UUID>/<labelname>/raw/0_1_2/
    write_location = json_data["write-location"] 
    bbox1 = json_data["bbox1"]
    bbox2 = json_data["bbox2"]

    sizes = [i - j for i, j in zip(bbox2, bbox1)]
    write_location += "/{sx}_{sy}_{sz}/{x}_{y}_{z}".format(sx=sizes[0],
            sy=sizes[1], sz=sizes[2], x=bbox1[0], y=bbox1[1], z=bbox1[2])

    # enable throttling
    write_location += "?throttle=on"

    if roi != "":
        write_location += "&roi=" + roi

    labels = labels.ravel().copy()
    labels_data = '<' + 'Q'*len(labels)
    labels_bin = struct.pack(labels_data, *labels)
   
    rfile = args.config_file + ".response"

    completed = False
    iter1 = 0

    requests.adapters.DEFAULT_RETRIES = 100
    from requests.adapters import HTTPAdapter

    s = requests.Session()
    s.mount(write_location, HTTPAdapter(max_retries=100))

    try:
        while not completed:
            completed = True
            iter1 += 1
            r = s.post(write_location, data=labels_bin,
                    headers={'content-type': 'application/octet-stream'}) 
            
            if r.status_code == 503:
                time.sleep(1)
                completed = False
            elif r.status_code == 200: 
                fout = open(rfile, 'w')
                fout.write(str(len(labels_bin))+": success in " + str(iter1) + " tries")
            else:
                fout = open(rfile, 'w')
                fout.write(str(len(labels_bin))+": failed "+str(r.status_code))
    except Exception, e:
        fout = open(rfile, 'w')
        fout.write("Exception: "+str(e)+ " num tries" + str(iter1))
    
