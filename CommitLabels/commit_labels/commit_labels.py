import argparse
import h5py
import numpy
import struct

def execute(argv):
    parser = ArgumentParser.argparse(description="Writes h5 to DVID")
    parser.add_argument('config_file', type=str, help="Location of configuration json")
    args = parser.parse_args()

    json_data = json.load(open(args.config_file))

    hfile = h5py.File(json_data["labels.h5"], 'r')
    labels = numpy.array(hfile['stack']).astype(numpy.uint64)

    # remapping is based off of an adjusted set of labels (if necessary)
    if json_data["offset"] != 0:
        labels = labels + json_data["offset"]
    
    # json mapping should exist
    if json_data["remap"] != "":
        mapping_col = labels.unique()
        label_mappings = dict(zip(mapping_col, mapping_col))
        
        remap_data = json.load(open(json_data["remap"]))
        for mapping in remap_data["mappings"]:
            label_mappings[mapping[0]] = mapping[1]
    
        vectorized_relabel = numpy.frompyfunc(label_mappings.__getitem__, 1, 1)
        labels = vectorized_relabel(labels).astype(numpy.uint64)

    # write dvid volume
    # <server>/api/node/<UUID>/<labelname>/0_1_2/
    write_location = json_data["write-location"] 
    bbox1 = json_data["bbox1"]
    bbox2 = json_data["bbox2"]

    sizes = [i - j for i, j in zip(bbox2, bbox1)]
    write_location += "{sx}_{sy}_{sz}/{x}_{y}_{z}".format(sx=sizes[0],
            sy=sizes[1], sz=sizes[2], x=bbox1[0], y=bbox1[1], z=bbox1[2])

    labels_data = '<' + 'Q'*len(labels)
    labels_bin = struct.pack(labels_data, *labels)
    requests.post(write_location, data=labels_bin,
            headers={'content-type': 'application/octet-stream'}) 

