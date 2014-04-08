import argparse
import h5py
import numpy
import struct
import json
import requests

# compute overlap -- assume first point is less than second
def intersects(pt1, pt2, pt1_2, pt2_2):
    if pt2 > pt1:
        raise Exception("point 2 greater than point 1")
    if pt2_2 > pt1_1:
        raise Exception("point 2 greater than point 1")

    val1 = max(pt1, pt1_2)
    val2 = min(pt2, pt2_2)
    size = val2-val1
    npt1 = val1 - pt1 
    npt1_2 = val1 - pt1_2

    return npt1, npt1+size, npt1_2, npt1_2+size

def execute(argv):
    parser = argparse.ArgumentParser(description="Analyzed overlapping label volumes and writes merge list")
    parser.add_argument('config_file', type=str, help="Location of configuration json")
    args = parser.parse_args()

    json_data = json.load(open(args.config_file))

    bbx1, bby1, bbz1 = json_data["bbox1"]
    bbx2, bby2, bbz2 = json_data["bbox2"]

    bbx1_2, bby1_2, bbz1_2 = json_data["bbox1_2"]
    bbx2_2, bby2_2, bbz2_2 = json_data["bbox2_2"]

    hfile = h5py.File(json_data["labels"], 'r')
    hfile2 = h5py.File(json_data["labels_2"], 'r')

    # crop two volumes to overlap
    offx1, offx2, offx1_2, offx2_2 = intersects(bbx1, bbx2, bbx1_2, bbx2_2)
    offy1, offy2, offy1_2, offy2_2 = intersects(bby1, bby2, bby1_2, bby2_2)
    offz1, offz2, offz1_2, offz2_2 = intersects(bbz1, bbz2, bbz1_2, bbz2_2)
        
    labels1 = numpy.array(hfile['stack'][offz1:offz2, offy1:offy2, offx1:offx2])
    labels2= numpy.array(hfile2['stack'][offz1:offz2_2, offy1:offy2_2, offx1:offx2_2])

    # determine list of bodies in play
    z2, y2, x2 = labels2.shape()
    z1 = y1 = x1 = 0 
    
    if 'x' in json_data["overlap-axis"]:
        x1 /= 2 
        x2 = x1 + 1
    if 'y' in json_data["overlap-axis"]:
        y1 /= 2 
        y2 = y1 + 1
    if 'z' in json_data["overlap-axis"]:
        z1 /= 2 
        z2 = z1 + 1
    eligible_bodies = set(numpy.unique(labels2[z1:z2, y1:y2, x1:x2]))
    body2body1 = {}
    body2body2 = {}

    label1_bodies = numpy.unique(labels1)
    label2_bodies = numpy.unique(labels1)
    
    for body in label2_bodies:
        body2body[body] = {}

    # traverse volume to find maximum overlap
    for (z,y,x), body1 in numpy.ndenumerate(labels1):
        body2 = labels2[z,y,z]
        body2body[body2][body1] += 1

    # create merge list 
    merge_list = []
    for body2, bodydict in body2body:
        if body2 in eligible_bodies:
            bodysave = -1
            max_val = 0
            for body1, val in bodydict:
                if val > max_val:
                    bodysave = body1
                    max_val = val
            merge_list.append([bodysave, body2])
   
    # output json
    outjson = {}
    outjson["id"] = json_data["id"]
    outjson["merge_list"] = merge_list 
    
    fout = open(json_data["output"], 'w')
    fout.write(json.dumps(outjson, indent=4))

