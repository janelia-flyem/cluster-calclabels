import argparse
import h5py
import numpy
import struct
import json
import requests

# compute overlap -- assume first point is less than second
def intersects(pt1, pt2, pt1_2, pt2_2):
    if pt1 > pt2:
        raise Exception("point 1 greater than point 2")
    if pt1_2 > pt2_2:
        raise Exception("point 1 greater than point 2")

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
    labels2= numpy.array(hfile2['stack'][offz1_2:offz2_2, offy1_2:offy2_2, offx1_2:offx2_2])

    # determine list of bodies in play
    z2, y2, x2 = labels2.shape
    z1 = y1 = x1 = 0 
    
    if 'x' in json_data["overlap-axis"]:
        x1 = x2/2 
        x2 = x1 + 1
    if 'y' in json_data["overlap-axis"]:
        y1 = y2/2 
        y2 = y1 + 1
    if 'z' in json_data["overlap-axis"]:
        z1 = z2/2 
        z2 = z1 + 1
    eligible_bodies = set(numpy.unique(labels2[z1:z2, y1:y2, x1:x2]))
    body2body = {}

    label2_bodies = numpy.unique(labels2)
    
    #"""
    for body in label2_bodies:
        body2body[body] = {}

    # traverse volume to find maximum overlap
    for (z,y,x), body1 in numpy.ndenumerate(labels1):
        body2 = labels2[z,y,x]
        if body1 not in body2body[body2]:
            body2body[body2][body1] = 0
        body2body[body2][body1] += 1
    #"""

    already_merged = set()

    # create merge list 
    merge_list = []
    for body2, bodydict in body2body.items():
        if body2 in eligible_bodies:
            bodysave = -1
            max_val = 0
            for body1, val in bodydict.items():
                if val > max_val:
                    bodysave = body1
                    max_val = val
            already_merged.add(int(bodysave))
            merge_list.append([int(bodysave), int(body2)])
  

    eligible_bodies = set(numpy.unique(labels1[z1:z2, y1:y2, x1:x2]))
    body2body = {}
    #"""
    label1_bodies = numpy.unique(labels1)
    for body in label1_bodies:
        body2body[body] = {}

    # traverse volume to find maximum overlap
    for (z,y,x), body1 in numpy.ndenumerate(labels1):
        body2 = labels2[z,y,x]
        if body2 not in body2body[body1]:
            body2body[body1][body2] = 0
        body2body[body1][body2] += 1
    #"""
    # add to merge list 
    for body1, bodydict in body2body.items():
        if body1 in eligible_bodies and body1 not in already_merged:
            bodysave = -1
            max_val = 0
            for body2, val in bodydict.items():
                if val > max_val:
                    bodysave = body2
                    max_val = val
            merge_list.append([int(body1), int(bodysave)])



    # output json
    outjson = {}
    outjson["id"] = json_data["id"]
    outjson["merge_list"] = merge_list
  
    fout = open(json_data["output"], 'w')
    jstr = json.dumps(outjson, indent=4)
    fout.write(jstr)

