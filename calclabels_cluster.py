import os
import argparse
import sys
import json
import requests
import drmaa
import time

"""
Basic Algorithm

0. Divide volume into overlapping partitions to be handled by different compute nodes
1. map: overlapping partitions of grayscale to gala-pixel, write label volume with overlap and max ID
2. emit: overlapping boundaries, coordinates
3. reduce: find merges between boundaries, produce merge list (optionally produce list of possible mergers for later evaluation)
4. create offset for each substack for relabeling, quick concat list of merges using offset in cluster script (optionally concat list of possible mergers for debugging)
5. map: label volume with offset and global merge list, write relabeled/merged label volume to DVID and delete local data
6. erase all meta data except for debug files

"""

# assumed constants
classifierName = "classifier.ilp"
jsonName = "config.json"
watershedExe = "gala-watershed"
commitLabels = "commit_labels"


# hold all options for command
class CommandOptions:
    def __init__(self, config_data, session_location):
        self.session_location = session_location
        self.classifier = session_location + "/" + classifierName
        self.callback = config_data["result-callback"] 
        self.job_size = config_data["job-size"]
        self.overlap_size = config_data["overlap-size"]
        self.uuid = config_data["uuid"] 
        self.dvidserver = config_data["dvid-server"] 
        self.labelname = config_data["label-name"]
        self.bbox1 = config_data["bbox1"]
        self.bbox2 = config_data["bbox2"]

def num_divs(total_span, substack_span, min_allowed):
    num = total_span / substack_span 
    mod = total_span % substack_span
    if mod != 0:
        num += 1
    if mod < min_allowed:
        num -= 1
    return num

class Bbox:
    def __init__(self, x1, y1, z1, x2, y2, z2):
        self.x1 = x1
        self.y1 = y1
        self.z1 = z1
        self.x2 = x2
        self.y2 = y2
        self.z2 = z2

class Substack:
    def __init__(self, substackid, main_region, boundbuffer):
        self.roi = main_region
        self.border = boundbuffer
        self.substackid = substackid

    # create working directory
    def create_directory(self, basepath):
        self.session_location = basepath + "/" + str(self.substackid)
        if not os.path.exists(self.session_location):
            os.makedirs(self.session_location)

    # write out configuration json and launch job
    def launch_label_job(self, cluster_session, config):
        config["bbox1"] = [self.roi.x1, self.roi.y1, self.roi.z1]
        config["bbox2"] = [self.roi.x2, self.roi.y2, self.roi.z2]
        config["border"] = self.border
        fout = open(self.session_location + "/config.json", 'w')
        fout.write(json.dumps(config, indent=4))
        fout.close()
        
        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = watershedExe

        # use current environment, use all slots for Ilastik
        jt.nativeSpecification = "-pe batch 16 -j y -o /dev/null -b y -cwd -V"
        jt.args = [self.session_location, "--config-file", self.session_location + "/config.json"]
        return cluster_session.runJob(jt)

    def launch_write_job(self, cluster_session, config):
        config["bbox1"] = [self.roi.x1, self.roi.y1, self.roi.z1]
        config["bbox2"] = [self.roi.x2, self.roi.y2, self.roi.z2]
        config["border"] = self.border
        config["labels"] = self.session_location + "/supervoxels.h5"
        fout = open(self.session_location + "/configw.json", 'w')
        fout.write(json.dumps(config, indent=4))
        fout.close()

        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = commitLabels 

        # use current environment, need only one slot
        jt.nativeSpecification = "-pe batch 1 -j y -o /dev/null -b y -cwd -V"
        jt.args = [self.session_location + "/configw.json"]
        return cluster_session.runJob(jt)




def orchestrate_labeling(options):
    json_header = {'content-type': 'application/json'}  
    
    # write status 'started' to DVID
    requests.post(options.callback, data='{"status": "started"}', headers=json_header)

    # make sure substacks are not smaller than this size in one dimension
    smallest_allowed = 50
    if options.overlap_size > smallest_allowed:
        smallest_allowed = options.overlap_size

    # find extents of stack (inclusive)
    x1, x2 = options.bbox1[0], options.bbox2[0]
    if x1 > x2:
        x1, x2 = x2, x1
    y1, y2 = options.bbox1[1], options.bbox2[1]
    if y1 > y2:
        y1, y2 = y2, y1
    z1, z2 = options.bbox1[2], options.bbox2[2]
    if z1 > z2:
        z1, z2 = z2, z1

    xspan = x2 - x1 + 1
    yspan = y2 - y1 + 1
    zspan = z2 - z1 + 1 

    # divide into substacks, create custom jsons/directories
    xnum = num_divs(xspan, options.job_size, smallest_allowed)   
    ynum = num_divs(yspan, options.job_size, smallest_allowed)   
    znum = num_divs(zspan, options.job_size, smallest_allowed)   

    substacks = []
    substackid = 0

    # load substacks -- do check for max-1
    for x in range(0, xnum):
        for y in range(0, ynum):
            for z in range(0, znum):
                startx = x * options.job_size + x1
                finishx = (x+1) * options.job_size + x1
                if x == (xnum-1):
                    finishx = x2
                
                starty = y * options.job_size + y1
                finishy = (y+1) * options.job_size + y1
                if y == (ynum-1):
                    finishy = y2
                
                startz = z * options.job_size + z1
                finishz = (z+1) * options.job_size + z1
                if z == (znum-1):
                    finishz = z2

                roi = Bbox(startx, starty, startz, finishx, finishy, finishz)  
                substacks.append(Substack(substackid, roi, options.overlap_size/2))
                substackid += 1 

    # load default config
    config = {}
    # assume datatype is called "grayscale"
    config["datasrc"] = options.dvidserver + "/api/node/" + options.uuid + "/grayscale" 
    config["classifier"] = options.classifier

    # create drmaa session (wait for all jobs to finish for now)
    cluster_session = drmaa.Session()
    cluster_session.initialize()
    job_ids = []
    for substack in substacks:
        substack.create_directory(options.session_location)
        # spawn cluster job -- return handler?
        job_ids.append(substack.launch_label_job(cluster_session, config))
        time.sleep(1)

    # wait for job completion
    cluster_session.synchronize(job_ids, drmaa.Session.TIMEOUT_WAIT_FOREVER, True)
   
    # write status: 'performed watershed'
    requests.post(options.callback, data='{"status": "generated initial labels"}', headers=json_header)
    
    #?! launch reduce jobs and wait
    
    # write status: 'stitched watershed'
    requests.post(options.callback, data='{"status": "stitched labels"}', headers=json_header)
    
    #?! collect merges, relabels, etc
    
    
    # create label name type
    dataset_name = options.dvidserver + "/api/dataset/"+ options.uuid + "/new/labels64/" + options.labelname
    requests.post(dataset_name, data='{}', headers=json_header) 
    
    # launch relabel and write jobs and wait 
    config = {}
    config["offset"] = 0 # ?! set accordingly
    config["remap"] = "" # ?! set accordingly
    config["write-location"] = options.dvidserver + "/api/node/" + options.uuid + "/" + options.labelname + "/raw/0_1_2"

    job_ids = []
    for substack in substacks:
        substack.create_directory(options.session_location)
        # spawn cluster job
        job_ids.append(substack.launch_write_job(cluster_session, config))
        time.sleep(1)

    # wait for job completion
    cluster_session.synchronize(job_ids, drmaa.Session.TIMEOUT_WAIT_FOREVER, True)

    # write status: 'finished'
    requests.post(options.callback, data='{"status": "finished"}', headers=json_header)
    cluster_session.exit()
   
#parses information in config json, assume classifier and json location given directory
def main(args):
    parser = argparse.ArgumentParser(description="Orchestrate map/reduce-like segmentation jobs")
    parser.add_argument('session_location', type=str, help="Location of directory that contains classifier and configuration json")
    args = parser.parse_args()

    config_data = json.load(open(args.session_location + "/" + jsonName))
     
    options = CommandOptions(config_data, args.session_location)
    orchestrate_labeling(options) 

if __name__ == "__main__":
    sys.exit(main(sys.argv))

