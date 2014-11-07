import os
import argparse
import sys
import json
import requests
import drmaa
import time
import numpy
import h5py

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
agglomclassifierName = "agglomclassifier.xml"
graphclassifierName = "graphclassifier.h5"
synapsesName = "synapses.json"
jsonName = "config.json"
watershedExe = "gala-watershed"
commitLabels = "commit_labels"
computeGraph = "neuroproof_graph_build_dvid"
computeProb = "neuroproof_agg_prob_dvid"
agglomerateGraph = "neuroproof_graph_predict"
stitchLabels = "stitch_labels"


# hold all options for command
class CommandOptions:
    def __init__(self, config_data, session_location):
        self.session_location = session_location
        self.classifier = session_location + "/" + classifierName
        self.agglomclassifier = session_location + "/" + agglomclassifierName
        self.graphclassifier = session_location + "/" + graphclassifierName
        self.synapses = session_location + "/" + synapsesName
        self.roi = config_data["roi"]
        self.callback = config_data["result-callback"] 
        self.job_size = config_data["job-size"]
        self.overlap_size = config_data["overlap-size"]
        self.uuid = config_data["uuid"] 
        self.dvidserver = config_data["dvid-server"] 
        self.labelname = config_data["label-name"]
        self.bbox1 = config_data["bbox1"]
        self.bbox2 = config_data["bbox2"]
        self.algorithm = config_data["algorithm"]
        self.stitch_mode = config_data["stitch-mode"]
        self.seed_size = config_data["seed-size"]
        self.agglom_threshold = config_data["agglom-threshold"]


def num_divs(total_span, substack_span, min_allowed):
    num = total_span / substack_span 
    mod = total_span % substack_span
    if num == 0 or mod >= min_allowed:
        num += 1
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
        self.num_stitch = 0
        self.synapsedata = False

    # create working directory
    def create_directory(self, basepath):
        self.session_location = basepath + "/" + str(self.substackid)
        if not os.path.exists(self.session_location):
            os.makedirs(self.session_location)

    # assume line[0] < line[1] and add border in calculation 
    def intersects(self, line1, line2):
        pt1, pt2 = line1[0], line1[1]
        #pt1 -= self.border
        #pt2 += self.border
        pt1_2, pt2_2 = line2[0], line2[1]
        #pt1_2 -= self.border
        #pt2_2 += self.border
        
        #if pt1_2 < pt2 and pt2_2 > pt1:
        if (pt1_2 < pt2 and pt1_2 >= pt1) or (pt2_2 <= pt2 and pt2_2 > pt1):
            return True
        return False 

    # returns true if two substacks overlap
    def isoverlap(self, substack2):
        linex1 = [self.roi.x1, self.roi.x2]
        linex2 = [substack2.roi.x1, substack2.roi.x2]
        liney1 = [self.roi.y1, self.roi.y2]
        liney2 = [substack2.roi.y1, substack2.roi.y2]
        linez1 = [self.roi.z1, self.roi.z2]
        linez2 = [substack2.roi.z1, substack2.roi.z2]
       
        # check intersection
        #if self.intersects(linex1, linex2) and self.intersects(liney1, liney2) and self.intersects(linez1, linez2):
        if (self.touches(linex1[0], linex1[1], linex2[0], linex2[1]) and self.intersects(liney1, liney2) and self.intersects(linez1, linez2)) or (self.touches(liney1[0], liney1[1], liney2[0], liney2[1]) and self.intersects(linex1, linex2) and self.intersects(linez1, linez2)) or (self.touches(linez1[0], linez1[1], linez2[0], linez2[1]) and self.intersects(liney1, liney2) and self.intersects(linex1, linex2)):
            return True 
       
        return False

    # handle gala output for max body id
    def set_max_id(self, id_offset):
        self.id_offset = id_offset
        data = json.load(open(self.session_location + "/max_body.json"))
        return id_offset + data["max_id"]


    # find body mappings
    def find_mappings(self, merge_list, substacks):
        if self.id_offset is None:
            raise "No offset specified"
       
        for i in range(self.num_stitch):
            jname = self.session_location + "/merge_" + str(i) + ".json"
            merge_data = json.load(open(jname))
            substack2 = substacks[merge_data["id"]]

            # first body is substack1, second body is substack2
            # put larger body id first
            for merge in merge_data["merge_list"]:
                body1 = merge[0] + self.id_offset
                body2 = merge[1] + substack2.id_offset
                if body2 > body1:
                    body1, body2 = body2, body1
                merge_list.append([body1, body2])

    # load synapse data into local file (assume data is a unique copy)
    def load_local_synapse_file(self, data):
        self.synapsedata = True
        lowerbound_in = numpy.array([self.roi.x1-self.border, self.roi.y1-self.border, self.roi.z1-self.border])
        upperbound_ex = numpy.array([self.roi.x2+self.border, self.roi.y2+self.border, self.roi.z2+self.border])
        dims = upperbound_ex - lowerbound_in

        synapse_list = []

        for synapse in data["data"]:
            foundviolation = False

            synapse["T-bar"]["location"] = list(numpy.array(synapse["T-bar"]["location"]) - lowerbound_in)

            # check if tbar is beyond bounding box
            vios1 = dims - numpy.array(synapse["T-bar"]["location"])
            vios2 = numpy.array(synapse["T-bar"]["location"])
            for iter in range(0, len(vios1)):
                if vios2[iter] < 0 or vios1[iter] <= 0:
                    foundviolation = True
                    break
            if foundviolation:
                continue
            # flip y to conform to raveler y format (which gala and neuroproof handle)
            synapse["T-bar"]["location"][1] = dims[1] - synapse["T-bar"]["location"][1] - 1

            # no constraints need to be added otherwise
            if len(synapse["partners"]) == 0:
                continue

            for partner in synapse["partners"]:
                partner["location"] = list(numpy.array(partner["location"]) - lowerbound_in)

                # check if psd is beyond bounding box
                vios1 = dims - numpy.array(partner["location"])
                vios2 = numpy.array(partner["location"])
                for iter in range(0, len(vios1)):
                    if vios2[iter] < 0 or vios1[iter] <= 0:
                        foundviolation = True
                        break
                if foundviolation:
                    break
                # flip y to conform to raveler y format (which gala and neuroproof handle)
                partner["location"][1] = dims[1] - partner["location"][1] - 1
                
            if foundviolation:
                continue

            synapse_list.append(synapse)

        new_data = {}
        new_data["data"] = synapse_list
        new_data["metadata"] = data["metadata"]

        fout = open(self.session_location + "/synapses_local.json", 'w')
        fout.write(json.dumps(new_data, indent=4))
        fout.close()

    # write out configuration json and launch job
    def launch_label_job(self, cluster_session, config):
        config["bbox1"] = [self.roi.x1, self.roi.y1, self.roi.z1]
        config["bbox2"] = [self.roi.x2, self.roi.y2, self.roi.z2]
        config["border"] = self.border

        if self.synapsedata:
            config["synapse-file"] = self.session_location + "/synapses_local.json"
        fout = open(self.session_location + "/config.json", 'w')
        fout.write(json.dumps(config, indent=4))
        fout.close()

        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = watershedExe
        jt.joinFiles = True
        jt.outputPath = ":" + self.session_location + "/watershed.out"

        # use current environment, use all slots for Ilastik
        jt.nativeSpecification = "-pe batch 4 -j y -o /dev/null -b y -cwd -V"
        jt.args = [self.session_location, "--config-file", self.session_location + "/config.json"]
        return cluster_session.runJob(jt)

    def touches(self, p1, p2, p1_2, p2_2):
        if p1 == p2_2 or p2 == p1_2:
            return True
        return False
    
    # launch substack stitch command
    def launch_stitch_job(self, substack2, cluster_session, options):
        config = {}
        config["bbox1"] = [self.roi.x1-self.border, self.roi.y1-self.border, self.roi.z1-self.border]
        config["bbox2"] = [self.roi.x2+self.border, self.roi.y2+self.border, self.roi.z2+self.border]

        config["bbox1_2"] = [substack2.roi.x1-self.border, substack2.roi.y1-self.border, substack2.roi.z1-self.border]
        config["bbox2_2"] = [substack2.roi.x2+self.border, substack2.roi.y2+self.border, substack2.roi.z2+self.border]

        config["labels"] = self.session_location + "/segmentation.h5"
        config["labels_2"] = substack2.session_location + "/segmentation.h5"
        
        configname = self.session_location + "/config_stitch" + str(self.num_stitch) + ".json"
        config["output"] = self.session_location + "/merge_" + str(self.num_stitch) + ".json"
        config["id"] = substack2.substackid

        config["stitching-mode"] = options.stitch_mode

        # axis where substacks touch, across which bodies need to be examined 
        axis = ""
        if self.touches(self.roi.x1, self.roi.x2, substack2.roi.x1, substack2.roi.x2):
            axis += "x"
        if self.touches(self.roi.y1, self.roi.y2, substack2.roi.y1, substack2.roi.y2):
            axis += "y"
        if self.touches(self.roi.z1, self.roi.z2, substack2.roi.z1, substack2.roi.z2):
            axis += "z"

        config["overlap-axis"] = axis

        self.num_stitch += 1

        fout = open(configname, 'w')
        fout.write(json.dumps(config, indent=4))
        fout.close()

        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = stitchLabels 
        jt.joinFiles = True
        jt.outputPath = ":" + self.session_location + "/stitch_" + str(self.num_stitch - 1) + ".out"

        # use current environment, need only one slot
        jt.nativeSpecification = "-pe batch 1 -j y -o /dev/null -b y -cwd -V"
        jt.args = [configname]
        return cluster_session.runJob(jt)


    def launch_compute_graph(self, cluster_session, options, graphname, labelvolname, docomputeprob):
        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = computeGraph 
        jt.joinFiles = True
        jt.outputPath = ":" + self.session_location + "/computegraph.out"
        # use current environment, need only one slot
        jt.nativeSpecification = "-pe batch 2 -j y -o /dev/null -b y -cwd -V"
        args = ["--dvid-server", options.dvidserver, "--uuid", options.uuid, "--label-name", labelvolname, "--graph-name", graphname, "--x", str(self.roi.x1), "--y", str(self.roi.y1), "--z", str(self.roi.z1), "--xsize", str(self.roi.x2-self.roi.x1), "--ysize", str(self.roi.y2-self.roi.y1), "--zsize", str(self.roi.z2-self.roi.z1)]

        if docomputeprob:
            # program will handle the fact that there is a uniform buffer in the prediction file
            args.append("--prediction-file")
            args.append(self.session_location + "/STACKED_prediction.h5")

            # for debugging purposes only
            args.append("--classifier-file")
            args.append(options.graphclassifier)
            args.append("--dumpgraph")
            args.append("1")

        jt.args = args
        return cluster_session.runJob(jt)

    # calculate the probability for every edge in the graph
    def launch_compute_probs(self, cluster_session, options, vertices, graphname):
        # write out json for vertices
        vertex_list = []
        for vertex in vertices:
            vertex_list.append(vertex["Id"])
        json_data = {}
        json_data["body-list"] = vertex_list
        fout = open(self.session_location + "/body_list.json", 'w')
        fout.write(json.dumps(json_data, indent=4))
        fout.close()

        # find number of channels
        h5pred = h5py.File(self.session_location + "/STACKED_prediction.h5")
        x,y,z,num_chans = h5pred["volume/predictions"].shape

        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = computeProb
        jt.joinFiles = True
        jt.outputPath = ":" + self.session_location + "/computeprob.out"
        # use current environment, need only one slot
        jt.nativeSpecification = "-pe batch 1 -j y -o /dev/null -b y -cwd -V"
        jt.args = ["--dvid-server", options.dvidserver, "--uuid", options.uuid, "--bodylist-name", self.session_location + "/body_list.json", "--graph-name", graphname, "--classifier-file", options.graphclassifier, "--num-chans", str(num_chans), "--dumpfile", "1"]
        print jt.args

        return cluster_session.runJob(jt)

    def launch_agglomerate(self, cluster_session, options):
        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = agglomerateGraph 
        jt.joinFiles = True
        jt.outputPath = ":" + self.session_location + "/agglomerate.out"
        # use current environment, need only one slot
        jt.nativeSpecification = "-pe batch 2 -j y -o /dev/null -b y -cwd -V"
        args = [self.session_location + "/supervoxels.h5", self.session_location + "/STACKED_prediction.h5", options.agglomclassifier, "--output-file", self.session_location + "/segmentation.h5", "--threshold", str(options.agglom_threshold)]
       
        # add synapse file if it exists
        if self.synapsedata:
            args.append("--synapse-file")
            args.append(self.session_location + "/synapses_local.json")
        jt.args = args

        #print jt.args
        return cluster_session.runJob(jt)


    def launch_write_job(self, cluster_session, config):
        config["offset"] = self.id_offset
        config["bbox1"] = [self.roi.x1, self.roi.y1, self.roi.z1]
        config["bbox2"] = [self.roi.x2, self.roi.y2, self.roi.z2]
        config["border"] = self.border
        config["labels"] = self.session_location + "/segmentation.h5"
        fout = open(self.session_location + "/configw.json", 'w')
        fout.write(json.dumps(config, indent=4))
        fout.close()

        # launch job on cluster
        jt = cluster_session.createJobTemplate()
        jt.remoteCommand = commitLabels 
        jt.joinFiles = True
        jt.outputPath = ":" + self.session_location + "/commit.out"

        # use current environment, need only one slot
        jt.nativeSpecification = "-pe batch 1 -j y -o /dev/null -b y -cwd -V"
        jt.args = [self.session_location + "/configw.json"]
        return cluster_session.runJob(jt)

# handles messages with the outside world
class Message:
    def __init__(self, url):
        self.url = url
        self.messagestr = ""

    def write_status(self, current_message=""):
        wrapped_message = "<html>"
        wrapped_message += self.messagestr
        wrapped_message += "<br>"
        if current_message != "":
            wrapped_message += "<b>Current Status: " + current_message + "</b>"
        wrapped_message += "</html>"
        requests.post(self.url, data=wrapped_message,
                headers={'content-type': 'text/html'})

def wait_for_jobs(cluster_session, job_ids, message, job_desc):
    cluster_session.synchronize(job_ids, drmaa.Session.TIMEOUT_WAIT_FOREVER, True)
    return
    
    completed = set()
    running = set()
    not_completed = set(job_ids)
    job_times = {}
    for job_id in job_ids:
        job_times[job_id] = 0

    start_time = time.time()

    while len(not_completed) > 0:
        time.sleep(1)
        for job_id in job_ids:
            status = cluster_session.jobStatus(job_id)
            if status == drmaa.JobState.DONE or status == drmaa.JobState.FAILED:
                running.add(job_id)
                completed.add(job_id)
                if job_id in not_completed:
                    not_completed.remove(job_id)
            elif status == drmaa.JobState.RUNNING:
                job_times[job_id] += 1
                running.add(job_id)
        current_message = "<b>Job description: " + job_desc + "</b><br>"
        total_time = time.time() - start_time
        current_message += "Job execution time: " + str(total_time) + " seconds" + "<br>"
        current_message += "Num waiting: " + str(len(job_ids) - len(running)) + "<br>"
        current_message += "Num running: " + str(len(running)-len(completed)) + "<br>"
        current_message += "Num completed: " + str(len(completed)) + "<br>"
        message.write_status(current_message)
    
    message.messagestr += "<b>Job description: " + job_desc + "</b><br>"
    total_time = time.time() - start_time
    message.messagestr += "Job execution time: " + str(total_time) + " seconds" + "<br>"
    message.messagestr += "Num completed jobs: " + str(len(completed)) + "<br>"
    total_val = 0
    for jobid, val in job_times.items():
        total_val += val
    message.messagestr += "Average runtime per job: " + str(total_val/len(completed)) + " seconds<br>"
    message.write_status()

def orchestrate_labeling(options, message):
    start_time = time.time()
    json_header = {'content-type': 'text/html'}

    # write status 'started' to DVID
    message.write_status("starting job")

    # make sure substacks are not smaller than this size in one dimension
    smallest_allowed = 50
    if options.overlap_size > smallest_allowed:
        smallest_allowed = options.overlap_size
    
    substacks = []
    substackid = 0

    if options.roi == "":
        # find extents of stack -- [min, max)
        x1, x2 = options.bbox1[0], options.bbox2[0]
        if x1 > x2:
            x1, x2 = x2, x1
        y1, y2 = options.bbox1[1], options.bbox2[1]
        if y1 > y2:
            y1, y2 = y2, y1
        z1, z2 = options.bbox1[2], options.bbox2[2]
        if z1 > z2:
            z1, z2 = z2, z1

        xspan = x2 - x1
        yspan = y2 - y1
        zspan = z2 - z1 

        # divide into substacks, create custom jsons/directories
        xnum = num_divs(xspan, options.job_size, smallest_allowed)   
        ynum = num_divs(yspan, options.job_size, smallest_allowed)   
        znum = num_divs(zspan, options.job_size, smallest_allowed)   


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
    else:
        # grab roi
        # !! manual ROIs (substack lists) can be stored in a keyvalue "roi" with key "partition"
        # ?! pass only real ROIs to Ilastik, so gray can be extracted -- add "?roi=blah" in gala/ilastik datasrc
        # ?! job size should be multiples of 32 if using ROI, unless ROI specifies its own partition size
        r = requests.get(options.dvidserver + "/api/node/" + options.uuid + "/" + options.roi + "/partition?batchsize=" + str(options.job_size/32)) 
        substack_data = r.json()
        for subvolume in substack_data["Subvolumes"]:
            substack = subvolume["MinPoint"]
            roi = None
            if "Sizes" in subvolume:
                sizes = subvolume["Sizes"]
                roi = Bbox(substack[0], substack[1], substack[2],
                        substack[0] + sizes[0], substack[1] + sizes[1],
                        substack[2] + sizes[2])  
            else:
                roi = Bbox(substack[0], substack[1], substack[2],
                        substack[0] + options.job_size, substack[1] + options.job_size,
                        substack[2] + options.job_size)  
            substacks.append(Substack(substackid, roi, options.overlap_size/2))
            substackid += 1
            #if substackid == 4:
            #    break

    # create drmaa session (wait for all jobs to finish for now)
    cluster_session = drmaa.Session()
    cluster_session.initialize()
  
    if options.algorithm == "segment":
        # read synapse file and catch error if not there
        synapseread = False
        try:
            # create synapse assignments if available
            for substack in substacks:
                fin = open(options.synapses)
                data = json.load(fin)
                synapseread = True
                # create local file and set synapse variable for relevant subsequent actions
                substack.create_directory(options.session_location)
                substack.load_local_synapse_file(data)
        except Exception, e:
            pass
        
        # load default config
        config = {}
        # assume datatype is called "grayscale"
        config["datasrc"] = options.dvidserver + "/api/node/" + options.uuid + "/grayscale" 
        config["classifier"] = options.classifier
        config["seed-size"] = options.seed_size

        # ?! cannot enable ROI fetch until I figure out how to zero out watershed
        #if options.roi != "":
        #    config["roi"] = options.roi

        job_ids = []
        job_num1 = 0
        for substack in substacks:
            if not synapseread:
                substack.create_directory(options.session_location)
            # spawn cluster job -- return handler?
            job_num1 += 1
            job_ids.append(substack.launch_label_job(cluster_session, config))
            
            if len(job_ids) == 100: 
                wait_for_jobs(cluster_session, job_ids, message, "watershed: " + str(job_num1) + " of " + str(len(substacks)))
                job_ids = []
            
            # throttling now supported
            #time.sleep(3) # will be handled by throttled command shortly making this moot

        # wait for job completion
        if len(job_ids) > 0: 
            wait_for_jobs(cluster_session, job_ids, message, "watershed")

        # write status: 'performed watershed'
        message.write_status("generated initial labels") 
   
        # launch neuroproof segmentation jobs
        job_ids = []
        for substack in substacks:
            # spawn cluster job
            job_ids.append(substack.launch_agglomerate(cluster_session, options))

        # wait for job completion
        wait_for_jobs(cluster_session, job_ids, message, "agglomerate")

        # write status: 'performed watershed'
        message.write_status("performed agglomeration") 

        # launch reduce jobs and wait
        job_ids = []
        
        for i in range(0, len(substacks)-1):
            for j in range(i+1, len(substacks)):
                if substacks[i].isoverlap(substacks[j]):
                    job_ids.append(substacks[i].launch_stitch_job(substacks[j], cluster_session, options))
        
        # wait for job completion
        wait_for_jobs(cluster_session, job_ids, message, "stitch")

        # write status: 'stitched watershed'
        message.write_status("stitched labels") 
    
        # collect merges, relabels, etc
        id_offset = 0
        merge_list = []
        for substack in substacks:
            id_offset = substack.set_max_id(id_offset)
        for substack in substacks:
            # find all substack labels that need to be remapped (sets the proper offset)
            # higher id first
            substack.find_mappings(merge_list, substacks) 

        # make a body2body map
        body1body2 = {}
        body2body1 = {}
        for merger in merge_list:
            # body1 -> body2
            body1 = merger[0]
            if merger[0] in body1body2:
                body1 = body1body2[merger[0]]
            body2 = merger[1]
            if merger[1] in body1body2:
                body2 = body1body2[merger[1]]

            if body2 not in body2body1:
                body2body1[body2] = set()
            
            # add body1 to body2 map
            body2body1[body2].add(body1)
            # add body1 -> body2 mapping
            body1body2[body1] = body2

            if body1 in body2body1:
                for tbody in body2body1[body1]:
                    body2body1[body2].add(tbody)
                    body1body2[tbody] = body2
        
        body2body = zip(body1body2.keys(), body1body2.values())

        # create label name type
        dataset_name = options.dvidserver + "/api/repo/"+ options.uuid + "/instance"
        
        req_json = {}
        req_json["typename"] = "labels64"
        req_json["dataname"] = options.labelname
        req_str = json.dumps(req_json)
        requests.post(dataset_name, data=req_str, headers=json_header) 
        
        # launch relabel and write jobs and wait 
        config = {}
        config["remap"] = body2body 
        config["write-location"] = options.dvidserver + "/api/node/" + options.uuid + "/" + options.labelname + "/raw/0_1_2"
        # ?! roi working but disabled
        #config["roi"] = options.roi
        config["roi"] = "" # options.roi

        job_ids = []
        job_num = 0
        for substack in substacks:
            # spawn cluster job
            # ?! modify commit label script to not handle ROI if provided just an endpoint ??
            job_num += 1
            job_ids.append(substack.launch_write_job(cluster_session, config))
            if len(job_ids) == 10: 
                wait_for_jobs(cluster_session, job_ids, message, "write-labels: " + str(job_num) + " of " + str(len(substacks)))
                job_ids = []

        if len(job_ids) > 0: 
            wait_for_jobs(cluster_session, job_ids, message, "write-labels: " + str(job_num) + " of " + str(len(substacks)))
        
        # wait for job completion
        #wait_for_jobs(cluster_session, job_ids, message, "write-labels")
        # write status: 'stitched watershed'
        message.write_status("wrote labels") 
    else:
        for substack in substacks:
            substack.create_directory(options.session_location)

    # always compute graph
    # create graph type
    graphname = options.labelname 
    labelvolname = "bodies"
    doprediction = False
    if options.algorithm == "segment":
        graphname = graphname + "graph"
        labelvolname = options.labelname 
        try:
            fin = open(options.graphclassifier)
            doprediction = True
        except Exception, e:
            try:
                fin = open(options.agglomclassifier)
                doprediction = True
                options.graphclassifier = options.session_location + "/" + agglomclassifierName
            except Exception, e:
                pass

    dataset_name = options.dvidserver + "/api/repo/"+ options.uuid + "/instance"
    req_json = {}
    req_json["typename"] = "labelgraph"
    req_json["dataname"] = graphname
    req_str = json.dumps(req_json)
    requests.post(dataset_name, data=req_str, headers=json_header) 
   
    job_ids = []
    for substack in substacks:
        # spawn cluster job
        job_ids.append(substack.launch_compute_graph(cluster_session, options, graphname, labelvolname, doprediction))

    # wait for job completion
    wait_for_jobs(cluster_session, job_ids, message, "compute-graph")

    # only compute probs if this was a segmentation run
    if doprediction:
        # handle prob calc (grab entire graph, create body lists, parse number of channels from ILP)
        r = requests.get(options.dvidserver + "/api/node/" + options.uuid + "/" + graphname + "/subgraph") 
        complete_graph = r.json()
       
        # find channels from ILP

        # retrieve all vertices
        import random
        vertices = complete_graph["Vertices"]
        random.shuffle(vertices)
        incr = len(vertices) / len(substacks) + 1
        start = 0

        job_ids = []
        for substack in substacks:
            # choose random set of vertices
            # spawn cluster job for computing probs
            job_ids.append(substack.launch_compute_probs(cluster_session, options, vertices[start:start+incr], graphname))
            #time.sleep(10) # not sure why I need to delay this but it is taking a long time so there must be a lot of contention 
            start += incr
            if len(job_ids) == 10:
                wait_for_jobs(cluster_session, job_ids, message, "compute-prob")
                job_ids = []

        if len(job_ids) > 0: 
            # wait for job completion
            wait_for_jobs(cluster_session, job_ids, message, "compute-prob")


    # calculate time
    total_time = time.time() - start_time

    # write status: 'finished'
    message.write_status("<b>Successfully finished in " + str(total_time) + " seconds</b>") 
    cluster_session.exit()
   
#parses information in config json, assume classifier and json location given directory
def execute(args):
    parser = argparse.ArgumentParser(description="Orchestrate map/reduce-like segmentation jobs")
    parser.add_argument('session_location', type=str, help="Location of directory that contains classifier and configuration json")
    args = parser.parse_args()


    config_data = json.load(open(args.session_location + "/" + jsonName))
     
    options = CommandOptions(config_data, args.session_location)
    message = Message(options.callback)
    try:
        orchestrate_labeling(options, message)
    except Exception, e:
        print e
        message.write_status("FAIL: " + str(e))

