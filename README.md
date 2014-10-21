# Cluster Calculate Image Label Tool [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/jfrc_grey_180x40.png)](http://www.janelia.org)

cluster-calclabels is a web service that executes segmentation/label
algorithms over large datasets.  It runs segmentation on overlaping
pieces of data and stitches those results together.  This tool is being
developed for the FlyEM project at Janelia Farm Research Campus.

##Installation 
This package includes the main executable for launching the cluster service to create
stitched label volumes hosted by [DVID](https://github.com/janelia-flyem/dvid).

To install the service:

    % go get github.com/janelia-flyem/cluster-calclabels

The orchestration for generating a large, stitched, label volume is done by the python package, CalcLabelOrchestration.
It should be installed on a machine that can access a SGE compute cluster.

CalcLabelOrchestration requires the requests and drmaa package.

## Overview

This cluster service takes a REST request for labeling an ROI of grayscale stored in DVID and producing a
graph corresponding to these labels.  (The user can also choose to generate a graph over previously computed labels.)  It will
invoke an orchestrating script on a compute cluster on overlapping chunks of the ROI.  Seperate executables for segmenting
and graph creation will be called for each chunk and stitched together accordingly and written back to DVID.

## Workflow

The cluster workflow calls the following tools which need to be accessible to each node in the cluster:

* [Gala](https://github.com/janelia-flyem/gala): calls [Ilastik](https://github.com/ilastik) for boundary prediction and performs seeded watershed (grayscale -> labels)
* [NeuroProof](https://github.com/janelia-flyem/neuroproof): performs agglomeration (labels->labels)
* CalcLabelOrchestration->stitch_labels: Subvolume stitching (labels->maps)
* CalcLabelOrchestration->commit_labels: Write stitched subvolumes to DVID
* NeuroProof: build region adjacency graph (RAG) from labels (labels->graph)
* NeuroProof: generate uncertainty between graph edges (graph->graph)

(Note: Gala should be set to the 'watershed' branch and installed using buildem.  CalcLabelOrchestration can be installed using 'python setup.py build; python setup.py install' with the buildem's python.  NeuroProof should be installed in buildem.)

## Other Details

For this script to work properly on the cluster, automatic ssh access must be available from the http server to the orchestrating script.  When launching the server,
the config json file in the main directory should be modified to point to the installed executables.  Information for each cluster run is stored in the directory
provided when starting up the server.

The graph classifier, agglomeration classifier, and pixel classifier used by this workflow should be stored in the DVID key value titled 'classifiers'.  Synapse
information should be stored in the DVID key value titled 'annotations'.

##TODO

* Regression and integration testing
* Improve web-page input validation to make tool easier to use
* Refactor backend orchestrating code (CalcLabelOrchestration) in favor of cluster workflow developed in Apache Spark
* Allow for flexible plug-n-play interface for calling different support algorithms
* Create a [buildem](https://github.com/janelia-flyem/buildem) or generic install for suite of tools
* Create google VM image for cluster installation
