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
It should be installed on a compute
cluster.  This package calls [Gala](https://github.com/janelia-flyem/gala) to generate a watershed and [NeuroProof](https://github.com/janelia-flyem/neuroproof)
to generate the graph.  (Please note that if one uses buildem to install gala and NeuroProof, the python in buildem could be used to install CalcLabelOrchestration.
The python requests package and drmaa package must be installed.)

## Workflow

This cluster service takes a REST request for labeling an ROI of grayscale stored in DVID.  It will
invoke an orchestrating script on a compute cluster which will run a watershed algorithm (Gala) on
overlapping chunks of the ROI.  These chunks are stitched and the labels are written back to DVID.
Ideally, this service should be able to call any watershed or labeling service.  Currently, the invocation
to Gala is hard-coded in the orchestrating script.

If the service is running on a machine that does not contain an installation of the orchestrator (e.g.,
when the service is not running on the compute cluster), a simple configuration
file should be provided to specify the location of the orchestrating script and the cluster environment.

##TODO

* Create a [buildem](https://github.com/janelia-flyem/buildem) installation for gala, Ilastik, and CalcLabelOrchestration
* Improve stitcher to handle corner cases in branching (just set a hard threshold)
* Change overlap boundaries for stitching
* Generic REST interfaces for watershed generation
