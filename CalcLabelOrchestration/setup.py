#from distutils.core import setup
from setuptools import setup, find_packages



setup(name = "CalcLabelOrchestration",
    version = "1.0",
    description = "CommitLabels is a utility for writing an h5 label volume to DVID with optional parameters for remapping the labels.",
    author = "Stephen Plaza",
    author_email = 'plazas@janelia.hhmi.org',
    license = 'LICENSE.txt',
    packages = ['orchestration'],
    package_data = {},
    install_requires = [ ],
    scripts = ["bin/commit_labels", "bin/remap_labels", "bin/calclabels", "bin/calclabels_cluster", "bin/stitch_labels"]
)
