#assume the environment is set properly by the service

import drmaa
import sys


s = drmma.Session()
jt = s.createJobTemplate()

# use current environment, use only one slot
jt.nativeSpecification = "-j y -o /dev/null -b y -cwd -V"

# run python command for calclabels_cluster with the session as the argument
jt = remoteCommand("python")
jt.args = ["calclabels_cluster.py", sys.argv[1]]

# run on only one slot
jobid = s.runJob(jt)


