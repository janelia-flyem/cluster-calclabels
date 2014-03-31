package main

import (
	"flag"
	"fmt"
	"github.com/janelia-flyem/cluster-calclabels/calclabels"
	"github.com/janelia-flyem/serviceproxy/register"
	"os"
)

const defaultPort = 25125

var (
	proxy    = flag.String("proxy", "", "")
	registry = flag.String("registry", "", "")
	portNum  = flag.Int("port", defaultPort, "")
	showHelp = flag.Bool("help", false, "")
        configFile = flag.String("config", "", "")
)

const helpMessage = `
Launches service that computes a label volume over a region using a compute cluster.

Usage: adderexample <data-directory>
      -proxy    (string)        Server and port number for proxy address of serviceproxy 
      -registry (string)        Server and port number for registry address of serviceproxy
      -port     (number)        Port for HTTP server
  -h, -help     (flag)          Show help message
  -c, -config     (flag)        Provide config file for remote cluster access (otherwise local machine can access the cluster)
`

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Parse()

	if *showHelp {
		fmt.Printf(helpMessage)
		os.Exit(0)
	}

	if flag.NArg() != 1 {
		fmt.Println("Must provide a directory for temporary segmentation data")
		fmt.Println(helpMessage)
		os.Exit(0)
	}

	if *registry != "" {
		// creates adder service and points to first argument
		serfagent := register.NewAgent("calcoverlap", *portNum)
		serfagent.RegisterService(*registry)
	}

	calclabels.Serve(*proxy, *portNum, *configFile, flag.Arg(0))
}
