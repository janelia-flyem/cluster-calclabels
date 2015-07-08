package calclabels

import (
	"encoding/json"
	"time"
        "fmt"
	"github.com/sigu-399/gojsonschema"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

const (
	// Contain URI location for interface
	interfacePath  = "/interface/"
	calclabelsPath = "/calculation/"
	classifierURI  = "classifiers/key/"
	annotationsURI  = "annotations/"
	classifierName = "classifier.ilp"
	agglomclassifierName = "agglomclassifier.xml"
	graphclassifierName = "graphclassifier.h5"
	synapsesName = "synapses.json"
	segStatusURI   = "clusterjobstatus"
	clusterScript  = "calclabels"
)

// Directory containing temporary results from segmentation (root + /.calclabels/)
var resultDirectory string

// machine where clusterscript is installed
var remoteMachine string

// user for remote program
var remoteUser string

// environment for remote command
var remoteEnv []string


// Address for proxy server
var proxyServer string

// webAddress is the http address for the server
var webAddress string

// parseURI is a utility function for retrieving parts of the URI
func parseURI(r *http.Request, prefix string) ([]string, string, error) {
	requestType := strings.ToLower(r.Method)
	prefix = strings.Trim(prefix, "/")
	path := strings.Trim(r.URL.Path, "/")
	prefix_list := strings.Split(prefix, "/")
	url_list := strings.Split(path, "/")
	var path_list []string

	if len(prefix_list) > len(url_list) {
		return path_list, requestType, fmt.Errorf("Incorrectly formatted URI")
	}

	for i, val := range prefix_list {
		if val != url_list[i] {
			return path_list, requestType, fmt.Errorf("Incorrectly formatted URI")
		}
	}

	if len(prefix_list) < len(url_list) {
		path_list = url_list[len(prefix_list):]
	}

	return path_list, requestType, nil
}

// badRequest is a halper for printing an http error message
func badRequest(w http.ResponseWriter, msg string) {
	fmt.Println(msg)
	http.Error(w, msg, http.StatusBadRequest)
}

// randomHex computes a random hash for storing service results
func randomHex() (randomStr string) {
	randomStr = ""
	for i := 0; i < 8; i++ {
		val := rand.Intn(16)
		randomStr += strconv.FormatInt(int64(val), 16)
	}
	return
}

// getDVIDserver retrieves the server from the JSON or looks it up
func getDVIDserver(jsondata map[string]interface{}) (string, error) {
	if _, found := jsondata["dvid-server"]; found {
		return ("http://" + jsondata["dvid-server"].(string)), nil
	} else if proxyServer != "" {
		resp, err := http.Get("http://" + proxyServer + "/services/dvid/node")
		if err != nil {
			return "", fmt.Errorf("dvid server not found at proxy")
			// handle error
		}
		defer resp.Body.Close()
		decoder := json.NewDecoder(resp.Body)
		dvidnode := make(map[string]interface{})
		err = decoder.Decode(&dvidnode)
		if err != nil {
			return "", fmt.Errorf("Error decoding JSON from proxy server")
		}
		if dvidnode["service-location"] == nil {
			return "", fmt.Errorf("No service location found for DVID")
		}
		return dvidnode["service-location"].(string), nil
	}
	return "", fmt.Errorf("No proxy server location exists")
}

// InterfaceHandler returns the RAML interface for any request at
// the /interface URI.
func interfaceHandler(w http.ResponseWriter, r *http.Request) {
	// allow resources to be accessed via ajax
	w.Header().Set("Content-Type", "application/raml+yaml")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, ramlInterface)
}

// frontHandler handles GET requests to "/"
func frontHandler(w http.ResponseWriter, r *http.Request) {
	pathlist, requestType, err := parseURI(r, "/")
	if err != nil || len(pathlist) != 0 {
		badRequest(w, "Error: incorrectly formatted request")
		return
	}
	if requestType != "get" {
		badRequest(w, "only supports gets")
		return
	}
	w.Header().Set("Content-Type", "text/html")

	tempdata := make(map[string]interface{})
	dvidserver, err := getDVIDserver(tempdata)
	if err != nil {
		dvidserver = ""
	} else {
		dvidserver = strings.Replace(dvidserver, "http://", "", 1)
	}
	formHTMLsub := strings.Replace(formHTML, "DEFAULT", dvidserver, 1)
	fmt.Fprintf(w, formHTMLsub)
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	pathlist, _, err := parseURI(r, "/jobstatus/")
	if err != nil {
		badRequest(w, "Error: incorrectly formatted request")
                return
        }
        dvidkey := strings.Join(pathlist[1:], "/")
        dvidkey = "http://" + dvidkey + "?interactive=false"
        resp, err := http.Get(dvidkey)
        if err != nil || resp.StatusCode != 200 {
            badRequest(w, "DVID job status URI could not be read")
            return
        }
        defer resp.Body.Close()
        bytes, err := ioutil.ReadAll(resp.Body)
        if err != nil {
            badRequest(w, "DVID response could not be read")
            return
        }
        fmt.Fprintf(w, string(bytes))
}        
	

// formHandler handles post request to "/formhandler" from the web interface
func formHandler(w http.ResponseWriter, r *http.Request) {
	pathlist, requestType, err := parseURI(r, "/formhandler/")
	if err != nil || len(pathlist) != 0 {
		badRequest(w, "Error: incorrectly formatted request")
		return
	}
	if requestType != "post" {
		badRequest(w, "only supports posts")
		return
	}

	json_data := make(map[string]interface{})
	dvidserver := r.FormValue("dvidserver")

	if dvidserver != "" {
		json_data["dvid-server"] = dvidserver
	}

	json_data["uuid"] = r.FormValue("uuid")

	bbox1 := r.FormValue("bbox1")
	bbox2 := r.FormValue("bbox2")

        var bbox1_list []interface{}
        var bbox2_list []interface{}
        if bbox1 != "" && bbox2 != "" {
                bbox1_str := strings.Split(bbox1, ",")
                bbox2_str := strings.Split(bbox2, ",")
                for _, coord_str := range bbox1_str {
                        coord, _ := strconv.Atoi(strings.Trim(coord_str, " "))
                        bbox1_list = append(bbox1_list, float64(coord))
                }
                for _, coord_str := range bbox2_str {
                        coord, _ := strconv.Atoi(strings.Trim(coord_str, " "))
                        bbox2_list = append(bbox2_list, float64(coord))
                }
        } else {
                bbox1_list = append(bbox1_list, float64(0))
                bbox1_list = append(bbox1_list, float64(0))
                bbox1_list = append(bbox1_list, float64(0))
                bbox2_list = append(bbox2_list, float64(0))
                bbox2_list = append(bbox2_list, float64(0))
                bbox2_list = append(bbox2_list, float64(0))
        }
        json_data["bbox1"] = bbox1_list
        json_data["bbox2"] = bbox2_list

        json_data["synapses"] = r.FormValue("synapses")
        json_data["roi"] = r.FormValue("roi")
	json_data["classifier"] = r.FormValue("classifier")
	json_data["agglomclassifier"] = r.FormValue("agglomclassifier")
	json_data["agglomfeaturefile"] = r.FormValue("agglomfeaturefile")
	json_data["graphclassifier"] = r.FormValue("graphclassifier")
	json_data["label-name"] = r.FormValue("labelname")
	json_data["algorithm"] = r.FormValue("algorithm")
	json_data["job-size"], _ = strconv.Atoi(r.FormValue("jobsize"))
	json_data["seed-size"], _ = strconv.Atoi(r.FormValue("seedsize"))
	json_data["stitch-mode"], _ = strconv.Atoi(r.FormValue("stitchmode"))
        json_data["agglom-threshold"], _ = strconv.ParseFloat(r.FormValue("agglomthreshold"), 32)

	calcLabels(w, json_data)
}

// calcLabels starts the cluster job for calculating labels and writes back results (assumes grayscale8 is in a datatype called grayscale
func calcLabels(w http.ResponseWriter, json_data map[string]interface{}) {
	// convert schema to json data
	var schema_data interface{}
	json.Unmarshal([]byte(calclabelsSchema), &schema_data)

	// validate json schema
	loader := gojsonschema.NewGoLoader(schema_data)
        schema, err := gojsonschema.NewSchema(loader)
        
        stringloader := gojsonschema.NewGoLoader(json_data)
	validationResult, _ := schema.Validate(stringloader)
	if !validationResult.Valid() {
		badRequest(w, "JSON did not pass validation")
		err = fmt.Errorf("JSON did not pass validation")
		return
	}

	// retrieve dvid server
	dvidserver, err := getDVIDserver(json_data)
	json_data["dvid-server"] = dvidserver
	if err != nil {
		badRequest(w, "DVID server could not be located on proxy")
		return
	}

	// get data uuid
	uuid := json_data["uuid"].(string)

	// base url for all dvid queries
	baseurl := dvidserver + "/api/node/" + uuid + "/"

	// must create random session id
	session_id := randomHex()

	// grab a timestamp (could overflow but is just used for a unique stamp)
	tstamp := int(time.Now().Unix())
	session_id = session_id + "-" + strconv.Itoa(tstamp)
	session_dir := resultDirectory + session_id + "/"
	err = os.MkdirAll(session_dir, 0755)
	if err != nil {
		badRequest(w, "No permission to write directory to: "+resultDirectory)
		return
	}

        if json_data["algorithm"].(string) == "segment" { 
                // must read classifier and dump to session
                classifier := json_data["classifier"].(string)
                classifier_url := baseurl + classifierURI + classifier

                // dump classifier to disk under session id (default to specified directory)
                resp, err := http.Get(classifier_url)
                if err != nil || resp.StatusCode != 200 {
                        badRequest(w, "Classifier could not be read from "+classifier_url)
                        return
                }
                defer resp.Body.Close()
                bytes, err := ioutil.ReadAll(resp.Body)
                if err != nil {
                        badRequest(w, "Classifier could not be read from "+classifier_url)
                        return
                }
                ioutil.WriteFile(session_dir+classifierName, bytes, 0644)
        
                agglomclassifier := json_data["agglomclassifier"].(string)
                agglomclassifier_url := baseurl + classifierURI + agglomclassifier

                // dump classifier to disk under session id (default to specified directory)
                resp, err = http.Get(agglomclassifier_url)
                if err != nil || resp.StatusCode != 200 {
                        badRequest(w, "Classifier could not be read from "+agglomclassifier_url)
                        return
                }
                defer resp.Body.Close()
                bytes, err = ioutil.ReadAll(resp.Body)
                if err != nil {
                        badRequest(w, "Classifier could not be read from "+agglomclassifier_url)
                        return
                }
                ioutil.WriteFile(session_dir+agglomclassifierName, bytes, 0644)
        
                // dump feature file
                agglomfeature := json_data["agglomfeaturefile"].(string)
                if agglomfeature != "" {
                        agglomfeature_url := baseurl + classifierURI + agglomfeature

                        // dump classifier to disk under session id (default to specified directory)
                        resp, err = http.Get(agglomfeature_url)
                        if err != nil || resp.StatusCode != 200 {
                                badRequest(w, "Classifier features could not be read from "+agglomfeature_url)
                                return
                        }
                        defer resp.Body.Close()
                        bytes, err = ioutil.ReadAll(resp.Body)
                        if err != nil {
                                badRequest(w, "Classifier could not be read from "+agglomfeature_url)
                                return
                        }
                        ioutil.WriteFile(session_dir+agglomfeature, bytes, 0644)
                }
                
                graphclassifier := json_data["graphclassifier"].(string)
                if graphclassifier != "" {
                        graphclassifier_url := baseurl + classifierURI + graphclassifier

                        // dump classifier to disk under session id (default to specified directory)
                        resp, err = http.Get(graphclassifier_url)
                        if err != nil || resp.StatusCode != 200 {
                                badRequest(w, "Classifier could not be read from "+graphclassifier_url)
                                return
                        }
                        defer resp.Body.Close()
                        bytes, err = ioutil.ReadAll(resp.Body)
                        if err != nil {
                                badRequest(w, "Classifier could not be read from "+graphclassifier_url)
                                return
                        }
                        ioutil.WriteFile(session_dir+graphclassifierName, bytes, 0644)
                }

                // dump synapses file if it exists
                synapses := json_data["synapses"].(string)
                
                if synapses != "" {
                        synapses_url := baseurl + annotationsURI + synapses

                        // dump synapses to disk under session id (default to specified directory)
                        resp, err := http.Get(synapses_url)
                        if err != nil || resp.StatusCode != 200 {
                                badRequest(w, "Synapses could not be read from "+synapses_url)
                                return
                        }
                        defer resp.Body.Close()
                        bytes, err := ioutil.ReadAll(resp.Body)
                        if err != nil {
                                badRequest(w, "Synapses could not be read from "+synapses_url)
                                return
                        }
                        ioutil.WriteFile(session_dir+synapsesName, bytes, 0644)
                }
        }

	// load default values
	if _, found := json_data["job-size"]; !found {
		json_data["job-size"] = 500 
	}
	if _, found := json_data["overlap-size"]; !found {
		json_data["overlap-size"] = 40
	}

	// write status in key value on DVID
	keyval_url := baseurl + segStatusURI + "/key/" + session_id
	json_data["result-callback"] = keyval_url

	// create segstatus uri (if not created) and write status
	payload := `{"typename" : "keyvalue", "dataname" : "`
        payload += segStatusURI
        payload += `"}`
	payload_rdr := strings.NewReader(payload)
        http.Post(dvidserver+"/api/repo/"+uuid+"/instance", "application/json", payload_rdr)
        payload = `{"status" : "not started"}`
	payload_rdr = strings.NewReader(payload)
	http.Post(keyval_url, "application/json", payload_rdr)

	// write json to session folder, call cluster script with directory location
	jsonbytes, _ := json.Marshal(json_data)
	config_loc := session_dir + "config.json"
	ioutil.WriteFile(config_loc, jsonbytes, 0644)
	go exeCommand(session_dir)

	// dump json callback
	w.Header().Set("Content-Type", "application/json")
	jsondata, _ := json.Marshal(map[string]interface{}{
		"result-callback": keyval_url,
	})
	fmt.Fprintf(w, string(jsondata))
}

// exeCommand wraps call to external program that runs on the cluster
func exeCommand(config_loc string) {
        if remoteMachine == "" {
	        exec.Command(clusterScript, config_loc).Output()
        } else {
                var argument_str string
                for _, envvar := range remoteEnv {
                        // assume shell allows for export of variables
                        argument_str += "export " + envvar + "; "                        
                } 
                argument_str += (clusterScript + " " + config_loc)
	        exec.Command("ssh", remoteUser + "@" + remoteMachine, argument_str).Output()
        }

}

// overlapHandler handles post request to "/service"
func calclabelsHandler(w http.ResponseWriter, r *http.Request) {
	pathlist, requestType, err := parseURI(r, calclabelsPath)
	if err != nil || len(pathlist) != 0 {
		badRequest(w, "Error: incorrectly formatted request")
		return
	}
	if requestType != "post" {
		badRequest(w, "only supports posts")
		return
	}

	// read json
	decoder := json.NewDecoder(r.Body)
	var json_data map[string]interface{}
	err = decoder.Decode(&json_data)

	calcLabels(w, json_data)
}

// Serve is the main server function call that creates http server and handlers
func Serve(proxyserver string, port int, config_file string, directory string) {
	resultDirectory = directory + "/"
	proxyServer = proxyserver

        // read and parse configuration file
        config_handle, _ := os.Open(config_file)
        decoder := json.NewDecoder(config_handle)
	config_data := make(map[string]interface{})
        decoder.Decode(&config_data)
        config_handle.Close()
       
        remoteMachine = "" 
        if mach, found := config_data["remote-machine"]; found {
            remoteMachine = mach.(string)
        }
        remoteUser = "" 
        if ruser, found := config_data["remote-user"]; found {
            remoteUser = ruser.(string)
        }
        if renv, found := config_data["remote-environment"]; found {
                env_list := renv.([]interface{})
                for _, envsing := range env_list {
                        remoteEnv = append(remoteEnv, envsing.(string))
                }
        }

	hname, _ := os.Hostname()
	webAddress = hname + ":" + strconv.Itoa(port)

	fmt.Printf("Web server address: %s\n", webAddress)
	fmt.Printf("Running...\n")

	httpserver := &http.Server{Addr: webAddress}

	// serve out static json schema and raml (allow access)
	http.HandleFunc(interfacePath, interfaceHandler)

	// front page containing simple form
	http.HandleFunc("/", frontHandler)

	// handle form inputs
	http.HandleFunc("/formhandler/", formHandler)

        // show updates to job status
	http.HandleFunc("/jobstatus/", statusHandler)

	// perform calclabel service
	http.HandleFunc(calclabelsPath, calclabelsHandler)

	// exit server if user presses Ctrl-C
	go func() {
		sigch := make(chan os.Signal)
		signal.Notify(sigch, os.Interrupt, syscall.SIGTERM)
		<-sigch
		fmt.Println("Exiting...")
		os.Exit(0)
	}()

	httpserver.ListenAndServe()
}
