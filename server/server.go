package calclabels

import (
	"encoding/binary"
	"encoding/json"
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
	calclabelpPath = "/calculation/"
	classifierURI  = "classifiers/"
	classifierName = "classifier.ilp"
	segStatusURI   = "calclabelstatus"
	clusterScript  = "ssh plazas@login2 calclabels"
)

// ?! Directory containing temporary results from segmentation (root + /.calclabels/)
var resultDirectory string

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

	bbox1_str := strings.Split(bodies, ",")
	bbox2_str := strings.Split(bodies, ",")
	for _, _coord_str := range bbox1_str {
		coord, _ := strconv.Atoi(strings.Trim(coord_str, " "))
		bbox1_list = append(bbox1_list, float64(coord))
	}
	for _, _coord_str := range bbox2_str {
		coord, _ := strconv.Atoi(strings.Trim(coord_str, " "))
		bbox2_list = append(bbox2_list, float64(coord))
	}

	json_data["bbox1"] = body_list
	json_data["bbox2"] = body_list

	json_data["classifier"] = r.FormValue("classifier")
	json_data["label-name"] = r.FormValue("labelname")

	calcLabels(w, json_data)
}

// calcLabels starts the cluster job for calculating labels and writes back results
func calcLabels(w http.ResponseWriter, json_data map[string]interface{}) {
	// convert schema to json data
	var schema_data interface{}
	json.Unmarshal([]byte(schemaData), &schema_data)

	// validate json schema
	schema, err := gojsonschema.NewJsonSchemaDocument(schema_data)
	validationResult := schema.Validate(json_data)
	if !validationResult.IsValid() {
		badRequest(w, "JSON did not pass validation")
		err = fmt.Errorf("JSON did not pass validation")
		return
	}

	// retrieve dvid server
	dvidserver, err := getDVIDserver(json_data)
	if err != nil {
		badRequest(w, "DVID server could not be located on proxy")
		return
	}

	// get data uuid
	uuid := json_data["uuid"].(string)

	// base url for all dvid queries
	baseurl := dvidserver + "/api/node/" + uuid + "/"

	// must create random session id
	session_id = randomHex()

	// grab a timestamp (could overflow but is just used for a unique stamp)
	tstamp := int(time.Now().Unix())
	session_id = session_id + "-" + strconv.Itoa(tstamp)
	session_dir := resultDirectory + session_id + "/"
	err = os.MkdirAll(session_dir, 0644)

	if err != nil {
		badRequest(w, "No permission to write directory to: "+resultDirectory)
		return
	}

	// must read classifier and dump to session
	classifier = json_data["classifier"].(string)
	classifier_url := baseurl + classifierURI + classifier

	// dump classifier to disk under session id (default to specified directory)
	resp, err := http.Get(classifier_url)
	if err != nil || resp.StatusCode != 200 {
		badRequet(w, "Classifier could not be read from "+classifier_url)
		return
	}
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		badRequet(w, "Classifier could not be read from "+classifier_url)
		return
	}
	ioutil.WriteFile(session_dir+classifierName, bytes, 0644)

	// load default values
	if _, found := json_data["job-size"]; !found {
		json_data["job-size"] = 500
	}
	if _, found := json_data["overlap-size"]; !found {
		json_data["job-size"] = 40
	}

	// write status in key value on DVID
	keyval_url = baseurl + segStatusURI + "/" + session_id
	json_data["result-callback"] = keyval_url

	// create segstatus uri (if not created) and write status
	payload := `{}`
	payload_rdr := strings.NewReader(payload)
	http.Post(dvidserver+"/api/dataset/"+uuid+"/new/keyvalue/"+segStatus, "application/json", payload_rdr)
	payload = `{"status" : "not started"}`
	payload_rdr = strings.NewReader(payload)
	http.Post(keyval_url, "application/json", payload_rdr)

	// write json to session folder, call cluster script with directory location
	jsonbytes, _ := json.Marshal(jsond_data)
	config_loc := session_dir + "config.json"
	ioutil.WriteFile(config_loc, jsonbytes, 0644)
	go exeCommand(config_loc)

	// dump json callback
	w.Header().Set("Content-Type", "application/json")
	jsondata, _ := json.Marshal(map[string]interface{}{
		"result-callback": keyval_url,
	})
	fmt.Fprintf(w, string(jsondata))
}

// exeCommand wraps call to external program that runs on the cluster
func exeCommand(config_loc string) {
	exec.Command(clusterScript, config_loc).Output()
}

// overlapHandler handles post request to "/service"
func calclabelsHandler(w http.ResponseWriter, r *http.Request) {
	pathlist, requestType, err := parseURI(r, overlapPath)
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
func Serve(proxyserver string, port int, directory String) {
	resultDirectory = directory
	proxyServer = proxyserver

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

	// perform calclabel service
	http.HandleFunc(calclabelsPath, calclabelsHandler)

	// perform bodystats service
	http.HandleFunc(bodystatsPath, bodystatsHandler)

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
