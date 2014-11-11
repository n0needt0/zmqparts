//  Lazy Pirate client
//  Use zmq_poll to do a safe request-reply
//  To run, start lpserver and then randomly kill/restart it
//
//  Author: iano <scaly.iano@gmail.com>
//  Based on C example

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"github.com/gorilla/pat"
	"github.com/paulbellamy/ratecounter"
	"github.com/vaughan0/go-ini"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//this is log file
var logFile *os.File

//this where errors go to die
var err error

//the followign are flags passed from commandline
var logdebug *bool = flag.Bool("debug", false, "enable debug logging")
var Configfile *string = flag.String("config", "haconfig.cfg", "Config json file location")
var help *bool = flag.Bool("help", false, "Show options")
var cfg ini.File

var GStats = struct {
	RateCounter     *ratecounter.RateCounter
	Rate            int64
	Workers         int
	ConsecutiveDead int64
	sync.RWMutex
}{
	ratecounter.NewRateCounter(time.Duration(1) * time.Second),
	0,
	0,
	0,
	sync.RWMutex{},
}

var HA = struct {
	Servers []string
	Timeout time.Duration
	Retries int
	sync.RWMutex
}{
	[]string{""},
	time.Duration(2500) * time.Millisecond,
	10,
	sync.RWMutex{},
}

//*****Processor helper functions

var GetRequest = func(sequence int) ([]byte, error) {
	var msg = struct {
		SERIAL int
		TS     int64
		LOAD   map[string]interface{}
	}{
		SERIAL: sequence,
		TS:     time.Now().Unix(),
		LOAD:   make(map[string]interface{}),
	}

	//TODO this is where you set payload

	msg.LOAD["el"] = "goes here"

	jsonstr, err := json.Marshal(msg)
	if err != nil {
		return []byte(""), err

	}

	return jsonstr, nil
}

var GetReply = func(JsonMsg []byte) (int, error) {
	var msg = struct {
		SERIAL int
		TS     int64
		LOAD   map[string]interface{}
	}{
		SERIAL: 0,
		TS:     0,
		LOAD:   make(map[string]interface{}),
	}

	err := json.Unmarshal(JsonMsg, &msg)
	if err != nil {
		return 0, err
	}

	msgerr, ok := msg.LOAD["error"]
	if ok {
		errstr, _ := msgerr.(string)
		return msg.SERIAL, errors.New(errstr)
	}

	return msg.SERIAL, nil
}

//parse command line
func init() {
	flag.Parse()
	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	//parse config
	cfg, err := ini.LoadFile(*Configfile)

	if err != nil {
		log.Fatal("parse config "+*Configfile+" file error: ", err)
		os.Exit(1)
	}

	logfile, ok := cfg.Get("system", "logfile")
	if !ok {
		log.Fatal("'logfile' missing from 'system' section")
		os.Exit(1)
	}

	//open log file
	logFile, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(fmt.Sprintf("Log file error: %s", logfile), err)
		os.Exit(1)
	}

	defer func() {
		logFile.WriteString(fmt.Sprintf("closing %s", time.UnixDate))
		logFile.Close()
	}()

	log.SetOutput(logFile)

	if *logdebug {
		//see what we have here
		for name, section := range cfg {
			debug(fmt.Sprintf("Section: %v\n", name))
			for k, v := range section {
				debug(fmt.Sprintf("%v: %v\n", k, v))
			}
		}
	}

	//kill channel to programatically
	killch := make(chan os.Signal, 1)
	signal.Notify(killch, os.Interrupt)
	signal.Notify(killch, syscall.SIGTERM)
	signal.Notify(killch, syscall.SIGINT)
	signal.Notify(killch, syscall.SIGQUIT)
	go func() {
		<-killch
		log.Printf("Interrupt %s", time.Now().String())
		os.Exit(1)
	}()

	//get goodies from config file
	//http server
	httphost, ok := cfg.Get("http", "host")
	if !ok {
		log.Fatal("'host' missing from 'http' section")
		os.Exit(1)
	}

	httpport, ok := cfg.Get("http", "port")
	if !ok {
		log.Fatal("'port' missing from 'http' section")
		os.Exit(1)
	}

	strworkers, ok := cfg.Get("system", "workers")
	if !ok {
		log.Fatal("'workers' missing from 'system' section")
		os.Exit(1)
	}

	numworkers, err := strconv.Atoi(strworkers)
	if err != nil {
		log.Fatal("'workers' parameter malformed in 'system' section")
		os.Exit(1)
	}

	targetstr, ok := cfg.Get("zmq", "targets")
	if !ok {
		log.Fatal("'targets' missing from 'zmq' section")
		os.Exit(1)
	}

	timeoutstr, ok := cfg.Get("zmq", "timeout")
	if !ok {
		log.Fatal("'timeout' missing from 'zmq' section")
		os.Exit(1)
	}

	HA.Servers = strings.Split(targetstr, ",")

	HA.Timeout, err = time.ParseDuration(timeoutstr)
	if err != nil {
		log.Fatal("'timeout' parameter malformed in 'system' section")
		os.Exit(1)
	}

	retries, ok := cfg.Get("zmq", "retries")
	if !ok {
		log.Fatal("'retries' missing from 'zmq' section")
		os.Exit(1)
	}

	HA.Retries, err = strconv.Atoi(retries)
	if err != nil {
		log.Fatal("'retries' parameter malformed in 'system' section")
		os.Exit(1)
	}

	go func() {
		for {
			select {
			case <-time.After(time.Duration(5) * time.Second):
				GStats.Lock()
				GStats.Rate = GStats.RateCounter.Rate()
				GStats.Unlock()
				log.Printf("rate %d sec, workers %d,  drops %d", GStats.Rate, GStats.Workers, GStats.ConsecutiveDead)
			}
		}
	}()

	//we need to start 2 servers, http for status and zmq
	wg := &sync.WaitGroup{}
	wg.Add(1)
	//first start http interface for self stats
	go func() {

		r := pat.New()
		r.Get("/health", http.HandlerFunc(healthHandle))

		http.Handle("/", r)

		log.Printf("HTTP Listening %s : %s", httphost, httpport)

		err = http.ListenAndServe(httphost+":"+httpport, nil)
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}

		wg.Done()
	}()

	wg.Add(1)

	go func() {

		//start workers
		for i := 1; i <= numworkers; i++ {
			go client()
			GStats.Lock()
			GStats.Workers++
			GStats.Unlock()
			time.Sleep(time.Duration(10) * time.Millisecond) //dont kill cpu
		}

		wg.Done()

	}()

	wg.Wait()

}

func client() {

	CurrentServerNum := 0 //at least one should be configured
	context, _ := zmq.NewContext()
	defer context.Close()

	client, err := context.NewSocket(zmq.REQ)
	if err != nil {
		log.Fatal("can not start worker", err)
	}

	err = client.Connect(HA.Servers[CurrentServerNum])
	if err != nil {
		log.Fatal("can not connect worker", err)
	}

	NumofRetries := 1

	if HA.Retries == 0 {
		//never timesout
		NumofRetries = 100
	} else {
		NumofRetries = HA.Retries
	}

	for sequence, retriesLeft := 1, NumofRetries; retriesLeft > 0; sequence++ {

		if HA.Retries == 0 {
			//never timesout
			retriesLeft = 10
		}

		if sequence > 100000 {
			sequence = 1 //rewind to prevent overflow, highwater mark is set at 1000 by default
		}

		//GET data for request
		data, err := GetRequest(sequence)
		if err != nil {
			log.Printf("Error on GetLoad %s", err)
			continue
		}
		//TODO
		//fmt.Sprintf("REQ (%s)", data)
		//debug("ASK (%d)\n", sequence)
		//client.Send([]byte(strconv.Itoa(sequence)), 0)
		client.Send(data, 0)

		for expectReply := true; expectReply; {
			//  Poll socket for a reply, with timeout
			items := zmq.PollItems{
				zmq.PollItem{Socket: client, Events: zmq.POLLIN},
			}
			if _, err := zmq.Poll(items, HA.Timeout); err != nil {
				log.Printf("err timeout %s", err) //  Timeout
				continue
			}

			//  .split process server reply
			//  Here we process a server reply and exit our loop if the
			//  reply is valid. If we didn't a reply we close the client
			//  socket and resend the request. We try a number of times
			//  before finally abandoning:

			if item := items[0]; item.REvents&zmq.POLLIN != 0 {
				//  We got a reply from the server, must match sequence
				reply, err := item.Socket.Recv(0)
				if err != nil {
					log.Printf("err receive %s", err) //  Timeout
					continue
				}

				debug("%s", reply)
				//unpack reply  here
				replySequence, err := GetReply(reply)
				if err != nil {
					log.Printf("reply err %s", err)
				}

				if replySequence == sequence {
					debug("OK seq:%d rep:%d", sequence, replySequence)
					retriesLeft = NumofRetries
					GStats.Lock()
					GStats.ConsecutiveDead = 0
					GStats.RateCounter.Incr(int64(1))
					GStats.Unlock()
					expectReply = false
				} else {
					log.Printf("Bad reply: %s", reply)

				}
			} else if retriesLeft--; retriesLeft == 0 {
				log.Printf("Server offline, abandoning %s", HA.Servers[CurrentServerNum])

				client.SetLinger(0)
				client.Close()
				log.Println("All Servers are down closing worker")
				GStats.Lock()
				GStats.Workers--
				GStats.Unlock()
				break

			} else {

				if HA.Retries == 0 {
					//never timesout
					retriesLeft = 10
				}

				if sequence > 100000 {
					sequence = 1 //rewind to prevent overflow, highwater mark is set at 1000 by default
				}

				if CurrentServerNum+1 >= len(HA.Servers) {
					CurrentServerNum = 0
				} else {
					//try another one
					CurrentServerNum++
				}

				GStats.Lock()
				GStats.ConsecutiveDead++
				GStats.Unlock()

				log.Printf("W: failing over to %s", HA.Servers[CurrentServerNum])
				//  Old socket is confused; close it and open a new one
				client.SetLinger(0)
				client.Close()

				client, err = context.NewSocket(zmq.REQ)
				if err != nil {
					log.Printf("can not start worker", err)
				}

				err = client.Connect(HA.Servers[CurrentServerNum])
				if err != nil {
					log.Fatal("can not connect worker", err)
				}

				log.Printf("Resending (%s)\n", data)
				//  Send request again, on new socket
				client.Send(data, 0)
			}
		}
	}
}

//Http Handlers
func serve404(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, "Not Found")
}

func healthHandle(w http.ResponseWriter, r *http.Request) {

	res := make(map[string]string)
	major, minor, patch := zmq.Version()

	res["status"] = "OK"

	if GStats.Workers < 1 {
		res["status"] = "DEAD"
	}

	res["ts"] = time.Now().String()
	res["zmq_version"] = fmt.Sprintf("%d.%d.%d", major, minor, patch)

	res["rate"] = fmt.Sprintf("%d", GStats.Rate)
	res["workers"] = fmt.Sprintf("%d", GStats.Workers)
	res["consecutivedead"] = fmt.Sprintf("%d", GStats.ConsecutiveDead)

	b, err := json.Marshal(res)
	if err != nil {
		log.Println("error:", err)
	}

	w.Write(b)
	return
}

//Utilities
//debuggin function dump
func debug(format string, args ...interface{}) {
	if *logdebug {
		if len(args) > 0 {
			log.Printf("DEBUG "+format, args)
		} else {
			log.Printf("DEBUG " + format)
		}
	}
	return
}

//dumps given obj
func dump(t interface{}) string {
	s := reflect.ValueOf(t).Elem()
	typeOfT := s.Type()
	res := ""

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		res = fmt.Sprint(res, fmt.Sprintf("%s %s = %v\n", typeOfT.Field(i).Name, f.Type(), f.Interface()))
	}

	return res
}
