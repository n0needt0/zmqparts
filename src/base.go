package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"github.com/gorilla/pat"
	"github.com/paulbellamy/ratecounter"
	"github.com/vaughan0/go-ini"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
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
var testfail *bool = flag.Bool("fail", false, "enable simulated failures")
var Configfile *string = flag.String("config", "dhchome.cfg", "Config json file location")
var help *bool = flag.Bool("help", false, "Show options")
var cfg ini.File
var Gbackuri, Gfronturi string

var GStats = struct {
	Rate    int64
	Workers int
	sync.RWMutex
}{
	int64(0), 0, sync.RWMutex{},
}

const (
	HEARTBEAT_INTERVAL = time.Second      //  time.Duration
	HEARTBEAT_LIVENESS = 3                //  1-3 is good
	INTERVAL_INIT      = time.Second      //  Initial reconnect
	INTERVAL_MAX       = 32 * time.Second //  After exponential backoff
	PPP_READY          = "\001"           //  Signals worker is ready
	PPP_HEARTBEAT      = "\002"           //  Signals worker heartbeat
)

//parse command line
func init() {
	flag.Parse()
	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

//  Helper function that returns a new configured socket
//  connected to the workers uri

func WorkerSocket(context *zmq.Context) *zmq.Socket {

	worker, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal("can not start backend worker", err)
	}

	err = worker.Connect(Gbackuri)
	if err != nil {
		log.Fatal("can not connect backend worker", err)
	}

	//  Tell queue we're ready for work
	debug("worker ready")

	err = worker.Send([]byte(PPP_READY), 0)

	if err != nil {
		log.Fatal("can not Send ready on backend worker", err)
	}
	return worker
}

type PPWorker struct {
	address []byte    //  Address of worker
	expiry  time.Time //  Expires at this time
}

func NewPPWorker(address []byte) *PPWorker {
	return &PPWorker{
		address: address,
		expiry:  time.Now().Add(HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL),
	}
}

type WorkerQueue struct {
	queue *list.List
}

func NewWorkerQueue() *WorkerQueue {
	return &WorkerQueue{
		queue: list.New(),
	}
}

func (workers *WorkerQueue) Len() int {
	return workers.queue.Len()
}

func (workers *WorkerQueue) Next() []byte {
	elem := workers.queue.Back()
	worker, _ := elem.Value.(*PPWorker)
	workers.queue.Remove(elem)
	return worker.address
}

func (workers *WorkerQueue) Ready(worker *PPWorker) {
	for elem := workers.queue.Front(); elem != nil; elem = elem.Next() {
		if w, _ := elem.Value.(*PPWorker); string(w.address) == string(worker.address) {
			workers.queue.Remove(elem)
			break
		}
	}
	workers.queue.PushBack(worker)
}

func (workers *WorkerQueue) Purge() {
	now := time.Now()
	for elem := workers.queue.Front(); elem != nil; elem = workers.queue.Front() {
		if w, _ := elem.Value.(*PPWorker); w.expiry.After(now) {
			break
		}
		workers.queue.Remove(elem)
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

	Gfronturi, ok = cfg.Get("zmq", "fronturi")
	if !ok {
		log.Fatal("'fronturi' uri string missing from 'zmq' section")
		os.Exit(1)
	}

	Gbackuri, ok = cfg.Get("zmq", "backuri")
	if !ok {
		log.Fatal("'backuri' missing from 'zmq' section")
		os.Exit(1)
	}

	//we need to start 2 servers, http for status and zmq
	wg := &sync.WaitGroup{}
	wg.Add(1)
	//first start http interface for self stats
	go func() {

		r := pat.New()
		r.Get("/health", http.HandlerFunc(healthHandle))

		http.Handle("/", r)

		log.Printf("Listening %s : %s", httphost, httpport)

		err = http.ListenAndServe(httphost+":"+httpport, nil)
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}

		wg.Done()
	}()

	wg.Add(1)

	go func() {

		//see if we run as slave no reason to run own router process
		//workers will use what ever specified in masteruri
		slave, ok := cfg.Get("system", "slave")
		if ok {
			if r, err := strconv.ParseBool(slave); err == nil && r == true {
				return
			}
		}

		counter := ratecounter.NewRateCounter(time.Duration(1) * time.Second)

		//start workers
		for i := 1; i <= numworkers; i++ {
			go worker()
		}

		// Prepare our context and sockets
		context, err := zmq.NewContext()
		if err != nil {
			log.Fatal("error starting zmq server context", err)
			os.Exit(1)
		}
		defer context.Close()

		//start service for outside connections
		log.Printf("Zmq server starting %s", Gfronturi)

		frontend, err := context.NewSocket(zmq.ROUTER)
		if err != nil {
			log.Fatal("error starting frontend zmq", err)
			os.Exit(1)
		}
		defer frontend.Close()
		err = frontend.Bind(Gfronturi)
		if err != nil {
			log.Fatal("error binding frontend zmq", err)
			os.Exit(1)
		}

		// start socket for internal workers connection
		backend, err := context.NewSocket(zmq.ROUTER)
		if err != nil {
			log.Fatal("error starting backend zmq", err)
			os.Exit(1)
		}
		defer backend.Close()
		err = backend.Bind(Gbackuri)
		if err != nil {
			log.Fatal("error binding backend zmq", err)
			os.Exit(1)
		}

		//prestart
		workers := NewWorkerQueue()
		heartbeatAt := time.Now().Add(HEARTBEAT_INTERVAL)

		go func() {
			for {
				select {
				case <-time.After(time.Duration(2) * time.Second):
					r := counter.Rate()
					w := workers.Len()
					GStats.Lock()
					GStats.Rate = r
					GStats.Workers = w
					GStats.Unlock()
					log.Printf("rate %d req/sec, workers %d", r, w)
				}
			}
		}()

		// connect work threads to client threads via a queue
		for {
			items := zmq.PollItems{
				zmq.PollItem{Socket: backend, Events: zmq.POLLIN},
				zmq.PollItem{Socket: frontend, Events: zmq.POLLIN},
			}

			//  Poll frontend only if we have available workers
			if workers.Len() > 0 {
				zmq.Poll(items, HEARTBEAT_INTERVAL)
			} else {
				zmq.Poll(items[:1], HEARTBEAT_INTERVAL)
			}

			//  Handle worker activity on backend
			if items[0].REvents&zmq.POLLIN != 0 {
				frames, err := backend.RecvMultipart(0)
				if err != nil {
					log.Printf("Error receiving from back end %s", err)
					//drop message to teh ground
					continue
				}

				address := frames[0]
				workers.Ready(NewPPWorker(address))

				//  Validate control message, or return reply to client
				if msg := frames[1:]; len(msg) == 1 {
					switch status := string(msg[0]); status {
					case PPP_READY:
						debug("I: PPWorker ready")
					case PPP_HEARTBEAT:
						debug("I: PPWorker heartbeat")
					default:
						log.Printf("E: Invalid message from worker: %v", msg)
					}
				} else {
					counter.Incr(int64(1))
					frontend.SendMultipart(msg, 0)
				}
			}

			if items[1].REvents&zmq.POLLIN != 0 {
				//  Now get next client request, route to next worker
				frames, err := frontend.RecvMultipart(0)
				if err != nil {
					log.Printf("Error receiving from front end %s", err)
					//drop message to teh ground
					continue
				}
				frames = append([][]byte{workers.Next()}, frames...)
				backend.SendMultipart(frames, 0)
			}

			//  .split handle heartbeating
			//  We handle heartbeating after any socket activity. First we send
			//  heartbeats to any idle workers if it's time. Then we purge any
			//  dead workers:
			if heartbeatAt.Before(time.Now()) {
				for elem := workers.queue.Front(); elem != nil; elem = elem.Next() {
					w, _ := elem.Value.(*PPWorker)
					msg := [][]byte{w.address, []byte(PPP_HEARTBEAT)}
					backend.SendMultipart(msg, 0)
				}
				heartbeatAt = time.Now().Add(HEARTBEAT_INTERVAL)
			}

			workers.Purge()
		}
		wg.Done()

	}()

	wg.Wait()

}

//Zmq worker Handlers, they may connect to local or remote queue
//  The interesting parts here are
//  the heartbeating, which lets the worker detect if the queue has
//  died, and vice-versa:

func worker() {
	src := rand.NewSource(time.Now().UnixNano())
	random := rand.New(src)

	context, err := zmq.NewContext()
	if err != nil {
		log.Printf("error starting workers context", err)
		return
	}
	defer context.Close()

	worker := WorkerSocket(context)

	liveness := HEARTBEAT_LIVENESS
	interval := INTERVAL_INIT
	heartbeatAt := time.Now().Add(HEARTBEAT_INTERVAL)
	cycles := 0

	for {
		items := zmq.PollItems{
			zmq.PollItem{Socket: worker, Events: zmq.POLLIN},
		}

		zmq.Poll(items, HEARTBEAT_INTERVAL)

		if items[0].REvents&zmq.POLLIN != 0 {
			frames, err := worker.RecvMultipart(0)
			if err != nil {
				log.Printf("worker err %s", err)
				worker.Close()
			}

			if len(frames) == 3 {
				cycles++
				if cycles > 3 && *testfail {
					switch r := random.Intn(5); r {
					case 0:
						debug("I: Simulating a crash")
						worker.Close()
					case 1:
						debug("I: Simulating CPU overload")
						time.Sleep(3 * time.Second)
					}
				}

				//real work

				debug("received %v", frames)
				//TODO
				time.Sleep(1 * time.Millisecond)
				worker.SendMultipart(frames, 0)
				liveness = HEARTBEAT_LIVENESS

				if *logdebug {
					time.Sleep(10 * time.Millisecond)
				}

			} else if len(frames) == 1 && string(frames[0]) == PPP_HEARTBEAT {
				debug("I: Queue heartbeat")
				liveness = HEARTBEAT_LIVENESS
			} else {
				debug("E: Invalid message")
			}
			interval = INTERVAL_INIT
		} else if liveness--; liveness == 0 {
			log.Printf("Heartbeat failure, Reconnecting queue in %ds...\n", interval/time.Second)
			time.Sleep(interval)
			if interval < INTERVAL_MAX {
				interval *= 2
			}
			worker.Close()
			worker = WorkerSocket(context)
			liveness = HEARTBEAT_LIVENESS
		}

		if heartbeatAt.Before(time.Now()) {
			heartbeatAt = time.Now().Add(HEARTBEAT_INTERVAL)
			worker.Send([]byte(PPP_HEARTBEAT), 0)
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
	res["status"] = "ok"
	res["ts"] = time.Now().String()
	res["zmq_version"] = fmt.Sprintf("%d.%d.%d", major, minor, patch)
	res["ratesec"] = fmt.Sprintf("%d", GStats.Rate)
	res["workers"] = fmt.Sprintf("%d", GStats.Workers)

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
