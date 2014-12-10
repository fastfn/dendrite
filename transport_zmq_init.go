package dendrite

import (
	zmq "github.com/pebbe/zmq4"
	"log"
	"sync"
	"time"
)

type controlType int

const (
	workerShutdownReq controlType = iota
	workerShutdownAllowed
	workerShutdownDenied
	workerShutdownConfirm
	workerRegisterReq
	workerRegisterAllowed
	workerRegisterDenied
)

// ZeroMQ Transport implementation
type ZMQTransport struct {
	lock              *sync.Mutex
	minHandlers       int
	maxHandlers       int
	incrHandlers      int
	activeRequests    int
	control_c         chan *workerComm
	dealer_sock       *zmq.Socket
	router_sock       *zmq.Socket
	zmq_context       *zmq.Context
	workerIdleTimeout time.Duration
}

// Creates ZeroMQ transport
// Multiplexes incoming connections which are then processed in separate go routines (workers)
// Multiplexer spawns go routines as needed, but 10 worker routines are created on start
// Every request times out after provided timeout duration
func InitZMQTransport(hostname string, timeout time.Duration) (Transport, error) {
	// initialize ZMQ Context
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	// setup router and bind() to tcp address for clients to connect to
	router_sock, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}
	err = router_sock.Bind("tcp://" + hostname)
	if err != nil {
		return nil, err
	}

	// setup dealer for
	dealer_sock, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}
	err = dealer_sock.Bind("inproc://dendrite-zmqdealer")
	if err != nil {
		return nil, err
	}
	poller := zmq.NewPoller()
	poller.Add(router_sock, zmq.POLLIN)
	poller.Add(dealer_sock, zmq.POLLIN)

	transport := &ZMQTransport{
		lock:              new(sync.Mutex),
		minHandlers:       10,
		maxHandlers:       1024,
		incrHandlers:      10,
		activeRequests:    0,
		workerIdleTimeout: 10 * time.Second,
		control_c:         make(chan *workerComm),
		dealer_sock:       dealer_sock,
		router_sock:       router_sock,
		zmq_context:       context,
	}
	// proxy messages between router and dealer
	go func() {
		for {
			sockets, _ := poller.Poll(-1)
			for _, socket := range sockets {
				switch s := socket.Socket; s {
				case router_sock:
					msg, err := s.RecvBytes(0)
					if err != nil {
						log.Println("ERR: TransportListener read bytes on router failed", err)
						continue
					}
					// forward to dealer
					_, err = dealer_sock.SendBytes(msg, 0)
					if err != nil {
						log.Println("ERR: TransportListener forward to dealer failed", err)
						continue
					}
					transport.activeRequests += 1
				case dealer_sock:
					msg, err := s.RecvBytes(0)
					if err != nil {
						log.Println("ERR: TransportListener read bytes on dealer failed", err)
						continue
					}
					// forward up to router
					_, err = router_sock.SendBytes(msg, 0)
					if err != nil {
						log.Println("ERR: TransportListener forward to router failed", err)
						continue
					}
					transport.activeRequests -= 1
				}
			}
		}
	}()

	// Scheduler goroutine keeps track of running workers
	// It spawns new ones if needed, and cancels ones that are idling
	go func() {
		sched_ticker := time.NewTicker(60 * time.Second)
		workers := make(map[*workerComm]bool)
		// fire up initial set of workers
		for i := 0; i < transport.minHandlers; i++ {
			go handleZMQreq(transport)
		}
		for {
			select {
			case comm := <-transport.control_c:
				// worker sent something...
				msg := <-comm.worker_out
				switch {
				case msg == workerRegisterReq:
					if len(workers) == transport.maxHandlers {
						comm.worker_in <- workerRegisterDenied
						log.Println("ERR: TransportListener - max number of workers reached")
						continue
					}
					if _, ok := workers[comm]; ok {
						// worker already registered
						continue
					}
					comm.worker_in <- workerRegisterAllowed
					workers[comm] = true
					log.Println("INFO: TransportListener - registered new worker, total:", len(workers))
				case msg == workerShutdownReq:
					//log.Println("Got shutdown req")
					if len(workers) > transport.minHandlers {
						comm.worker_in <- workerShutdownAllowed
						for _ = range comm.worker_out {
							// wait until worker closes the channel
						}
						delete(workers, comm)
					} else {
						comm.worker_in <- workerShutdownDenied
					}
				}
			case <-sched_ticker.C:
				// check if requests are piling up and start more workers if that's the case
				if transport.activeRequests > 3*len(workers) {
					for i := 0; i < transport.incrHandlers; i++ {
						go handleZMQreq(transport)
					}
				}
			}
		}
	}()
	return transport, nil
}

type workerComm struct {
	worker_in  chan controlType // worker's input channel for two way communication with scheduler
	worker_out chan controlType // worker's output channel for two way communication with scheduler
	worker_ctl chan controlType // worker's control channel for communication with scheduler
}

func handleZMQreq(transport *ZMQTransport) {
	// setup REP socket
	rep_sock, err := transport.zmq_context.NewSocket(zmq.REP)
	if err != nil {
		log.Println("ERR: TransportListener worker failed to create REP socket", err)
		return
	}
	err = rep_sock.Connect("inproc://dendrite-zmqdealer")
	if err != nil {
		log.Println("ERR: TransportListener worker failed to connect to dealer", err)
		return
	}

	// setup communication channels with scheduler
	worker_in := make(chan controlType, 1)
	worker_out := make(chan controlType, 1)
	worker_ctl := make(chan controlType, 1)
	comm := &workerComm{
		worker_in:  worker_in,
		worker_out: worker_out,
		worker_ctl: worker_ctl,
	}
	// notify scheduler that we're up
	worker_out <- workerRegisterReq
	transport.control_c <- comm
	v := <-worker_in
	if v == workerRegisterDenied {
		return
	}

	// setup socket read channel
	read_c := make(chan []byte)
	poller := zmq.NewPoller()
	poller.Add(rep_sock, zmq.POLLIN)
	cancel_c := make(chan bool, 1)
	// read from socket and emit data, or stop if canceled
	go func() {
	MAINLOOP:
		for {
			// poll for 5 seconds, but then see if we should be canceled
			sockets, _ := poller.Poll(5 * time.Second)
			for _, socket := range sockets {
				msg, err := socket.Socket.RecvBytes(0)
				if err != nil {
					log.Println("ERR: TransportListener error while reading from REP, ", err)
					continue
				}
				// emit data
				read_c <- msg
			}
			// check for cancel request
			select {
			case <-cancel_c:
				break MAINLOOP
			default:
				break
			}
		}
	}()

	// read from socket and process request -- OR
	// shutdown if scheduler wants me to -- OR
	// request shutdown from scheduler if idling and exit if allowed
	ticker := time.NewTicker(transport.workerIdleTimeout)
	for {
		select {
		case data := <-read_c:
			// handle data
			log.Println("Got data", data)
			ticker.Stop()
			ticker = time.NewTicker(transport.workerIdleTimeout)

		case controlMsg := <-comm.worker_ctl:
			// got control message, probably shutdown instruction
			log.Println("Got control msg", controlMsg)
		case <-ticker.C:
			// we're idling, lets request shutdown
			comm.worker_out <- workerShutdownReq
			transport.control_c <- comm
			v := <-comm.worker_in
			if v == workerShutdownAllowed {
				log.Println("Shuting down now....")
				close(comm.worker_out)
				cancel_c <- true
				close(cancel_c)
				return
			}
		}
	}
}
