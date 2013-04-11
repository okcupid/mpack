// -*- mode: go; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-

package mpack

import (
	"bytes"
	"github.com/okcupid/jsonw"
	"github.com/okcupid/logchan"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	LOG_NONE        logchan.Level = 0x0
	LOG_CONNECTIONS logchan.Level = 0x1           // 'c'
	LOG_TRACE       logchan.Level = 0x2           // 't'
	LOG_TIMINGS     logchan.Level = 0x4           // 'T'
	LOG_STARTUP     logchan.Level = 0x8           // 's'
	LOG_ALL         logchan.Level = (1 << 63) - 1 // 'A'
)

type Server struct {
	host    string
	framed  bool
	handler RpcHandler
	Logger  *logchan.Logger
}

type ServerConn struct {
	conn    net.Conn
	framed  bool
	results chan []byte
	quit    chan bool
	srv     *Server
}

type RpcHandler interface {
	Handle(string, jsonw.Wrapper, net.Conn) (jsonw.Wrapper, error)
}

func NewServerConn(srv *Server, conn net.Conn) (sc *ServerConn) {
	sc = new(ServerConn)
	sc.conn = conn
	sc.srv = srv
	sc.results = make(chan []byte, 1024)
	sc.quit = make(chan bool)
	return
}

func NewServer(host string, framed bool, h RpcHandler) (s *Server) {
	s = new(Server)
	s.host = host
	s.framed = framed
	s.handler = h
	ch := logchan.Channels{
		logchan.Channel{LOG_NONE, '0', "no logging"},
		logchan.Channel{LOG_CONNECTIONS, 'c', "connections"},
		logchan.Channel{LOG_TRACE, 't', "trace"},
		logchan.Channel{LOG_TIMINGS, 'T', "timings"},
		logchan.Channel{LOG_STARTUP, 's', "startup"},
		logchan.Channel{LOG_ALL, 'A', "all"}}
	s.Logger = logchan.NewLogger(ch, LOG_STARTUP|LOG_CONNECTIONS)
	return
}

func (srv *Server) ListenAndServe() error {

	tcpAddr, err := net.ResolveTCPAddr("tcp", srv.host)
	var listener net.Listener

	if err != nil {
		log.Printf("error resolving address %s: %s", srv.host, err)
	} else {
		listener, err = net.Listen(tcpAddr.Network(), tcpAddr.String())
		if err != nil {
			log.Printf("error listening: %s", err)
		}
	}

	if err == nil {

		srv.Logger.Printf(LOG_STARTUP,
			"Starting server run loop, listening on %s", srv.host)

		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("accept error: %v", err)
			} else {
				sc := NewServerConn(srv, conn)
				go sc.serve()
			}
		}

		listener.Close()
	}
	return err
}

func (sc *ServerConn) serve() {

	sc.srv.Logger.Printf(LOG_CONNECTIONS,
		"connection established: remote=%s\n", sc.conn.RemoteAddr())

	go sc.sendResults()

	gogo := true
	for gogo {
		rpc, _, err := Unpack(sc.conn, sc.srv.framed)
		if err != nil {
			gogo = false
		} else {
			go sc.srv.processRpc(jsonw.NewWrapper(rpc), sc)
		}
	}
	sc.quit <- true
}

func (sc *ServerConn) sendResults() {
	gogo := true
	for gogo {
		select {
		case result := <-sc.results:
			n, err := sc.conn.Write(result)
			if err != nil {
				log.Printf("error writing result: %s", err)
			}
			if n != len(result) {
				log.Printf("didn't fully write result. wrote %d bytes, not %d bytes", n, len(result))
			}
		case <-sc.quit:
			gogo = false
		}
	}
}

func (srv *Server) processRpc(rpc *jsonw.Wrapper, sc *ServerConn) {

	startTime := time.Now()

	var e error
	var prfx int
	var msgid uint
	var procedure string
	var args *jsonw.Wrapper
	var res jsonw.Wrapper
	var resdat interface{}

	if prfx, e = rpc.AtIndex(0).GetInt(); e != nil {
		log.Printf("Error reading prefix byte: %s", e)
	} else if byte(prfx) != rpc_request {
		log.Printf("did not receive an rpc request")
	} else if msgid, e = rpc.AtIndex(1).GetUint(); e != nil {
		log.Printf("Bad msgid ID received: %s", e)
	} else if procedure, e = rpc.AtIndex(2).GetString(); e != nil {
		log.Printf("Cannot find procedure name: %s", e)
	} else if args = rpc.AtIndex(3); !args.IsOk() {
		log.Printf("No arguments found: %s", args.Error())
		e = args.Error()
	} else {
		srv.Logger.Printf(LOG_TRACE,
			"rpc request: msgid=%d, proc=%s, args=%s",
			msgid, procedure, args.GetDataOrNil())

		res, e = srv.handler.Handle(procedure, *args, sc.conn)

		srv.Logger.Printf(LOG_TRACE,
			"rpc request: msgid=%d, proc=%s, res=%s, err=%s",
			msgid, procedure, res.GetDataOrNil(), e)
	}

	if e == nil {
		resdat, e = res.GetData()
	}

	var out []byte

	if e == nil {
		out, e = successResponse(uint32(msgid), resdat, srv.framed)
	}
	if e != nil {
		out, _ = errorResponse(uint32(msgid), e.Error(), srv.framed)
	}
	sc.results <- out

	srv.Logger.Printf(LOG_TIMINGS, "rpc execute time: %.3f ms",
		(float64)(time.Now().Sub(startTime))/1e6)
}

func errorResponse(msgid uint32, message string, framed bool) ([]byte, error) {
	response := makeResponse(msgid)
	response[2] = message
	return packMessage(response, framed)
}

func successResponse(msgid uint32, result interface{}, framed bool) ([]byte, error) {
	response := makeResponse(msgid)
	response[3] = result
	return packMessage(response, framed)
}

func makeResponse(msgid uint32) []interface{} {
	response := make([]interface{}, 4)
	response[0] = rpc_response
	response[1] = msgid
	response[2] = nil
	response[3] = nil
	return response
}

func packMessage(message interface{}, framed bool) ([]byte, error) {
	b := new(bytes.Buffer)
	_, err := Pack(b, message, framed)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

type ReplyPair struct {
	data *jsonw.Wrapper
	err  error
}

type Error struct {
	msg string
}

func (e Error) Error() string { return e.msg }

type ReplyPairChan chan ReplyPair

type JsonWrapChan chan *jsonw.Wrapper

type Client struct {
	Host           string
	conn           net.Conn
	idCounter      int64
	outputChannels map[int64]ReplyPairChan
	Connected      bool
	Framed         bool
}

func NewClient(host string, framed bool) (*Client, error) {
	result := new(Client)
	result.Host = host
	result.Framed = framed

	result.outputChannels = make(map[int64]ReplyPairChan)

	tcpAddr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return nil, err
	}
	result.conn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	result.Connected = true
	go result.StartReader()
	return result, nil
}

func (client *Client) StartReader() {
	gogo := true

	for gogo {
		gogo = client.ReadOne()
	}
	client.Connected = false
}

func (cli *Client) ReadOne() bool {
	generic, _, err := Unpack(cli.conn, cli.Framed)
	host := cli.Host
	ret := true

	if err == io.EOF {
		log.Printf("%s: EOF", cli)
		ret = false
	} else if err.Error() == "use of closed network connection" {
		log.Printf("%s: unpack error: %s", host, err)
		ret = false
	} else if err != nil {
		log.Printf("%s: unpack error: %s", host, err)
	} else if response := jsonw.NewWrapper(generic); response == nil {
		log.Printf("%s: unexpected JSON failure")
	} else if p, e := response.AtIndex(0).GetInt(); e != nil {
		log.Printf("%s: error decoding prefix: %s", host, e)
	} else if byte(p) != rpc_response {
		log.Printf("%s: didn't get rpc_response", host)
	} else if id, e := response.AtIndex(1).GetInt64(); e != nil {
		log.Printf("%s: no msgid found: %s", host, e)
	} else if output, present := cli.outputChannels[id]; !present {
		log.Printf("%s: no output channel found for msgid %d", host, id)
	} else {
		var rp ReplyPair
		if !response.AtIndex(2).IsNil() {
			s, e := response.AtIndex(2).GetString()
			if e != nil {
				rp.err = e
				s = e.Error()
			} else {
				rp.err = Error{s}
			}
			log.Printf("%s: error: %s", host, s)
		} else if reply, e := response.AtIndex(3).GetData(); e != nil {
			rp.err = e
			log.Printf("%s: invalid reply sent: %s", host, e)
		} else {
			rp.data = jsonw.NewWrapper(reply)
		}
		output <- rp
		delete(cli.outputChannels, id)
	}
	return ret
}

// XXX let them call this with multiple params and wrap them in an array
func (client *Client) CallSync(procedure string, params *jsonw.Wrapper) (res *jsonw.Wrapper, err error) {
	ch := make(ReplyPairChan)
	err = client.Call(procedure, params, ch)
	if err == nil {
		rp := <-ch
		if rp.err != nil {
			err = rp.err
		}
		res = rp.data
	}
	return
}

// params is a dictionary
func (client *Client) Call(procedure string, params *jsonw.Wrapper, output ReplyPairChan) error {

	msgid := client.idCounter
	client.idCounter += 1

	request := jsonw.NewArray(4)
	request.SetIndex(0, jsonw.NewInt(int(rpc_request)))
	request.SetIndex(1, jsonw.NewInt(int(msgid)))
	request.SetIndex(2, jsonw.NewString(procedure))
	request.SetIndex(3, params)

	msg, err := packMessage(request.GetDataOrNil(), client.Framed)
	if err != nil {
		log.Printf("Error packing message: %s", err)
		return err
	}
	client.outputChannels[msgid] = output
	client.conn.Write(msg)

	return nil
}

func (client *Client) IsConnected() bool {
	return client.Connected
}

func (client *Client) Close() {
	client.Connected = false
	client.conn.Close()
}

type ClientPool struct {
	Host    string
	MaxSize int
	clients []*Client
	lock    *sync.Mutex
	Framed  bool
}

func NewClientPool(host string, framed bool) *ClientPool {
	result := new(ClientPool)
	result.Host = host
	result.lock = new(sync.Mutex)
	result.MaxSize = 10
	result.Framed = framed
	return result
}

func (cp *ClientPool) Get() (*Client, error) {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	for len(cp.clients) > 0 {
		n := len(cp.clients)
		result := cp.clients[n-1]
		cp.clients = cp.clients[:n-1]
		if result.Connected {
			return result, nil
		}
		log.Printf("discarding disconnected client")
		// XXX could check for stale connections like Pool does...
	}

	result, err := NewClient(cp.Host, cp.Framed)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (cp *ClientPool) Put(client *Client) {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	if client.Connected == false {
		log.Printf("returning a disconnected client...closing...")
		client.Close()
		log.Printf("close finished")
		return
	}

	if len(cp.clients) >= cp.MaxSize {
		log.Printf("pool is full, discarding client")
		client.Close()
		return
	}

	cp.clients = append(cp.clients, client)
}
