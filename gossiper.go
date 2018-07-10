package gossiper

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Gossiper ...
type Gossiper struct {
	ctx    context.Context
	cancel context.CancelFunc

	option    Option
	cacheLock sync.RWMutex
	cache     map[string]GossipValue

	poolLock sync.Mutex
	connPool map[string]net.Conn
}

// InitGossiper init a Gossiper
func InitGossiper(opts ...ServerOption) *Gossiper {
	rand.Seed(time.Now().UnixNano())

	option := DefaultOption
	for _, o := range opts {
		o(&option)
	}

	s := new(Gossiper)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.cache = make(map[string]GossipValue)
	s.connPool = make(map[string]net.Conn)
	s.option = option

	return s
}

// Start start gossiper
func (s *Gossiper) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", s.option.ListenPort))
	if err != nil {
		return err
	}

	go s.worker()

	var tempDelay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(interface {
				Temporary() bool
			}); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Fatalf("Accept error: %v; retrying in %v\n", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}

			log.Fatalf("done serving; Accept = %v\n", err)
			return err
		}
		tempDelay = 0

		go handlerGossip(conn, s)
	}
}

// CompareAndStore store the part of input value which are newer than cache and return the values of cache which are
// newer than input.
func (s *Gossiper) CompareAndStore(input []GossipValue) []GossipValue {
	var pushEntry []GossipValue
	if len(input) == 0 {
		return s.CacheSnapShoot()
	}

	s.cacheLock.Lock()
	for _, v := range input {
		if oldValue, ok := s.cache[v.Key]; ok && oldValue.Version > v.Version {
			pushEntry = append(pushEntry, oldValue)
		} else {
			s.cache[v.Key] = v
		}
	}
	s.cacheLock.Unlock()

	s.cacheLock.RLock()
	if len(s.cache) > len(input) {
		temp := make(map[string]GossipValue)
		for _, v := range input {
			temp[v.Key] = v
		}

		for k, v := range s.cache {
			if _, ok := temp[k]; !ok && !v.DeleteFlag {
				pushEntry = append(pushEntry, v)
			}
		}
	}
	s.cacheLock.RUnlock()

	return pushEntry
}

func (s *Gossiper) addConn(conn net.Conn) {
	addr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	s.poolLock.Lock()
	s.connPool[addr] = conn
	s.poolLock.Unlock()
}

func (s *Gossiper) removeConn(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	s.poolLock.Lock()
	delete(s.connPool, addr)
	s.poolLock.Unlock()
}

// CacheSnapShoot get a snapshoot about cache
func (s *Gossiper) CacheSnapShoot() []GossipValue {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	var result []GossipValue
	for _, v := range s.cache {
		result = append(result, v)
	}

	return result
}

func (s *Gossiper) worker() {
	ticker := time.NewTicker(s.option.SyncPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			input := s.CacheSnapShoot()
			num := rand.Int()
			addr := s.option.PeerList[num%len(s.option.PeerList)]
			conn, err := s.getConn(addr)
			if err != nil {
				log.Fatalf("get_conn_error:%s", err.Error())
				continue
			}
			writeToPeer(conn, input)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Gossiper) getConn(addr string) (net.Conn, error) {
	var err error
	s.poolLock.Lock()
	defer s.poolLock.Unlock()
	conn, ok := s.connPool[addr]
	if !ok {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		s.connPool[addr] = conn
		go handlerGossip(conn, s)
	}
	return conn, nil
}

// Stop stop this gossiper
func (s *Gossiper) Stop() {
	s.cancel()
}
