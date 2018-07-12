package gossiper

import (
	"context"
	"fmt"
	"github.com/victor-simida/gossiper/pb"
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
	cache     map[string]pb.Message

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
	s.cache = make(map[string]pb.Message)
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

func (s *Gossiper) CompareDigest(input []*pb.Digest) ([]*pb.Message, []*pb.Digest) {
	var myPeculiar []*pb.Message
	var yourPeculiar []*pb.Digest

	s.cacheLock.RLock()
	for _, v := range input {
		if oldValue, ok := s.cache[v.Key]; ok && oldValue.Version > v.GetVersion() {
			myPeculiar = append(myPeculiar, &pb.Message{
				Key:     v.GetKey(),
				Version: oldValue.Version,
				Value:   oldValue.Value,
			})
		} else if !ok || oldValue.Version < v.GetVersion() {
			yourPeculiar = append(yourPeculiar, v)
		}
	}

	temp := make(map[string]bool)
	for _, v := range input {
		temp[v.Key] = true
	}

	for k, v := range s.cache {
		if ok := temp[k]; !ok && !v.DeleteFlag {
			myPeculiar = append(myPeculiar, &pb.Message{
				Key:     v.Key,
				Version: v.Version,
				Value:   v.Value,
			})
		}
	}
	s.cacheLock.RUnlock()

	return myPeculiar, yourPeculiar
}

func (s *Gossiper) GetByDigest(input []*pb.Digest) []*pb.Message {
	var myPeculiar []*pb.Message

	s.cacheLock.RLock()
	for _, v := range input {
		if oldValue, ok := s.cache[v.Key]; ok {
			myPeculiar = append(myPeculiar, &pb.Message{
				Key:     v.GetKey(),
				Version: oldValue.Version,
				Value:   oldValue.Value,
			})
		}
	}
	s.cacheLock.RUnlock()

	return myPeculiar
}

func (s *Gossiper) StoreMessage(input []*pb.Message) {
	s.cacheLock.Lock()
	for _, v := range input {
		if oldValue, ok := s.cache[v.Key]; !ok || oldValue.Version < v.GetVersion() {
			s.cache[v.GetKey()] = pb.Message{
				Key:        v.GetKey(),
				Version:    v.GetVersion(),
				Value:      v.GetValue(),
				DeleteFlag: v.GetDeleteFlag(),
			}
		}
	}
	s.cacheLock.Unlock()

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
func (s *Gossiper) CacheSnapShoot() []pb.Message {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	var result []pb.Message
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
			var degist []*pb.Digest
			for _, v := range input {
				degist = append(degist, &pb.Digest{
					Key:     v.GetKey(),
					Version: v.GetVersion(),
				})
			}
			num := rand.Int()
			addr := s.option.PeerList[num%len(s.option.PeerList)]
			conn, err := s.getConn(addr)
			if err != nil {
				log.Fatalf("get_conn_error:%s", err.Error())
				continue
			}
			writeToPeer(conn, &pb.ExchangeBeginRequest{
				MyApple: degist,
			})
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
			log.Fatalf("dial_error_%s", err.Error())
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
