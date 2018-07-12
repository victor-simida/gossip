package gossiper

import (
	"github.com/gogo/protobuf/proto"
	"github.com/victor-simida/gossiper/pb"
	"io"
	"log"
	"net"
)

const (
	ExchangeBeginRequest = iota + 1
	ExchangeRequest
	ExchangeEndRequest
)

func handlerGossip(conn net.Conn, s *Gossiper) {
	s.addConn(conn)
	defer s.removeConn(conn)
	for {
		input, err := readFromPeer(conn)
		if err != nil {
			return
		}

		go handler(conn, input, s)
	}
}

func readFromPeer(conn net.Conn) (proto.Message, error) {
	var lenBuf [4]byte
	_, err := io.ReadFull(conn, lenBuf[:])
	if err != nil {
		return nil, err
	}

	length := uint32(lenBuf[0]) | uint32(lenBuf[1])<<8 | uint32(lenBuf[2])<<16 | uint32(lenBuf[3])<<24

	headBuf := make([]byte, 2)
	_, err = io.ReadFull(conn, headBuf)
	if err != nil {
		log.Fatalf("read full %s", err.Error())
		return nil, err
	}

	var header pb.Header
	err = proto.Unmarshal(headBuf, &header)
	if err != nil {
		log.Fatalf("proto error %s", err.Error())
		return nil, err
	}

	payload := make([]byte, length-2)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		log.Fatalf("read full %s", err.Error())
		return nil, err
	}

	var ret proto.Message

	switch header.GetFlag() {
	case ExchangeBeginRequest:
		ret = new(pb.ExchangeBeginRequest)
	case ExchangeRequest:
		ret = new(pb.ExchangeRequest)
	case ExchangeEndRequest:
		ret = new(pb.ExchangeEndRequest)
	default:
		panic("wrong type")
	}

	err = proto.Unmarshal(payload, ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func handler(conn net.Conn, reqeust proto.Message, s *Gossiper) {
	switch reqeust.(type) {
	case *pb.ExchangeBeginRequest:
		nextReq := new(pb.ExchangeRequest)
		nextReq.MyPeculiarApple, nextReq.YourPeculiarApple = s.CompareDigest(reqeust.(*pb.ExchangeBeginRequest).GetMyApple())
		writeToPeer(conn, nextReq)
	case *pb.ExchangeRequest:
		s.StoreMessage(reqeust.(*pb.ExchangeRequest).GetMyPeculiarApple())
		nextReq := new(pb.ExchangeEndRequest)
		nextReq.MyPeculiarApple = s.GetByDigest(reqeust.(*pb.ExchangeRequest).GetYourPeculiarApple())
		writeToPeer(conn, nextReq)
	case *pb.ExchangeEndRequest:
		s.StoreMessage(reqeust.(*pb.ExchangeEndRequest).GetMyPeculiarApple())
	}
}

func writeToPeer(conn net.Conn, input proto.Message) error {

	body, err := proto.Marshal(input)
	if err != nil {
		return err
	}

	// 2 is fixed header length
	totalLength := len(body) + 2
	header := new(pb.Header)
	switch input.(type) {
	case *pb.ExchangeBeginRequest:
		header.Flag = ExchangeBeginRequest
	case *pb.ExchangeRequest:
		header.Flag = ExchangeRequest
	case *pb.ExchangeEndRequest:
		header.Flag = ExchangeEndRequest
	default:
		panic("wrong type")
	}

	head, err := proto.Marshal(header)
	if err != nil {
		log.Fatalf("marshal error:%s %+v", err.Error(), header)
		return err
	}

	b := make([]byte, totalLength+4)
	b[0] = byte(totalLength)
	b[1] = byte(totalLength >> 8)
	b[2] = byte(totalLength >> 16)
	b[3] = byte(totalLength >> 24)
	copy(b[4:], head)
	copy(b[6:], body)

	_, err = conn.Write(b)
	return err

}
