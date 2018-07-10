package gossiper

import (
	"encoding/json"
	"io"
	"net"
)

func handlerGossip(conn net.Conn, s *Gossiper) {
	s.addConn(conn)
	defer s.removeConn(conn)
	for {
		input, err := readFromPeer(conn)
		if err != nil {
			return
		}

		pushEntry := s.CompareAndStore(input.List)
		if len(pushEntry) == 0 {
			continue
		}

		writeToPeer(conn, pushEntry)
	}
}

func readFromPeer(conn net.Conn) (*gossipRequest, error) {
	var lenBuf [4]byte
	_, err := io.ReadFull(conn, lenBuf[:])
	if err != nil {
		return nil, err
	}

	length := uint32(lenBuf[0]) | uint32(lenBuf[1])<<8 | uint32(lenBuf[2])<<16 | uint32(lenBuf[3])<<24

	payload := make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return nil, err
	}

	ret := new(gossipRequest)
	err = json.Unmarshal(payload, ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func writeToPeer(conn net.Conn, input []GossipValue) error {
	var request gossipRequest
	request.List = input

	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	totalLength := len(body)

	b := make([]byte, totalLength+4)
	b[0] = byte(totalLength)
	b[1] = byte(totalLength >> 8)
	b[2] = byte(totalLength >> 16)
	b[3] = byte(totalLength >> 24)
	copy(b[4:], body)

	_, err = conn.Write(b)
	return err

}
