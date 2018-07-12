package pb

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"testing"
)

func TestDigest_Descriptor(t *testing.T) {
	var h Header
	h.Flag = 1
	h1, _ := h.Marshal()
	fmt.Println(len(h1))
	fmt.Println(h1)

	h.Flag = 2
	h1, _ = h.Marshal()
	fmt.Println(len(h1))
	fmt.Println(h1)

	h.Flag = 3
	h1, _ = h.Marshal()
	fmt.Println(len(h1))
	fmt.Println(h1)

	h.Flag = proto.Int(0)
	h1, _ = h.Marshal()
	fmt.Println(len(h1))
	fmt.Println(h1)
}
