.PHONY: proto
all: proto
	echo "success"
proto:
	protoc --gofast_out="./" ./proto/define.proto