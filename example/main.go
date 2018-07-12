package main

import (
	"fmt"
	"github.com/victor-simida/gossiper"
	"github.com/victor-simida/gossiper/pb"
	"time"
)

func main() {
	opt1 := make([]gossiper.ServerOption, 0)
	opt1 = append(opt1, gossiper.WithPeerList([]string{"127.0.0.1:5101", "127.0.0.1:5102"}))
	opt1 = append(opt1, gossiper.WithSyncPeriod(time.Second))
	opt1 = append(opt1, gossiper.WithListenPort(5100))
	s1 := gossiper.InitGossiper(opt1...)

	opt2 := make([]gossiper.ServerOption, 0)
	opt2 = append(opt1, gossiper.WithPeerList([]string{"127.0.0.1:5100", "127.0.0.1:5102"}))
	opt2 = append(opt1, gossiper.WithSyncPeriod(time.Second))
	opt2 = append(opt1, gossiper.WithListenPort(5101))
	s2 := gossiper.InitGossiper(opt2...)

	opt3 := make([]gossiper.ServerOption, 0)
	opt3 = append(opt1, gossiper.WithPeerList([]string{"127.0.0.1:5101", "127.0.0.1:5100"}))
	opt3 = append(opt1, gossiper.WithSyncPeriod(time.Second))
	opt3 = append(opt1, gossiper.WithListenPort(5102))
	s3 := gossiper.InitGossiper(opt3...)

	go s1.Start()
	go s2.Start()
	go s3.Start()
	defer s1.Stop()
	defer s2.Stop()
	defer s3.Stop()

	s1.StoreMessage([]*pb.Message{
		{
			Key:     "1",
			Version: 1,
			Value:   "1",
		},
	})

	time.Sleep(3 * time.Second)
	fmt.Printf("%+v\n", s1.CacheSnapShoot())
	fmt.Printf("%+v\n", s2.CacheSnapShoot())
	fmt.Printf("%+v\n", s3.CacheSnapShoot())

	s1.StoreMessage([]*pb.Message{
		{
			Key:     "2",
			Version: 1,
			Value:   "2",
		},
		{
			Key:     "1",
			Version: 2,
			Value:   "1.2",
		},
	})
	time.Sleep(3 * time.Second)
	fmt.Printf("%+v\n", s1.CacheSnapShoot())
	fmt.Printf("%+v\n", s2.CacheSnapShoot())
	fmt.Printf("%+v\n", s3.CacheSnapShoot())

	s1.StoreMessage([]*pb.Message{
		{
			Key:     "3",
			Version: 1,
			Value:   "3",
		},
	})
	s1.StoreMessage([]*pb.Message{
		{
			Key:     "4",
			Version: 1,
			Value:   "4",
		},
	})
	time.Sleep(3 * time.Second)
	fmt.Printf("%+v\n", s1.CacheSnapShoot())
	fmt.Printf("%+v\n", s2.CacheSnapShoot())
	fmt.Printf("%+v\n", s3.CacheSnapShoot())

}
