package main

import (
	"github.com/coreos/etcd/client"
	"github.com/golang/glog"
	"os"
	"golang.org/x/net/context"
	"etcdSync/sync"
	"flag"
)

func main(){
	sourEndpoint := flag.String("source", "", "Source endpoint")
	distEndpoint := flag.String("target", "", "Target endpoint")
	sourUser := flag.String("sour_user", "", "User that used to access source endpoint")
	sourPassword := flag.String("sour_password", "", "Password of the sour_user")
	distUser := flag.String("dist_user", "", "User that used to access target endpoint")
	distPassword := flag.String("dist_password", "", "Password of the dist_user")
	dirFLag := flag.String("dir", "", "Target dir to sync")
	flag.Parse()
	if *sourEndpoint == "" || *distEndpoint == ""{
		glog.Errorf("Please specify sourEndpoint and distEndpoint value!")
		os.Exit(1)
	}

	dir := "/"
	if *dirFLag != ""{
		dir = *dirFLag
	}
	sourCli, err := client.New(client.Config{
		Username: *sourUser,
		Password: *sourPassword,
		Endpoints: []string{*sourEndpoint},
		Transport: client.DefaultTransport,
	})
	if err != nil {
		glog.Errorf("Failed to get source endpoint!", err)
	}
	distCli, err := client.New(client.Config{
		Username: *distUser,
		Password: *distPassword,
		Endpoints: []string{*distEndpoint},
		Transport: client.DefaultTransport,
	})
	if err != nil{
		glog.Errorf("Failed to get target endpoint!", err)
	}

	syncer := sync.NewSyncer(&sourCli, &distCli, dir)
	err = syncer.Run(context.TODO())
	if err != nil{
		glog.Errorf("Failed to sync data!", err)
	}
	os.Exit(1)
}