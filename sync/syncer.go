package sync

import (
	"golang.org/x/net/context"
	"github.com/coreos/etcd/client"
	"github.com/golang/glog"
	"fmt"
)

const(
	SET string = "set"
	DELETE string = "delete"
	UPDATE string = "update"
)

type Syncer interface {
	SyncAll(sourKapi, distKapi client.KeysAPI)  error
	SyncUpdates(sourKapi, distKapi client.KeysAPI) <-chan error
	SetIndex(latestIndex uint64)
	Run(ctx context.Context) error
}

func NewSyncer(c *client.Client, dc *client.Client, dir string) Syncer {
	return &syncer{sourCli: c, distCli: dc,  dir: dir}
}

type syncer struct {
	sourCli      *client.Client
	distCli 	*client.Client
	dir	string
	LatestIndex	uint64
}

func (s *syncer) Run(ctx context.Context) error{
	sourKapi := client.NewKeysAPI(*s.sourCli)
	distKapi := client.NewKeysAPI(*s.distCli)

	//Sync updates
	errc := s.SyncUpdates(sourKapi, distKapi)

	//Try to get the latest index
	var latestIndex uint64
	respGet, err := sourKapi.Get(ctx, "ATestKeyToGetTheLatestIndex", &client.GetOptions{Recursive: true,})
	//fmt.Println(" respGet.Index : %v, err: %v",respGet, err)
	if err != nil{
		respSet, err := sourKapi.Set(ctx, "ATestKeyToGetTheLatestIndex", "", &client.SetOptions{})
		if err != nil{
			glog.Errorf("Failed to set key!  %+v", err)
			return err
		}
		latestIndex = respSet.Index
	}
	latestIndex = respGet.Index
	s.SetIndex(latestIndex)

	//Sync all
	err = s.SyncAll(sourKapi, distKapi)
	if err != nil{
		glog.Errorf("Failed to sync all! %+v", err)
		return err
	}

	err = <-errc
	if err != nil{
		glog.Errorf("Failed to sync updates! %+v", err)
		return err
	}
	return nil
}


func (s *syncer) SetIndex(latestIndex uint64){
	s.LatestIndex = latestIndex
}

func (s *syncer) SyncAll(sourKapi, distKapi client.KeysAPI) error{

	resp_source, err := sourKapi.Get(context.TODO(), s.dir, &client.GetOptions{Recursive: true,})
	if err != nil{
		return fmt.Errorf("Failed to get data from source!")
	}

	queue := make([]*client.Node, 0)
	queue = append(queue, resp_source.Node)
	for len(queue) != 0 {
		curNode := queue[0]
		queue = queue[1:]

		if curNode.ModifiedIndex >= s.LatestIndex {continue}

		if curNode.Dir == false{
			_, err := distKapi.Set(context.TODO(), curNode.Key, curNode.Value, &client.SetOptions{})
			if err != nil{
				glog.Warning("error: %+v, Key:  %+v", err, curNode.Key)
			}
		}else {
			_, err := distKapi.Set(context.TODO(), curNode.Key, curNode.Value, &client.SetOptions{Dir: true,})
			if err != nil{
				glog.Warning("error: %+v, Key:  %+v", err, curNode.Key)
			}
			for _, node := range curNode.Nodes {
				queue = append(queue, node)
			}
		}
	}
	return nil
}

func (s *syncer) SyncUpdates(sourKapi, distKapi client.KeysAPI) <-chan error{
	errc := make(chan error)

	go func(){
		watcher := sourKapi.Watcher(s.dir, &client.WatcherOptions{
			Recursive: true,
		})
		for {
			detail, err := watcher.Next(context.Background())
			if(err != nil){
				glog.Errorf("[ERROR]: Unable to get next key! Error info: %+v", err)
				errc <- err
			}
			node := detail.Node
			key := node.Key
			value := node.Value
			switch detail.Action {
			case SET:
				distKapi.Set(context.TODO(), key, value, &client.SetOptions{})
			case DELETE:
				distKapi.Delete(context.TODO(), key, &client.DeleteOptions{
					Recursive: true,
				})
			case UPDATE:
				distKapi.Update(context.TODO(), key, value)
			default:
				panic("unexpected event type")
			}
		}
	}()
	return errc
}
