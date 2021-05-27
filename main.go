package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"os"
	"os/signal"
	"syscall"
	"time"
)


func main1() {
	exit := make(chan bool)

	c, err := v3.New(v3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
	})
	if err != nil {
		logrus.WithError(err).Error("failed to connect to etcd")
		return
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		var killSignal = make(chan os.Signal)
		signal.Notify(killSignal, os.Interrupt, syscall.SIGTERM)
		for {
			fmt.Println("something is happening")
			//
			select {
			case watchRes := <-c.Watch(ctx, "/name", v3.WithPrefix()):
				for _, event := range watchRes.Events {
					logrus.Info(event.Type)
					logrus.Info(string(event.PrevKv.Key))
					logrus.Info(string(event.PrevKv.Value))
					logrus.Info(string(event.Kv.Key))
					logrus.Info(string(event.Kv.Value))
				}
			case <-killSignal:
				cancel()
				logrus.Info("I am dying now...")
				exit <- true
				return
			}
		}


	}()
	<-exit

}

func main() {
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	cli, err := v3.New(v3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
		//DialOptions: []grpc.DialOption{
		//	grpc.WithBlock(),
		//},
	})
	if err != nil {
		logrus.WithError(err).Error("failed to connect to etcd")
		return
	}
	defer cli.Close()

	//s, err := concurrency.NewSession(cli)
	//if err != nil {
	//	logrus.WithError(err).Error("failed to open a concurrent etcd session")
	//}
	ctx:=context.Background()


	go func() {
		session, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.WithError(err).Error("failed to create concurrent session")
			return
		}

		mx:=concurrency.NewMutex(session,"/users")

		err = mx.Lock(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to acquire lock on users key")
			return
		}

		defer func() {
			err = mx.Unlock(ctx)
			if err != nil {
				logrus.WithError(err).Error("failed to delete lock on users key")
				return
			}
		}()
		resp, err := cli.Get(ctx, "/users")
		if err != nil {
			logrus.WithError(err).Error("failed to read users key")
			return
		}
		time.Sleep(10*time.Second)
		var users []string
		err = json.Unmarshal(resp.Kvs[0].Value, &users)
		if err != nil {
			logrus.WithError(err).Error("failed to unmarshall json")
			return
		}
		users = append(users, "goroutine1")

		d, err := json.Marshal(users)
		if err != nil {
			logrus.WithError(err).Error("failed to marshall json")
			return
		}
		_, err = cli.Put(ctx, "/users", string(d))
		if err != nil {
			logrus.WithError(err).Error("failed to write to users key")
			return
		}
		logrus.Info("goroutine1 run successfully")

	}()

	go func() {
		session, err := concurrency.NewSession(cli)
		if err != nil {
			logrus.WithError(err).Error("failed to create concurrent session")
			return
		}

		mx:=concurrency.NewMutex(session,"/users")

		err = mx.Lock(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to acquire lock on users key")
			return
		}
		defer func() {
			err = mx.Unlock(ctx)
			if err != nil {
				logrus.WithError(err).Error("failed to delete lock on users key")
				return
			}
		}()
		resp, err := cli.Get(ctx, "/users")
		if err != nil {
			logrus.WithError(err).Error("failed to read users key")
			return
		}
		time.Sleep(10*time.Second)

		var users []string
		err = json.Unmarshal(resp.Kvs[0].Value, &users)
		if err != nil {
			logrus.WithError(err).Error("failed to unmarshall json")
			return
		}
		users = append(users, "goroutine2")

		d, err := json.Marshal(users)
		if err != nil {
			logrus.WithError(err).Error("failed to marshall json")
			return
		}
		_, err = cli.Put(ctx, "/users", string(d))
		if err != nil {
			logrus.WithError(err).Error("failed to write to users key")
			return
		}
		logrus.Info("goroutine2 run successfully")
	}()
	logrus.Info("waiting for interruption")
	<-interrupt
	logrus.Info("i am dying now :)")

}
