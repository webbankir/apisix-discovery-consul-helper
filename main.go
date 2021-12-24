package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"log"
	"os"
	"sync"
)

var (
	ctx             = context.Background()
	services        = make(map[string]*watch.Plan)
	redisServerAddr string
	consulAddr      string
	consulDc        string
	consulToken     string
)

func main() {
	redisServerAddr = os.Getenv("REDIS_SERVER_ADDR")
	consulAddr = os.Getenv("CONSUL_ADDR")
	consulDc = os.Getenv("CONSUL_DC")
	consulToken = os.Getenv("CONSUL_TOKEN")

	if len(redisServerAddr) == 0 {
		redisServerAddr = "127.0.0.1:6379"
	}

	if len(consulAddr) == 0 {
		consulAddr = "127.0.0.1:8500"
	}

	if len(consulDc) == 0 {
		consulDc = "dc1"
	}

	if len(consulToken) == 0 {
		panic("Please set env CONSUL_TOKEN")
	}

	log.Printf("DD %v", redisServerAddr)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisServerAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	var waitGroup = &sync.WaitGroup{}

	servicesWatchPlan, err := watch.Parse(map[string]interface{}{
		"type":       "services",
		"token":      consulToken,
		"datacenter": consulDc,
	})
	if err != nil {
		fmt.Printf("jopka %v", err)
		panic(err)
	}
	mtx := sync.RWMutex{}
	servicesWatchPlan.Handler = func(idx uint64, data interface{}) {
		mtx.Lock()
		for service := range data.(map[string][]string) {
			if services[service] == nil {
				waitGroup.Add(1)
				startWatchingService(service, redisClient)
			}
		}
		mtx.Unlock()
	}

	waitGroup.Add(1)
	go func() {
		if err := servicesWatchPlan.Run(consulAddr); err != nil {
			log.Printf("err: %v", err)
		}
	}()
	defer servicesWatchPlan.Stop()
	defer waitGroup.Done()

	waitGroup.Wait()
}

func startWatchingService(name string, redisClient *redis.Client) {
	serviceWatchPlan, err := watch.Parse(map[string]interface{}{
		"type":       "service",
		"token":      consulToken,
		"datacenter": consulDc,
		"service":    name,
	})

	if err != nil {
		fmt.Printf("jopka %v", err)
		panic(err)
	}

	serviceWatchPlan.Handler = func(idx uint64, data interface{}) {

		switch d := data.(type) {

		case []*api.ServiceEntry:
			var nodes []map[string]interface{}
			var serviceName string
			for _, i := range d {
				serviceName = i.Service.Service
				if i.Checks.AggregatedStatus() == "passing" {
					host := i.Service.Address
					if host == "" {
						host = i.Node.Address
					}
					nodes = append(nodes, map[string]interface{}{
						"host":   host,
						"port":   i.Service.Port,
						"weight": 1,
					})
				}
			}

			log.Printf("data name %v, %v", name, nodes)

			redisKey := "service_" + serviceName

			if len(nodes) > 0 {
				bb, _ := json.Marshal(nodes)
				err = redisClient.Set(ctx, redisKey, string(bb), 0).Err()
				if err != nil {
					log.Printf("Can't store keys to redis: %v, with reason: '%v'", redisKey, err)
				}
			} else {
				if err := redisClient.Del(ctx, redisKey).Err(); err != nil {
					log.Printf("Can't delete keys from redis: %v, with reason: '%v'", redisKey, err)
				}
			}
		}
	}
	services[name] = serviceWatchPlan
	go func() {
		if err := serviceWatchPlan.Run(consulAddr); err != nil {
			log.Printf("err: %v", err)
		}
	}()
}
