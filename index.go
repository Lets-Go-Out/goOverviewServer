package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	newrelic "github.com/newrelic/go-agent"
)

type SessionHandler struct {
	Session     *gocql.Session
	RedisClient *redis.Client
}

func getOneById(session *gocql.Session, id string) ([]map[string]interface{}, error) {
	q := session.Query(`SELECT * FROM restaurants WHERE id = ? LIMIT 1`, id)
	iter := q.Iter()
	restaurant, sliceErr := iter.SliceMap()
	if sliceErr != nil {
		log.Println(sliceErr)
	}
	if closeErr := iter.Close(); closeErr != nil {
		log.Println(closeErr)
	}
	q.Release()
	return restaurant, sliceErr
}
func (sh *SessionHandler) cassandraForwarder(w http.ResponseWriter, r *http.Request) {
	regex, _ := regexp.Compile("/api/restaurants/overview/([0-9]{0,})")
	if regex.MatchString(r.URL.Path) == false {
		routeErrorHandler(w, r, http.StatusNotFound)
		return
	}
	id := r.URL.Path[len("/api/restaurants/overview/"):]
	switch r.Method {
	case http.MethodGet:
		val, redisErr := sh.RedisClient.Get(id).Result()
		if redisErr != nil || redisErr == redis.Nil {
			restaurant, dbErr := getOneById(sh.Session, id)
			if dbErr != nil || len(restaurant) == 0 {
				w.Write([]byte("Cannot find restaurant with id: " + id))
			} else {
				resJSON, jsonErr := json.Marshal(restaurant)
				if jsonErr != nil {
					w.WriteHeader(http.StatusNotFound)
				} else {
					err := sh.RedisClient.Set(id, resJSON, 0).Err()
					if err != nil {
						log.Println(err)
					}
					w.Header().Set("Content-Type", "application/json")
					w.Write(resJSON)
				}
			}
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(val))
		}
	case http.MethodPost:

	}
}
func routeErrorHandler(w http.ResponseWriter, r *http.Request, status int) {
	w.WriteHeader(status)
	fmt.Fprint(w, "Not a route, try again")
}
func main() {
	config := newrelic.NewConfig("goOverviewService", "bc4034d18b0b4c25d08ad3173e8fc39a28186972")
	app, err := newrelic.NewApplication(config)
	if err != nil {
		log.Print(err)
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	})
	pong, err := redisClient.Ping().Result()
	log.Println(pong, err)

	cluster := gocql.NewCluster("13.57.10.233", "52.53.200.196", "18.144.42.11", "54.183.167.29", "13.57.26.145", "13.57.5.151")
	cluster.Keyspace = "restaurants"
	cluster.ProtoVersion = 3
	cluster.Timeout = 1500 * time.Millisecond
	cluster.ConnectTimeout = 1500 * time.Millisecond
	cluster.NumConns = 4
	cluster.SocketKeepalive = 10 * time.Second
	log.Println("HERE")
	session, err := cluster.CreateSession()
	if err != nil {
		log.Print(err)
	} else {
		log.Println("Connection successful")
	}
	defer func() {
		session.Close()
		log.Println("session closed")
	}()
	newSessionHandler := &SessionHandler{Session: session, RedisClient: redisClient}
	http.Handle(newrelic.WrapHandle(app, "/", http.FileServer(http.Dir("./static"))))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/api/restaurants/overview/", newSessionHandler.cassandraForwarder))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-68bb5d3349374cb02c1530b83e9f26ab.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-68bb5d3349374cb02c1530b83e9f26ab.txt")
	}))
	log.Fatal(http.ListenAndServe(":3002", nil))
}

// server ec2-13-57-179-6.us-west-1.compute.amazonaws.com;
// server ec2-13-56-12-66.us-west-1.compute.amazonaws.com;
// server ec2-54-183-212-2.us-west-1.compute.amazonaws.com;
// server ec2-18-144-43-249.us-west-1.compute.amazonaws.com;
// server ec2-13-57-42-39.us-west-1.compute.amazonaws.com;
// server ec2-54-219-183-253.us-west-1.compute.amazonaws.com;
// server ec2-13-57-219-171.us-west-1.compute.amazonaws.com;
// server ec2-52-53-129-248.us-west-1.compute.amazonaws.com;

// cd goOverviewServer && git fetch --all && git reset --hard origin/master && go build && ./goOverviewServer

// cd goOverviewServer && ./goOverviewServer
