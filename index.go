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
			if dbErr != nil {
				w.WriteHeader(http.StatusRequestTimeout)
				w.Write([]byte(dbErr.Error()))
				log.Println(dbErr.Error())
			} else if len(restaurant) == 0 {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("Cannot find restaurant with id: " + id))
			} else {
				resJSON, jsonErr := json.Marshal(restaurant)
				if jsonErr != nil {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(jsonErr.Error()))
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

	cluster := gocql.NewCluster("13.56.12.179", "13.57.196.193", "13.57.219.114", "18.144.45.65", "13.56.224.145", "54.183.212.132")
	cluster.Keyspace = "restaurants"
	cluster.ProtoVersion = 3
	cluster.Timeout = 60000 * time.Millisecond
	cluster.ConnectTimeout = 60000 * time.Millisecond
	cluster.ReconnectInterval = 1 * time.Second
	cluster.Consistency = 0x01
	cluster.NumConns = 8
	session, err := cluster.CreateSession()
	if err != nil {
		log.Print(err)
	} else {
		log.Println("Connection successful")
	}
	defer session.Close()
	newSessionHandler := &SessionHandler{Session: session, RedisClient: redisClient}
	http.Handle(newrelic.WrapHandle(app, "/", http.FileServer(http.Dir("./static"))))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/api/restaurants/overview/", newSessionHandler.cassandraForwarder))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-cbeabceba201153e739d61f39a94004c.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-cbeabceba201153e739d61f39a94004c.txt")
	}))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-04ab469c903d910e3e638cc4ebc4a326.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-04ab469c903d910e3e638cc4ebc4a326.txt")
	}))
	log.Fatal(http.ListenAndServe(":3002", nil))
}

// server ec2-54-215-168-121.us-west-1.compute.amazonaws.com;
// server ec2-13-56-77-85.us-west-1.compute.amazonaws.com
// server ec2-18-144-62-109.us-west-1.compute.amazonaws.com;
// server ec2-52-53-246-139.us-west-1.compute.amazonaws.com;
// server ec2-18-144-70-226.us-west-1.compute.amazonaws.com;
// server ec2-54-219-170-43.us-west-1.compute.amazonaws.com;
// server ec2-13-57-219-171.us-west-1.compute.amazonaws.com;
// server ec2-52-53-129-248.us-west-1.compute.amazonaws.com;

// cd goOverviewServer && git fetch --all && git reset --hard origin/master && go build && ./goOverviewServer

// cd goOverviewServer && ./goOverviewServer
