package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	newrelic "github.com/newrelic/go-agent"
)

type SessionHandler struct {
	Session     *gocql.Session
	RedisClient *redis.Client
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

	cluster := gocql.NewCluster("13.57.186.93") //, "13.57.216.102", "52.53.190.38", "13.56.13.148", "18.144.74.172", "54.193.12.248")
	cluster.Keyspace = "restaurants"
	cluster.ProtoVersion = 3
	// cluster.Timeout = 60000 * time.Millisecond
	// cluster.ConnectTimeout = 60000 * time.Millisecond
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
	// http.HandleFunc(newrelic.WrapHandleFunc(app, "/api/restaurants/overview/", func(w http.ResponseWriter, r *http.Request) {
	// 	cassandraForwarder(newSessionHandler, w, r)
	// 	log.Println("hello")
	// }))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/api/restaurants/overview/", newSessionHandler.cassandraForwarder))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-cbeabceba201153e739d61f39a94004c.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-cbeabceba201153e739d61f39a94004c.txt")
	}))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-04ab469c903d910e3e638cc4ebc4a326.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-04ab469c903d910e3e638cc4ebc4a326.txt")
	}))
	http.Handle(newrelic.WrapHandle(app, "/", http.FileServer(http.Dir("./static"))))

	log.Fatal(http.ListenAndServe(":3002", nil))
}

// (sh *SessionHandler)
func (sh *SessionHandler) cassandraForwarder(w http.ResponseWriter, r *http.Request) {
	// regex, _ := regexp.Compile("/api/restaurants/overview/([0-9]{0,})")
	// if regex.MatchString(r.URL.Path) == false {
	// 	routeErrorHandler(w, r, http.StatusNotFound)
	// 	return
	// }
	switch r.Method {
	case http.MethodGet:
		id := r.URL.Path[len("/api/restaurants/overview/"):]
		log.Println(id)
		val, redisErr := sh.RedisClient.Get(id).Result()
		log.Println(val)
		if redisErr != nil || redisErr == redis.Nil {
			log.Println("here")
			restaurant, err := getOneById(sh.Session, sh.RedisClient, id)
			if err != nil {
				w.WriteHeader(http.StatusRequestTimeout)
				w.Write([]byte(err.Error()))
				log.Println(err.Error())
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write(restaurant)
			}
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(val))
		}
		break
	case http.MethodPost:
		body, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		log.Println(body)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), 500)
			return
		}
		var msg map[string]interface{}
		err = json.Unmarshal(body, &msg)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), 500)
			return
		}
		createOne(sh.Session, msg)
	}
}

// func routeErrorHandler(w http.ResponseWriter, r *http.Request, status int) {

// 	w.WriteHeader(status)
// 	fmt.Fprint(w, "Not a route, try again")
// }

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
