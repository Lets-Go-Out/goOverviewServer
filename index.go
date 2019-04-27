package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
	newrelic "github.com/newrelic/go-agent"
)

type SessionHandler struct {
	Session     *gocql.Session
	RedisClient *redis.Client
}

func getOneById(session *gocql.Session, id string) ([]map[string]interface{}, error) {
	iter := session.Query(`SELECT * FROM restaurants WHERE id = ? LIMIT 1`, id).Iter()

	restaurant, sliceErr := iter.SliceMap()
	if sliceErr != nil {
		log.Println(sliceErr)
	}
	if closeErr := iter.Close(); closeErr != nil {
		log.Println(closeErr)
	}
	log.Println(restaurant)
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
				log.Println(dbErr, restaurant)
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
					log.Println("set cache")
					w.Header().Set("Content-Type", "application/json")
					w.Write(resJSON)
				}
			}
		} else {
			log.Println("used cache")
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

	cluster := gocql.NewCluster("13.56.161.22",
		"54.153.94.139",
		"13.56.248.92",
		"54.183.65.58",
		"54.219.182.29",
		"13.56.232.185")
	cluster.Keyspace = "restaurants"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Print(err)
	} else {
		log.Println("Connection successful")
	}
	newSessionHandler := &SessionHandler{Session: session, RedisClient: redisClient}
	http.Handle(newrelic.WrapHandle(app, "/", http.FileServer(http.Dir("./static"))))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/api/restaurants/overview/", newSessionHandler.cassandraForwarder))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-aa7f4472cf256de11e8e791c7314f1a1.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-aa7f4472cf256de11e8e791c7314f1a1.txt")
	}))
	log.Fatal(http.ListenAndServe(":3002", nil))
}
