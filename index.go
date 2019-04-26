package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
)

type SessionHandler struct {
	Session     *gocql.Session
	RedisClient *redis.Client
}

func getOneById(session *gocql.Session, id string) []map[string]interface{} {
	res, err := session.Query(`SELECT * FROM restaurants WHERE id = ? LIMIT 1`, id).Iter().SliceMap()
	if err != nil {
		log.Println("Error: ", err.Error())
	}
	return res
}
func (sh *SessionHandler) cassandraForwarder(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/api/restaurants/overview/"):]
	switch r.Method {
	case http.MethodGet:
		val, redisErr := sh.RedisClient.Get(id).Result()
		if redisErr != nil {
			restaurant := getOneById(sh.Session, id)
			resJSON, jsonErr := json.Marshal(restaurant)
			if jsonErr != nil {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("Cannot find restaurant with id: " + id))
			} else {
				err := sh.RedisClient.Set(id, resJSON, 0).Err()
				if err != nil {
					log.Println(err)
				}
				log.Println("set cache")
				w.Header().Set("Content-Type", "application/json")
				w.Write(resJSON)
			}
		} else {
			log.Println("used cache")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(val))
		}
	case http.MethodPost:

	}
}
func main() {
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
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/api/restaurants/overview/", newSessionHandler.cassandraForwarder)
	log.Fatal(http.ListenAndServe(":3000", nil))

}
