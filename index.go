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
	cluster := gocql.NewCluster("13.57.199.237", "13.57.199.59", "13.56.12.5", "13.57.187.238", "54.219.184.104", "54.183.249.183") //, "13.57.216.102", "52.53.190.38", "13.56.13.148", "18.144.74.172", "54.193.12.248")
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
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/api/restaurants/overview/", newSessionHandler.cassandraForwarder))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-0a2f1a7cf7e88afe8316e5618805739c.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-0a2f1a7cf7e88afe8316e5618805739c.txt")
	}))
	http.HandleFunc(newrelic.WrapHandleFunc(app, "/loaderio-loaderio-0a2f1a7cf7e88afe8316e5618805739c.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./loaderio/loaderio-loaderio-0a2f1a7cf7e88afe8316e5618805739c.txt")
	}))
	http.Handle(newrelic.WrapHandle(app, "/", http.FileServer(http.Dir("./static"))))

	log.Fatal(http.ListenAndServe(":3002", nil))
}
func (sh *SessionHandler) cassandraForwarder(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		id := r.URL.Path[len("/api/restaurants/overview/"):]
		val, redisErr := sh.RedisClient.Get(id).Result()
		if redisErr != nil || redisErr == redis.Nil {
			restaurant, err := getOneById(sh.Session, id)
			if err != nil {
				w.WriteHeader(http.StatusRequestTimeout)
				w.Write([]byte(err.Error()))
				log.Println(err.Error())
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write(restaurant)
				err := sh.RedisClient.Set(id, restaurant, 0)
				if err != nil {
					log.Println(err)
				}
			}
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(val))
		}
		break
	case http.MethodPost:
		body, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
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
		err = createOne(sh.Session, msg)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte("Successfully inserted"))
	case http.MethodPatch:
		body, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		id := r.URL.Path[len("/api/restaurants/overview/"):]
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
		updateErr := updateOne(sh.Session, id, body)
		if updateErr != nil {
			http.Error(w, updateErr.Error(), 500)
			return
		}
	case http.MethodDelete:
		id := r.URL.Path[len("/api/restaurants/overview/"):]
		if id != "the password is password" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Insufficient Permission"))
			return
		}
		deleteErr := deleteOne(sh.Session, id)
		if deleteErr != nil {
			http.Error(w, deleteErr.Error(), 500)
			return
		}
	}
}

// const rest = {address_line_1: "31346 Wehner Plaza",
// address_line_2: "Apt. 049",
// catering: "Iusto enim mollitia consequuntur omnis.",
// city: "East Mozellfort",
// created_at: "Tue Apr 23 2019 21:25:25 GMT+0000 (Coordinated Universal Time)",
// cross_street: "Avon & Brown",
// cuisine_type: "back-end,seamless,collaborative,virtual,ubiquitous,bleeding-edge,intuitive",
// cuisine_types: ["back-end","seamless","collaborative","virtual","ubiquitous"],
// description: "Molestias vitae ducimus. Blanditiis illum sunt. Dolores cum nisi consequatur sint accusamus magni nam.Saepe beatae quidem in illum. Ex veritatis libero est possimus sunt adipisci aliquam. Adipisci veritatis quaerat iste autem aut qui perferendis velit. Dolores incidunt est porro repudiandae beatae itaque et non.Deserunt recusandae cupiditate voluptatem itaque dolore nulla atque. Adipisci molestiae qui consequuntur aut commodi voluptas quibusdam. Odit id consequatur accusamus alias. Fugit consectetur eius tenetur eaque. Dolores eos quia eos repellendus quo sit et officiis aut.",
// dining_style: "Digitized",
// dress_code: "Facilitator",
// executive_chef: "Ashtyn Volkman",
// hours: "Itaque qui cupiditate facilis sed nulla similique et qui qui.",
// latitude: "59.3450",
// longitude: "117.1338",
// name: "Schumm Inc",
// neighborhood: "Stark Wiza Torphy",
// parking_details: "Non temporibus qui necessitatibus dolorem sit placeat.",
// payment_options: "Nesciunt ut ullam placeat rerum repellat magni cupiditate.",
// phone_number: "364-029-0784 x3875",
// price_range: "$",
// private_dining: "Vero ea sit officiis voluptatem est quis nobis eveniet.",
// private_part_fac: "Libero dolorum modi sunt.",
// private_party_contact: "Nisi non accusamus at consectetur delectus provident occaecati facilis.",
// promos: "Voluptas dolores quisquam itaque et quos sunt.",
// public_transit: "Consequatur qui et inventore qui numquam sint voluptates.",
// review_average: "53.46844129070669",
// review_count: 90,
// state: "HI",
// tags: ["value-added", "analyzing"],
// website: "https://jazlyn.com",
// zip: "33321-4628"}

// type Message struct {
// 	id                    int     //`json:"id"`
// // created_at            string  //`json:"created_at"`
// 	name                  string  //`json:"name"`
// 	address_line_1        string  //`json:"address_line_1"`
// 	address_line_2        string  //`json:"address_line_2"`
// 	city                  string  // `json:"city"`
// 	state                 string  //`json:"state"`
// 	zip                   string  // `json:"zip"`
// 	longitude             float32 //`json:"longitude"`
// 	latitude              float32 //`json:"latitude"`
// 	neighborhood          string  // `json:"neighborhood"`
// 	website               string  // `json:"website"`
// 	description           string  // `json:"description"`
// 	hours                 string  //`json:"hours"`
// 	phone_number          string  //`json:"phone_number"`
// 	price_range           string  //`json:"price_range"`
// 	review_average        float32 //`json:"review_average"`
// 	review_count          int     // `json:"review_count"`
// 	dining_style          string  // `json:"dining_style"`
// 	cuisine_type          string  // `json:"cuisine_type"`
// 	private_dining        string  // `json:"private_dining"`
// 	executive_chef        string  //`json:"executive_chef"`
// 	dress_code            string  // `json:"dress_code"`
// 	catering              string  // `json:"catering"`
// 	payment_options       string  //`json:"payment_options"`
// 	parking_details       string  //`json:"parking_details"`
// 	cross_street          string  //`json:"cross_street"`
// 	promos                string  // `json:"promos"`
// 	public_transit        string  //`json:"public_transit"`
// 	private_part_fac      string  //`json:"private_part_fac"`
// 	private_party_contact string  //`json:"private_party_contact"`
// 	tags                  string  //`json:"tags"`
// }
