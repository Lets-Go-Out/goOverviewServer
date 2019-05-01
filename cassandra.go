package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
)

func getOneById(session *gocql.Session, id string, w http.ResponseWriter, client *redis.Client) {
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
	if sliceErr != nil {
		w.WriteHeader(http.StatusRequestTimeout)
		w.Write([]byte(sliceErr.Error()))
		log.Println(sliceErr.Error())
	} else if len(restaurant) == 0 {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Cannot find restaurant with id: " + id))
	} else {
		resJSON, jsonErr := json.Marshal(restaurant)
		if jsonErr != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(jsonErr.Error()))
		} else {
			err := client.Set(id, resJSON, 0).Err()
			if err != nil {
				log.Println(err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(resJSON)
		}
	}
	return
}
func createOne(session *gocql.Session, restaurant map[string]interface{}, w http.ResponseWriter) {
	var increment int
	var id int
	if err := session.Query("SELECT increment FROM restaurants.counter WHERE id = 0").Consistency(0x06).Scan(&increment, &id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	insertQuery := session.Query("INSERT INTO restaurants.restaurants (id, name, address_line_1, address_line_2, city, state, zip, longitude, latitude, neighborhood, website, description, hours, phone_number, price_range, review_average, review_count, dining_style, cuisine_type, private_dining, executive_chef, dress_code, catering, payment_options, cross_street, promos, public_transit, private_part_fac, private_party_contact, tags) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", id, restaurant["name"], restaurant["address_line_1"], restaurant["address_line_2"], restaurant["city"], restaurant["state"], restaurant["zip"], restaurant["longitude"], restaurant["latitude"], restaurant["neighborhood"], restaurant["website"], restaurant["description"], restaurant["hours"], restaurant["phone_number"], restaurant["price_range"], restaurant["review_average"], restaurant["review_count"], restaurant["dining_style"], restaurant["cuisine_type"], restaurant["private_dining"], restaurant["executive_chef"], restaurant["dress_code"], restaurant["catering"], restaurant["payment_options"], restaurant["cross_street"], restaurant["promos"], restaurant["public_transit"], restaurant["private_part_fac"], restaurant["private_party_contact"], restaurant["tags"])
	insertErr := insertQuery.Consistency(0x06).Exec()
	if insertErr != nil {
		http.Error(w, insertErr.Error(), 500)
		return
	}
	updateCounterErr := session.Query("UPDATE restaurants.counter SET increment = ? WHERE id = 0", id).Consistency(0x06).Exec()
	if updateCounterErr != nil {
		http.Error(w, updateCounterErr.Error(), 500)
		return
	}
	w.Write([]byte("Document Inserted"))
	return
}

// type Message struct {
// 	id                    int     //`json:"id"`
// 	created_at            string  //`json:"created_at"`
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
