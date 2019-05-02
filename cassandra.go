package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

func getOneById(session *gocql.Session, id string) ([]byte, error) {
	q := session.Query(`SELECT * FROM restaurants WHERE id = ? LIMIT 1`, id)
	iter := q.Iter()
	restaurant, sliceErr := iter.SliceMap()
	if sliceErr != nil {
		log.Println(sliceErr)
	}
	log.Println(restaurant)
	if closeErr := iter.Close(); closeErr != nil {
		log.Println(closeErr)
	}
	q.Release()
	if sliceErr != nil {
		return nil, sliceErr
	} else if len(restaurant) == 0 {
		return nil, errors.New("Restaurant not found")
	} else {
		resJSON, jsonErr := json.Marshal(restaurant)
		if jsonErr != nil {
			return nil, jsonErr
		} else {
			return resJSON, nil
		}
	}
}
func createOne(session *gocql.Session, restaurant map[string]interface{}) error {
	var id int
	if err := session.Query("SELECT increment FROM restaurants.counter WHERE id = 0").Scan(&id); err != nil {
		return err
	}
	//float32(restaurant["longitude"].(float64)), float32(restaurant["latitude"].(float64)),
	insertQuery := session.Query("INSERT INTO restaurants.restaurants (id, name, address_line_1, address_line_2, city, state, zip, neighborhood, website, description, hours, phone_number, price_range, review_count, dining_style, cuisine_type, private_dining, executive_chef, dress_code, catering, payment_options, cross_street, promos, public_transit, private_part_fac, private_party_contact, tags) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", id+1, restaurant["name"], restaurant["address_line_1"], restaurant["address_line_2"], restaurant["city"], restaurant["state"], restaurant["zip"], restaurant["neighborhood"], restaurant["website"], restaurant["description"], restaurant["hours"], restaurant["phone_number"], restaurant["price_range"], restaurant["review_count"], restaurant["dining_style"], restaurant["cuisine_type"], restaurant["private_dining"], restaurant["executive_chef"], restaurant["dress_code"], restaurant["catering"], restaurant["payment_options"], restaurant["cross_street"], restaurant["promos"], restaurant["public_transit"], restaurant["private_part_fac"], restaurant["private_party_contact"], restaurant["tags"])
	insertErr := insertQuery.Consistency(0x06).Exec()
	log.Println(id+1, restaurant["name"], restaurant["address_line_1"], restaurant["address_line_2"], restaurant["city"], restaurant["state"], restaurant["zip"], restaurant["longitude"], restaurant["latitude"], restaurant["neighborhood"], restaurant["website"], restaurant["description"], restaurant["hours"], restaurant["phone_number"], restaurant["price_range"], restaurant["review_average"], restaurant["review_count"], restaurant["dining_style"], restaurant["cuisine_type"], restaurant["private_dining"], restaurant["executive_chef"], restaurant["dress_code"], restaurant["catering"], restaurant["payment_options"], restaurant["cross_street"], restaurant["promos"], restaurant["public_transit"], restaurant["private_part_fac"], restaurant["private_party_contact"], restaurant["tags"])
	if insertErr != nil {
		return insertErr
	}
	updateCounterErr := session.Query("UPDATE restaurants.counter SET increment = ? WHERE id = 0", int(id+7)).Exec()
	if updateCounterErr != nil {
		return updateCounterErr
	}
	return nil
}
func updateOne(session *gocql.Session, id string, body []byte) error {
	var updateStr strings.Builder
	bodyMap, err := json.Marshal(body)
	if err != nil {
		return err
	}
	updateStr.Write([]byte("UPDATE restaurants.restaurants SET "))
	for k, v := range bodyMap {
		kStr := strconv.Itoa(k)
		subStr := fmt.Sprintf("%s = %s WHERE id = %s", kStr, string(v), id)
		updateStr.Write([]byte(subStr))
	}
	updateQuery := session.Query(updateStr.String())
	updateErr := updateQuery.Exec()
	if updateErr != nil {
		return updateErr
	}
	return nil
}

func deleteOne(session *gocql.Session, id string) error {
	return session.Query("DELETE * FROM restaurants WHERE id = ?", id).Exec()
}
