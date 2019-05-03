package main

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/gocql/gocql"
)

func getOneById(session *gocql.Session, id string) ([]byte, error) {
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
	if err := session.Query("SELECT increment FROM restaurants.counter WHERE id = 0").Consistency(0x06).Scan(&id); err != nil {
		return err
	}
	insertQuery := session.Query("INSERT INTO restaurants.restaurants (id, name, address_line_1, address_line_2, city, state, zip, neighborhood, website, description, hours, phone_number, price_range, review_count, dining_style, cuisine_type, private_dining, executive_chef, dress_code, catering, payment_options, cross_street, promos, public_transit, private_part_fac, private_party_contact, tags) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", id+1, restaurant["name"], restaurant["address_line_1"], restaurant["address_line_2"], restaurant["city"], restaurant["state"], restaurant["zip"], restaurant["neighborhood"], restaurant["website"], restaurant["description"], restaurant["hours"], restaurant["phone_number"], restaurant["price_range"], restaurant["review_count"], restaurant["dining_style"], restaurant["cuisine_type"], restaurant["private_dining"], restaurant["executive_chef"], restaurant["dress_code"], restaurant["catering"], restaurant["payment_options"], restaurant["cross_street"], restaurant["promos"], restaurant["public_transit"], restaurant["private_part_fac"], restaurant["private_party_contact"], restaurant["tags"])
	insertErr := insertQuery.Consistency(0x06).Exec()
	if insertErr != nil {
		return insertErr
	}
	updateCounterErr := session.Query("UPDATE restaurants.counter SET increment = ? WHERE id = 0", int(id+7)).Consistency(0x06).Exec()
	if updateCounterErr != nil {
		return updateCounterErr
	}
	return nil
}
func updateOne(session *gocql.Session, id string, body []byte) error {
	var updateMap map[string]interface{}
	err := json.Unmarshal(body, &updateMap)
	if err != nil {
		return err
	}
	for k, v := range updateMap {
		query := session.Query("UPDATE restaurants.restaurants SET "+k+" = ? WHERE id = ?", v, id)
		err = query.Exec()
		if err != nil {
			return err
		}

	}
	return nil
}

func deleteOne(session *gocql.Session, id string) error {
	return session.Query("DELETE * FROM restaurants WHERE id = ?", id).Exec()
}
