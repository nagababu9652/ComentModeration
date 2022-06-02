package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

func HomeLink(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Welcome home!")
}

func CreateEvent(w http.ResponseWriter, r *http.Request) {
	var newEvent Proschema
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}

	json.Unmarshal(reqBody, &newEvent)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newEvent)

	var _, err1 = gocql.ParseUUID(newEvent.CommentId)
	var _, err2 = gocql.ParseUUID(newEvent.CreatorId)
	if err1 == nil && err2 == nil {
		//Push data in to producer
		comment, _ := json.Marshal(newEvent)
		mainPro(comment)
	}

}

func main() {

	//calling the consumer
	//KafkaNewConsumer()

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", HomeLink)
	router.HandleFunc("/comment", CreateEvent).Methods("POST")
	log.Fatal(http.ListenAndServe(":8080", router))
}
