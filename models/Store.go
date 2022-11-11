package models

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"

	"github.com/tsuna/gohbase/hrpc"
)

type StoreDB struct {
	StoreDB []Store `json:"Store"`
}

type Store struct {
	BusinessEntityID int64  `json:"BusinessEntityID"`
	Name             string `json:"Name"`
	SalesPersonID    int64  `json:"SalesPersonID"`
	Demographics     string `json:"Demographics"`
	rowguid          string `json:"rowguid"`
}

func readFileStore() {
	data, _ := ioutil.ReadFile("./export_json/Store.json")
	var res StoreDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Info":    map[string]string{},
		"Display": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("Store"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.StoreDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Info": map[string][]byte{
				"Name":          []byte(item.Name),
				"SalesPersonID": []byte(strconv.FormatInt(item.SalesPersonID, 10)),
				"rowguid":       []byte(item.rowguid),
			},
			"Display": map[string][]byte{
				"Demographics": []byte(item.Demographics),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "Store", strconv.FormatInt(item.BusinessEntityID, 10), values)
		if err != nil {
			fmt.Println(err)
		}
		_, err = HbaseClient.Put(putRequest)
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println(i)
}

func ImportStore() {
	readFileStore()
	fmt.Println("Done Store")
}
