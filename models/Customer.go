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

type CustomerDB struct {
	CustomerDB []Customer `json:"Customer"`
}

type Customer struct {
	CustomerID    int64  `json:"CustomerID"`
	PersonID      int64  `json:"PersonID"`
	StoreID       int64  `json:"StoreID"`
	TerritoryID   int64  `json:"TerritoryID"`
	AccountNumber string `json:"AccountNumber"`
	rowguid       string `json:"rowguid"`
	ModifiedDate  string `json:"ModifiedDate"`
}

func readFileCustomer() {
	data, _ := ioutil.ReadFile("./export_json/Customer.json")
	var res CustomerDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"ID":      map[string]string{},
		"Account": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("Customer"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i = 0
	for _, item := range res.CustomerDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"ID": map[string][]byte{
				"Person":    []byte(strconv.FormatInt(item.PersonID, 10)),
				"Store":     []byte(strconv.FormatInt(item.StoreID, 10)),
				"Territory": []byte(strconv.FormatInt(item.TerritoryID, 10)),
			},
			"Account": map[string][]byte{
				"AccountNumber": []byte(item.AccountNumber),
				"rowguid":       []byte(item.rowguid),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "Customer", strconv.FormatInt(item.CustomerID, 10), values)
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

func ImportCustomer() {
	readFileCustomer()
	fmt.Println("Done ImportCustomer")
}
