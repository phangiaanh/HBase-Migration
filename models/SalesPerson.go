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

type SalesPersonDB struct {
	SalesPersonDB []SalesPerson `json:"SalesPerson"`
}

type SalesPerson struct {
	BusinessEntityID int64  `json:"BusinessEntityID"`
	TerritoryID      int64  `json:"TerritoryID"`
	SalesQuota       string `json:"SalesQuota"`
	Bonus            string `json:"Bonus"`
	CommissionPct    string `json:"CommissionPct"`
	SalesYTD         string `json:"SalesYTD"`
	SalesLastYear    string `json:"SalesLastYear"`
	rowguid          string `json:"rowguid"`
}

func readFileSalesPerson() {
	data, _ := ioutil.ReadFile("./export_json/SalesPerson.json")
	var res SalesPersonDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Sales":    map[string]string{},
		"Addition": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("SalesPerson"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.SalesPersonDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Sales": map[string][]byte{
				"TerritoryID":   []byte(strconv.FormatInt(item.TerritoryID, 10)),
				"SalesQuota":    []byte(item.SalesQuota),
				"SalesYTD":      []byte(item.SalesYTD),
				"SalesLastYear": []byte(item.SalesLastYear),
				"rowguid":       []byte(item.rowguid),
			},
			"Addition": map[string][]byte{
				"Bonus":         []byte(item.Bonus),
				"CommissionPct": []byte(item.CommissionPct),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "SalesPerson", strconv.FormatInt(item.BusinessEntityID, 10), values)
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

func ImportSalesPerson() {
	readFileSalesPerson()
	fmt.Println("Done SalesPerson")
}
