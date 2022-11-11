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
	SalesQuota       int64  `json:"SalesQuota"`
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
		"Code": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("CountryRegionCurrency"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.SalesPersonDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Code": map[string][]byte{
				"CountryRegion": []byte(item.CountryRegionCode),
				"Currency":      []byte(item.CurrencyCode),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "CountryRegionCurrency", strconv.FormatInt(i, 10), values)
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
