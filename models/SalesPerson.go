package models

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"

	"github.com/tsuna/gohbase/filter"
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

	analColumns := map[string]map[string]string{
		"Analysis": map[string]string{},
		"Category": map[string]string{},
	}
	analCreateReq := hrpc.NewCreateTable(context.Background(), []byte("SalesAnalysis"), analColumns)
	err = HbaseAdminClient.CreateTable(analCreateReq)
	if err != nil {
		log.Println("err:", err)
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

		analValue := map[string]map[string][]byte{
			"Analysis": map[string][]byte{
				"CusNum":   []byte(getCusNumByID(strconv.FormatInt(item.BusinessEntityID, 10))),
				"OrderNum": []byte(getCusNumByID(strconv.FormatInt(item.BusinessEntityID, 10))),
			},
			"Category": map[string][]byte{},
		}
		analPutRequest, err := hrpc.NewPutStr(context.Background(), "SalesAnalysis", strconv.FormatInt(item.BusinessEntityID, 10), analValue)
		if err != nil {
			fmt.Println(err)
		}
		_, err = HbaseClient.Put(analPutRequest)
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

func getCusNumByID(id string) string {
	var res int64 = 0
	pFilter := filter.NewSingleColumnValueFilter([]byte("Info"), []byte("SalesPersonID"), filter.CompareType(filter.Equal), filter.NewBinaryComparator(filter.NewByteArrayComparable([]byte(id))), true, true)
	scanRequest, _ := hrpc.NewScanStr(context.Background(), "Store",
		hrpc.Filters(pFilter))
	scanRsp := HbaseClient.Scan(scanRequest)
	var err error
	item, err := scanRsp.Next()
	for item != nil {
		res += getCusNumByStoreID(string(item.Cells[0].Row))
		item, err = scanRsp.Next()
		if err != nil {
			fmt.Println(err)
			continue
		}
	}

	return strconv.FormatInt(res, 10)
}

func getCusNumByStoreID(id string) int64 {
	var res int64 = 0
	pFilter := filter.NewSingleColumnValueFilter([]byte("ID"), []byte("Store"), filter.CompareType(filter.Equal), filter.NewBinaryComparator(filter.NewByteArrayComparable([]byte(id))), true, true)
	scanRequest, _ := hrpc.NewScanStr(context.Background(), "Customer",
		hrpc.Filters(pFilter))
	scanRsp := HbaseClient.Scan(scanRequest)
	var err error
	item, err := scanRsp.Next()
	for item != nil {
		res += 1
		item, err = scanRsp.Next()
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
	return res
}

func getOrderNumByID(id string) string {
	pFilter := filter.NewSingleColumnValueFilter([]byte("ID"), []byte("SalesPersonID"), filter.CompareType(filter.Equal), filter.NewBinaryComparator(filter.NewByteArrayComparable([]byte(id))), true, true)
	scanRequest, _ := hrpc.NewScanStr(context.Background(), "SalesOrderHeader",
		hrpc.Filters(pFilter))
	scanRsp := HbaseClient.Scan(scanRequest)
	var err error
	item, err := scanRsp.Next()
	var res int64 = 0
	for item != nil {
		res += 1
		item, err = scanRsp.Next()
		if err != nil {
			fmt.Println(err)
			continue
		}
		// res = append(res, string(item.Cells[0].Row))
	}
	return strconv.FormatInt(res, 10)
}
