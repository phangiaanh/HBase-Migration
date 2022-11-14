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

type SalesOrderDetailDB struct {
	SalesOrderDetailDB []SalesOrderDetail `json:"SalesOrderDetail"`
}

type SalesOrderDetail struct {
	SalesOrderID       int64 `json:"SalesOrderID"`
	SalesOrderDetailID int64 `json:"SalesOrderDetailID"`

	CarrierTrackingNumber string `json:"CarrierTrackingNumber"`
	OrderQty              int64  `json:"OrderQty"`

	ProductID      int64 `json:"ProductID"`
	SpecialOfferID int64 `json:"SpecialOfferID"`

	UnitPrice         string  `json:"UnitPrice"`
	UnitPriceDiscount string  `json:"UnitPriceDiscount"`
	LineTotal         float64 `json:"LineTotal"`
	rowguid           string  `json:"rowguid"`
}

func readFileSalesOrderDetail() {
	data, _ := ioutil.ReadFile("./export_json/SalesOrderDetail.json")
	var res SalesOrderDetailDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Sales":   map[string]string{},
		"Carrier": map[string]string{},
		"ID":      map[string]string{},
		"Price":   map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("SalesOrderDetail"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.SalesOrderDetailDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Sales": map[string][]byte{
				"SalesOrderID":       []byte(strconv.FormatInt(item.SalesOrderID, 10)),
				"SalesOrderDetailID": []byte(strconv.FormatInt(item.SalesOrderDetailID, 10)),
			},
			"Carrier": map[string][]byte{
				"CarrierTrackingNumber": []byte(item.CarrierTrackingNumber),
				"OrderQty":              []byte(strconv.FormatInt(item.OrderQty, 10)),
			},
			"ID": map[string][]byte{
				"ProductID":      []byte(strconv.FormatInt(item.ProductID, 10)),
				"SpecialOfferID": []byte(strconv.FormatInt(item.SpecialOfferID, 10)),
			},
			"Price": map[string][]byte{
				"UnitPrice":         []byte(item.UnitPrice),
				"UnitPriceDiscount": []byte(item.UnitPriceDiscount),
				"LineTotal":         []byte(strconv.FormatFloat(item.LineTotal, 'f', 6, 64)),
				"rowguid":           []byte(item.rowguid),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "SalesOrderDetail", strconv.FormatInt(i, 10), values)
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

func ImportSalesOrderDetail() {
	readFileSalesOrderDetail()
	fmt.Println("Done SalesOrderDetail")
}
