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

type SalesOrderHeaderDB struct {
	SalesOrderHeaderDB []SalesOrderHeader `json:"SalesOrderHeader"`
}

type SalesOrderHeader struct {
	SalesOrderID int64 `json:"SalesOrderID"`

	OrderDate string `json:"OrderDate"`
	DueDate   string `json:"DueDate"`
	ShipDate  string `json:"ShipDate"`

	Status          int64 `json:"Status"`
	OnlineOrderFlag int64 `json:"OnlineOrderFlag"`

	RevisionNumber      int64  `json:"RevisionNumber"`
	SalesOrderNumber    string `json:"SalesOrderNumber"`
	PurchaseOrderNumber string `json:"PurchaseOrderNumber"`
	AccountNumber       string `json:"AccountNumber"`

	CustomerID             int64  `json:"CustomerID"`
	SalesPersonID          int64  `json:"SalesPersonID"`
	TerritoryID            int64  `json:"TerritoryID"`
	BillToAddressID        int64  `json:"BillToAddressID"`
	ShipToAddressID        int64  `json:"ShipToAddressID"`
	ShipMethodID           int64  `json:"ShipMethodID"`
	CreditCardID           int64  `json:"CreditCardID"`
	CreditCardApprovalCode string `json:"CreditCardApprovalCode"`
	CurrencyRateID         int64  `json:"CurrencyRateID"`

	SubTotal string `json:"SubTotal"`
	TaxAmt   string `json:"TaxAmt"`
	Freight  string `json:"Freight"`
	TotalDue string `json:"TotalDue"`
	Comment  string `json:"Comment"`
	rowguid  string `json:"rowguid"`
}

func readFileSalesOrderHeader() {
	data, _ := ioutil.ReadFile("./export_json/SalesOrderHeader.json")
	var res SalesOrderHeaderDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Date":   map[string]string{},
		"Status": map[string]string{},
		"Number": map[string]string{},
		"ID":     map[string]string{},
		"Info":   map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("SalesOrderHeader"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.SalesOrderHeaderDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Date": map[string][]byte{
				"OrderDate": []byte(item.OrderDate),
				"DueDate":   []byte(item.DueDate),
				"ShipDate":  []byte(item.ShipDate),
			},
			"Status": map[string][]byte{
				"Status":          []byte(strconv.FormatInt(item.Status, 10)),
				"OnlineOrderFlag": []byte(strconv.FormatInt(item.OnlineOrderFlag, 10)),
			},
			"Number": map[string][]byte{
				"RevisionNumber":      []byte(strconv.FormatInt(item.RevisionNumber, 10)),
				"SalesOrderNumber":    []byte(item.SalesOrderNumber),
				"PurchaseOrderNumber": []byte(item.PurchaseOrderNumber),
				"AccountNumber":       []byte(item.AccountNumber),
			},
			"ID": map[string][]byte{
				"CustomerID":             []byte(strconv.FormatInt(item.CustomerID, 10)),
				"SalesPersonID":          []byte(strconv.FormatInt(item.SalesPersonID, 10)),
				"TerritoryID":            []byte(strconv.FormatInt(item.TerritoryID, 10)),
				"BillToAddressID":        []byte(strconv.FormatInt(item.BillToAddressID, 10)),
				"ShipToAddressID":        []byte(strconv.FormatInt(item.ShipToAddressID, 10)),
				"ShipMethodID":           []byte(strconv.FormatInt(item.ShipMethodID, 10)),
				"CreditCardID":           []byte(strconv.FormatInt(item.CreditCardID, 10)),
				"CreditCardApprovalCode": []byte(item.CreditCardApprovalCode),
				"CurrencyRateID":         []byte(strconv.FormatInt(item.CurrencyRateID, 10)),
			},
			"Info": map[string][]byte{
				"SubTotal": []byte(item.SubTotal),
				"TaxAmt":   []byte(item.TaxAmt),
				"Freight":  []byte(item.Freight),
				"TotalDue": []byte(item.TotalDue),
				"Comment":  []byte(item.Comment),
				"rowguid":  []byte(item.rowguid),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "SalesOrderHeader", strconv.FormatInt(item.SalesOrderID, 10), values)
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

func ImportSalesOrderHeader() {
	readFileSalesOrderHeader()
	fmt.Println("Done SalesOrderHeader")
}
