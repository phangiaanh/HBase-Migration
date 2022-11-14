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

type SalesPersonQuotaHistoryDB struct {
	SalesPersonQuotaHistoryDB []SalesPersonQuotaHistory `json:"SalesPersonQuotaHistory"`
}

type SalesPersonQuotaHistory struct {
	BusinessEntityID int64  `json:"BusinessEntityID"`
	QuotaDate        string `json:"QuotaDate"`
	SalesQuota       string `json:"SalesQuota"`
}

func readFileSalesPersonQuotaHistory() {
	data, _ := ioutil.ReadFile("./export_json/SalesPersonQuotaHistory.json")
	var res SalesPersonQuotaHistoryDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"ID":    map[string]string{},
		"Quota": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("SalesPersonQuotaHistory"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.SalesPersonQuotaHistoryDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"ID": map[string][]byte{
				"BusinessEntityID": []byte(strconv.FormatInt(item.BusinessEntityID, 10)),
				"QuotaDate":        []byte(item.QuotaDate),
			},
			"Quota": map[string][]byte{
				"SalesQuota": []byte(item.SalesQuota),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "SalesPersonQuotaHistory", strconv.FormatInt(i, 10), values)
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

func ImportSalesPersonQuotaHistory() {
	readFileSalesPersonQuotaHistory()
	fmt.Println("Done SalesPersonQuotaHistory")
}
