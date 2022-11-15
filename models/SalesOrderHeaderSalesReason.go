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

type SalesOrderHeaderSalesReasonDB struct {
	SalesOrderHeaderSalesReasonDB []SalesOrderHeaderSalesReason `json:"SalesOrderHeaderSalesReason"`
}

type SalesOrderHeaderSalesReason struct {
	SalesOrderID  int64 `json:"SalesOrderID"`
	SalesReasonID int64 `json:"SalesReasonID"`
}

func readFileSalesOrderHeaderSalesReason() {
	data, _ := ioutil.ReadFile("./export_json/SalesOrderHeaderSalesReason.json")
	var res SalesOrderHeaderSalesReasonDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Reason": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("SalesOrderHeaderSalesReason"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.SalesOrderHeaderSalesReasonDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Reason": map[string][]byte{
				"ID":     []byte(strconv.FormatInt(item.SalesOrderID, 10)),
				"Reason": []byte(strconv.FormatInt(item.SalesReasonID, 10)),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "SalesOrderHeaderSalesReason", strconv.FormatInt(i, 10), values)
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

func ImportSalesOrderHeaderSalesReason() {
	readFileSalesOrderHeaderSalesReason()
	fmt.Println("Done SalesOrderHeaderSalesReason")
}
