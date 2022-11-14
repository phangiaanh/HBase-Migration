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

type SalesReasonDB struct {
	SalesReasonDB []SalesReason `json:"SalesReason"`
}

type SalesReason struct {
	SalesReasonID int64  `json:"SalesReasonID"`
	Name          string `json:"Name"`
	ReasonType    string `json:"ReasonType"`
}

func readFileSalesReason() {
	data, _ := ioutil.ReadFile("./export_json/SalesReason.json")
	var res SalesReasonDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Reason": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("SalesReason"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i int64 = 0
	for _, item := range res.SalesReasonDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Reason": map[string][]byte{
				"Name":       []byte(item.Name),
				"ReasonType": []byte(item.ReasonType),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "SalesReason", strconv.FormatInt(i, 10), values)
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

func ImportSalesReason() {
	readFileSalesReason()
	fmt.Println("Done SalesReason")
}
