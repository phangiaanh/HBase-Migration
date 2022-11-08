package models

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/tsuna/gohbase/hrpc"
)

type CurrencyDB struct {
	CurrencyDB []Currency `json:"Currency"`
}

type Currency struct {
	CurrencyCode string `json:"CurrencyCode"`
	Name         string `json:"Name"`
}

func readFileCurrency() {
	data, _ := ioutil.ReadFile("./export_json/Currency.json")
	var res CurrencyDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Name": map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("Currency"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i = 0
	for _, item := range res.CurrencyDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Name": map[string][]byte{
				"Name": []byte(item.Name),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "Currency", item.CurrencyCode, values)
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

func ImportCurrency() {
	readFileCurrency()
	fmt.Println("Done Currency")
}
