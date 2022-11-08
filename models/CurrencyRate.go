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

type CurrencyRateDB struct {
	CurrencyRateDB []CurrencyRate `json:"CurrencyRate"`
}

type CurrencyRate struct {
	CurrencyRateID   int64  `json:"CurrencyRateID"`
	CurrencyRateDate string `json:"CurrencyRateDate"`
	FromCurrencyCode string `json:"FromCurrencyCode"`
	ToCurrencyCode   string `json:"ToCurrencyCode"`
	AverageRate      string `json:"AverageRate"`
	EndOfDayRate     string `json:"EndOfDayRate"`
}

func readFileCurrencyRate() {
	data, _ := ioutil.ReadFile("./export_json/CurrencyRate.json")
	var res CurrencyRateDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Exchange": map[string]string{},
		"Rate":     map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("CurrencyRate"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i = 0
	for _, item := range res.CurrencyRateDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Exchange": map[string][]byte{
				"From": []byte(item.FromCurrencyCode),
				"To":   []byte(item.ToCurrencyCode),
			},
			"Rate": map[string][]byte{
				"Date":    []byte(item.CurrencyRateDate),
				"Average": []byte(item.AverageRate),
				"EOD":     []byte(item.EndOfDayRate),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "CurrencyRate", strconv.FormatInt(item.CurrencyRateID, 10), values)
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

func ImportCurrencyRate() {
	readFileCurrencyRate()
	fmt.Println("Done CurrencyRate")
}
