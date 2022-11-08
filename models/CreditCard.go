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

type CreditCardDB struct {
	CreditCardDB []CreditCard `json:"CreditCard"`
}

type CreditCard struct {
	CreditCardID int64  `json:"CreditCardID"`
	CardType     string `json:"CardType"`
	CardNumber   string `json:"CardNumber"`
	ExpMonth     int64  `json:"ExpMonth"`
	ExpYear      int64  `json:"ExpYear"`
}

func readFileCreditCard() {
	data, _ := ioutil.ReadFile("./export_json/CreditCard.json")
	var res CreditCardDB
	err := json.Unmarshal(data, &res)
	if err != nil {
		fmt.Println(err)
	}

	columns := map[string]map[string]string{
		"Card": map[string]string{},
		"Exp":  map[string]string{},
	}
	createReq := hrpc.NewCreateTable(context.Background(), []byte("CreditCard"), columns)
	err = HbaseAdminClient.CreateTable(createReq)
	if err != nil {
		log.Println("err:", err)
	}

	var i = 0
	for _, item := range res.CreditCardDB {
		i = i + 1
		values := map[string]map[string][]byte{
			"Card": map[string][]byte{
				"Type":   []byte(item.CardType),
				"Number": []byte(item.CardNumber),
			},
			"Exp": map[string][]byte{
				"Month": []byte(strconv.FormatInt(item.ExpMonth, 10)),
				"Year":  []byte(strconv.FormatInt(item.ExpYear, 10)),
			},
		}
		putRequest, err := hrpc.NewPutStr(context.Background(), "CreditCard", strconv.FormatInt(item.CreditCardID, 10), values)
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

func ImportCreditCard() {
	readFileCreditCard()
	fmt.Println("Done ImportCreditCard")
}
