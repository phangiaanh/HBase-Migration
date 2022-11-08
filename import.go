package main

import (
	"context"
	"fmt"
	"hbase-import/models"
	"time"

	"github.com/tsuna/gohbase/hrpc"
)

// disableReq := hrpc.NewDisableTable(context.Background(), []byte("goTest"))
// err = models.HbaseAdminClient.DisableTable(disableReq)
// if err != nil {
// 	log.Println("err: ", err)
// }
// log.Println("3")
// deleteReq := hrpc.NewDeleteTable(context.Background(), []byte("goTest"))
// err = models.HbaseAdminClient.DeleteTable(deleteReq)
// if err != nil {
// 	log.Println("err: ", err)
// }

// disableReq = hrpc.NewDisableTable(context.Background(), []byte("PGA"))
// err = models.HbaseAdminClient.DisableTable(disableReq)
// if err != nil {
// 	log.Println("err: ", err)
// }
// log.Println("3")
// deleteReq = hrpc.NewDeleteTable(context.Background(), []byte("PGA"))
// err = models.HbaseAdminClient.DeleteTable(deleteReq)
// if err != nil {
// 	log.Println("err: ", err)
// }

func main() {
	time.Sleep(5)

	// disableReq := hrpc.NewDisableTable(context.Background(), []byte("CurrencyRate"))
	// err := models.HbaseAdminClient.DisableTable(disableReq)
	// if err != nil {
	// 	log.Println("err: ", err)
	// }
	// log.Println("3")
	// deleteReq := hrpc.NewDeleteTable(context.Background(), []byte("CurrencyRate"))
	// err = models.HbaseAdminClient.DeleteTable(deleteReq)
	// if err != nil {
	// 	log.Println("err: ", err)
	// }

	var currentTables map[string]bool = make(map[string]bool)
	lstTable, err := hrpc.NewListTableNames(context.Background())
	if err != nil {
		fmt.Println(err)
	}
	tables, err := models.HbaseAdminClient.ListTableNames(lstTable)
	if err != nil {
		fmt.Println(err)
	} else {
		for _, item := range tables {
			currentTables[string(item.Qualifier)] = true
			fmt.Println(string(item.Qualifier))
		}
	}

	if ok := currentTables["Customer"]; !ok {
		models.ImportCustomer()
	}

	if ok := currentTables["CountryRegionCurrency"]; !ok {
		models.ImportCountryRegionCurrency()
	}

	if ok := currentTables["CreditCard"]; !ok {
		models.ImportCreditCard()
	}

	if ok := currentTables["Currency"]; !ok {
		models.ImportCurrency()
	}

	if ok := currentTables["CurrencyRate"]; !ok {
		models.ImportCurrencyRate()
	}
}
