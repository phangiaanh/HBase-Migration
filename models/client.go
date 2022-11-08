package models

import (
	"log"

	hbase "github.com/tsuna/gohbase"
)

var (
	HbaseClient      hbase.Client
	HbaseAdminClient hbase.AdminClient
)

func init() {
	HbaseAdminClient = hbase.NewAdminClient("hbase-docker")
	HbaseClient = hbase.NewClient("hbase-docker")
	log.Println("AAA")
}
