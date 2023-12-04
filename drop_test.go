package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	mysqlImage_8_2_0  = "docker.io/mysql:8.2.0"
	mysqlImage_8_0_35 = "docker.io/mysql:8.0.35"
	mysqlImage_5_7_44 = "docker.io/mysql:5.7.44"

	SIZE_1KB  = 0
	SIZE_1MB  = SIZE_1KB + 10
	SIZE_1GB  = SIZE_1MB + 10
	SIZE_16GB = SIZE_1GB + 4

	TEST_TABLE_NAME = "TEST_TABLE"
)

// Created table size bigger than 1 << exponentialSize KB.
// As a rule of thumb, there is overhead size about 30% of data in case of bigger than 1GB.
func createTable(db *sql.DB, table string, exponentialSize int) error {
	// Clear table
	dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	_, err := db.Exec(dropTableSql)
	if err != nil {
		return err
	}

	// Create table has 1024 Byte per row
	createTableSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (a INT,b CHAR(255),c CHAR(255),d CHAR(255),e CHAR(255)) charset=latin1", table)
	_, err = db.Exec(createTableSql)
	if err != nil {
		return err
	}

	insertInitRowSql := fmt.Sprintf("INSERT INTO %s VALUES(1,repeat('x', 255),repeat('x', 255),repeat('x', 255),repeat('x', 255))", table)
	_, err = db.Exec(insertInitRowSql)
	if err != nil {
		return err
	}

	bloatTableSql := fmt.Sprintf("INSERT INTO %[1]s SELECT * FROM %[1]s", table)
	for i := 0; i < exponentialSize; i++ {
		_, err = db.Exec(bloatTableSql)
		if err != nil {
			return err
		}
	}

	return nil
}

func createContainer(image string) (testcontainers.Container, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "password",
			"MYSQL_DATABASE":      "database",
		},
		WaitingFor: wait.ForExposedPort(),
	}
	mysqlC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return mysqlC, nil
}

func Test_NormalDrop_Mysql8_2_0_1GB(t *testing.T) {
	mysqlC, err := createContainer(mysqlImage_8_2_0)
	if err != nil {
		log.Fatal(err)
	}
	endpoint, _ := mysqlC.Endpoint(context.Background(), "tcp")

	db, err := sql.Open("mysql", fmt.Sprintf("root:password@tcp(%s)/database", strings.TrimPrefix(endpoint, "tcp://")))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = createTable(db, TEST_TABLE_NAME, SIZE_1GB)
	if err != nil {
		log.Fatal(err)
	}

	beforeDiskIo, _ := disk.IOCounters()
	beforeTime := time.Now()
	NormalDrop(db, TEST_TABLE_NAME)
	afterTime := time.Now()
	afterDiskIo, _ := disk.IOCounters()

	log.Printf("drop table takes %v ", afterTime.Sub(beforeTime).String())

	var ReadCount,
		MergedReadCount,
		WriteCount,
		MergedWriteCount,
		ReadBytes,
		WriteBytes,
		ReadTime,
		WriteTime,
		IopsInProgress,
		IoTime,
		WeightedIO uint64

	for k, v := range beforeDiskIo {
		ReadCount += afterDiskIo[k].ReadCount - v.ReadCount
		MergedReadCount += afterDiskIo[k].MergedReadCount - v.MergedReadCount
		WriteCount += afterDiskIo[k].WriteCount - v.WriteCount
		MergedWriteCount += afterDiskIo[k].MergedWriteCount - v.MergedWriteCount
		ReadBytes += afterDiskIo[k].ReadBytes - v.ReadBytes
		WriteBytes += afterDiskIo[k].WriteBytes - v.WriteBytes
		ReadTime += afterDiskIo[k].ReadTime - v.ReadTime
		WriteTime += afterDiskIo[k].WriteTime - v.WriteTime
		IopsInProgress += afterDiskIo[k].IopsInProgress - v.IopsInProgress
		IoTime += afterDiskIo[k].IoTime - v.IoTime
		WeightedIO += afterDiskIo[k].WeightedIO - v.WeightedIO
	}
	log.Printf("drop table cause %s %d", "ReadCount", ReadCount)
	log.Printf("drop table cause %s %d", "MergedReadCount", MergedReadCount)
	log.Printf("drop table cause %s %d", "WriteCount", WriteCount)
	log.Printf("drop table cause %s %d", "MergedWriteCount", MergedWriteCount)
	log.Printf("drop table cause %s %d", "ReadBytes", ReadBytes)
	log.Printf("drop table cause %s %d", "WriteBytes", WriteBytes)
	log.Printf("drop table cause %s %d", "ReadTime", ReadTime)
	log.Printf("drop table cause %s %d", "WriteTime", WriteTime)
	log.Printf("drop table cause %s %d", "IopsInProgress", IopsInProgress)
	log.Printf("drop table cause %s %d", "IoTime", IoTime)
	log.Printf("drop table cause %s %d", "WeightedIO", WeightedIO)
}
