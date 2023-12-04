package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func NormalDrop(db *sql.DB, table string) error {
	dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	_, err := db.Exec(dropTableSql)
	if err != nil {
		return err
	}
	return nil
}
