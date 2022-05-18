// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START bigtable_functions_quickstart]

// Package bigtable contains an example of using Bigtable from a Cloud Function.
package bigtable

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"

	"cloud.google.com/go/bigtable"
)

const (
	tableName        = "test-table"
	columnFamilyName = "cf1"
	columnName       = "greeting"
)

// client is a global Bigtable client, to avoid initializing a new client for
// every request.
var client *bigtable.Client
var clientOnce sync.Once

// BigtableRead is an example of reading Bigtable from a Cloud Function.
func BigtableRead(w http.ResponseWriter, r *http.Request) {

	project := flag.String("striped-proxy-187410", "", "The Google Cloud Platform project ID. Required.")
	instance := flag.String("test-instance", "", "The Google Cloud Bigtable instance ID. Required.")

	clientOnce.Do(func() {
		// Declare a separate err variable to avoid shadowing client.
		var err error
		client, err = bigtable.NewClient(context.Background(), *project, *instance)
		if err != nil {
			http.Error(w, "Error initializing client", http.StatusInternalServerError)
			log.Printf("bigtable.NewClient: %v", err)
			return
		}
	})

	var greetings = []string{"Hello World!", "Hello Cloud Bigtable!", "Hello golang!"}
	tbl := client.Open(r.Header.Get("tableID"))

	muts := make([]*bigtable.Mutation, len(greetings))
	rowKeys := make([]string, len(greetings))

	log.Printf("Writing greeting rows to table")
	for i, greeting := range greetings {
		muts[i] = bigtable.NewMutation()
		muts[i].Set(columnFamilyName, columnName, bigtable.Now(), []byte(greeting))

		// Each row has a unique row key.
		//
		// Note: This example uses sequential numeric IDs for simplicity, but
		// this can result in poor performance in a production application.
		// Since rows are stored in sorted order by key, sequential keys can
		// result in poor distribution of operations across nodes.
		//
		// For more information about how to design a Bigtable schema for the
		// best performance, see the documentation:
		//
		//     https://cloud.google.com/bigtable/docs/schema-design
		rowKeys[i] = fmt.Sprintf("%s%d", columnName, i)
	}

	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		log.Fatalf("Could not apply bulk row mutation: %v", err)
	}
	if rowErrs != nil {
		for _, rowErr := range rowErrs {
			log.Printf("Error writing row: %v", rowErr)
		}
		log.Fatalf("Could not write some rows")
	}

	// [START bigtable_hw_get_by_key]
	log.Printf("Getting a single greeting by row key:")
	row, err := tbl.ReadRow(ctx, rowKeys[0], bigtable.RowFilter(bigtable.ColumnFilter(columnName)))
	if err != nil {
		log.Fatalf("Could not read row with key %s: %v", rowKeys[0], err)
	}
	log.Printf("\t%s = %s\n", rowKeys[0], string(row[columnFamilyName][0].Value))
	// [END bigtable_hw_get_by_key]

	err := tbl.ReadRows(r.Context(), bigtable.PrefixRange("phone#"),
		func(row bigtable.Row) bool {
			osBuild := ""
			for _, col := range row["stats_summary"] {
				if col.Column == "stats_summary:os_build" {
					osBuild = string(col.Value)
				}
			}

			fmt.Fprintf(w, "Rowkey: %s, os_build:  %s\n", row.Key(), osBuild)
			return true
		})

	if err != nil {
		http.Error(w, "Error reading rows", http.StatusInternalServerError)
		log.Printf("tbl.ReadRows(): %v", err)
	}
}

// [END bigtable_functions_quickstart]
