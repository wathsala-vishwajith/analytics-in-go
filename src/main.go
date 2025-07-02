package main

import (
	"fmt"
	"log"

	"analytics-in-go/src/config"
	"analytics-in-go/src/processor"
	"analytics-in-go/src/writer"
)

func main() {
	cfg, err := config.LoadConfig("config/country_level_revenue.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	rows, err := processor.LoadCSV(cfg.InputCSV)
	if err != nil {
		log.Fatalf("Failed to load CSV: %v", err)
	}

	aggRows, err := processor.Aggregate(rows, cfg)
	if err != nil {
		log.Fatalf("Failed to aggregate: %v", err)
	}

	err = writer.WriteParquet(cfg.OutputParquet, aggRows)
	if err != nil {
		log.Fatalf("Failed to write parquet: %v", err)
	}

	fmt.Println("✅ Preprocessing complete. Output written to:", cfg.OutputParquet)
}
