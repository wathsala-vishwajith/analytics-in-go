package main

import (
    "fmt"
    "log"

    "go-data-preprocessor/config"
    "go-data-preprocessor/processor"
    "go-data-preprocessor/writer"
)

func main() {
    cfg, err := config.LoadConfig("config/country_revenue.yaml")
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    rows, err := processor.LoadCSV(cfg.InputFile)
    if err != nil {
        log.Fatalf("Failed to load CSV: %v", err)
    }

    aggRows, err := processor.Aggregate(rows, cfg)
    if err != nil {
        log.Fatalf("Failed to aggregate: %v", err)
    }

    err = writer.WriteParquet(cfg.OutputFile, aggRows)
    if err != nil {
        log.Fatalf("Failed to write parquet: %v", err)
    }

    fmt.Println("✅ Preprocessing complete. Output written to:", cfg.OutputFile)
}

