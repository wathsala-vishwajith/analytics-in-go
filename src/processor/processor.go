package processor

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"go-data-preprocessor/config"
	"go-data-preprocessor/model"
)

type AggregatedRow map[string]interface{}

func LoadCSV(path string) ([]model.Row, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var rows []model.Row
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		row := make(model.Row)
		for i, h := range headers {
			row[h] = record[i]
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func Aggregate(rows []model.Row, cfg *config.Config) ([]AggregatedRow, error) {
	groupMap := make(map[string]AggregatedRow)

	for _, row := range rows {
		key := ""
		for _, col := range cfg.GroupBy {
			key += row[col] + "|"
		}

		if _, exists := groupMap[key]; !exists {
			groupMap[key] = make(AggregatedRow)
			for _, col := range cfg.GroupBy {
				groupMap[key][col] = row[col]
			}
		}

		result := groupMap[key]
		for _, agg := range cfg.Aggregations {
			switch agg.Operation {
			case "sum":
				val, _ := strconv.ParseFloat(row[agg.Column], 64)
				if curr, ok := result[agg.Name].(float64); ok {
					result[agg.Name] = curr + val
				} else {
					result[agg.Name] = val
				}
			case "count":
				if curr, ok := result[agg.Name].(int); ok {
					result[agg.Name] = curr + 1
				} else {
					result[agg.Name] = 1
				}
			default:
				return nil, fmt.Errorf("unsupported operation: %s", agg.Operation)
			}
		}
	}

	// Convert map to slice
	var resultList []AggregatedRow
	for _, v := range groupMap {
		resultList = append(resultList, v)
	}

	return resultList, nil
}
