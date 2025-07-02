package processor

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"

	"analytics-in-go/src/config"
	"analytics-in-go/src/model"
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
	// Group by columns without aggregate
	var groupByCols []string
	var aggCols []config.Column
	for _, col := range cfg.Columns {
		if col.Aggregate == "" {
			groupByCols = append(groupByCols, col.Name)
		} else {
			aggCols = append(aggCols, col)
		}
	}

	groupMap := make(map[string]AggregatedRow)

	for _, row := range rows {
		key := ""
		for _, col := range groupByCols {
			key += row[col] + "|"
		}

		if _, exists := groupMap[key]; !exists {
			groupMap[key] = make(AggregatedRow)
			for _, col := range groupByCols {
				groupMap[key][col] = row[col]
			}
		}

		result := groupMap[key]
		for _, col := range aggCols {
			switch col.Aggregate {
			case "sum":
				val, _ := strconv.ParseFloat(row[col.Name], 64)
				if curr, ok := result[col.Name].(float64); ok {
					result[col.Name] = curr + val
				} else {
					result[col.Name] = val
				}
			case "count":
				if curr, ok := result[col.Name].(int); ok {
					result[col.Name] = curr + 1
				} else {
					result[col.Name] = 1
				}
			default:
				return nil, fmt.Errorf("unsupported aggregate: %s", col.Aggregate)
			}
		}
	}

	// Convert map to slice
	var resultList []AggregatedRow
	for _, v := range groupMap {
		resultList = append(resultList, v)
	}

	// Sort if needed
	if cfg.SortBy.Column != "" {
		sort.Slice(resultList, func(i, j int) bool {
			vi, vj := resultList[i][cfg.SortBy.Column], resultList[j][cfg.SortBy.Column]
			if cfg.SortBy.Order == "desc" {
				switch vi := vi.(type) {
				case float64:
					return vi > vj.(float64)
				case int:
					return vi > vj.(int)
				case string:
					return vi > vj.(string)
				}
			} else {
				switch vi := vi.(type) {
				case float64:
					return vi < vj.(float64)
				case int:
					return vi < vj.(int)
				case string:
					return vi < vj.(string)
				}
			}
			return false
		})
	}

	return resultList, nil
}
