package processor

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"analytics-in-go/src/config"
	"analytics-in-go/src/model"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	pqarrow "github.com/apache/arrow/go/v14/parquet/pqarrow"
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
	// For avg, keep sum and count per group
	avgSums := make(map[string]map[string]float64) // key -> col.Name -> sum
	avgCounts := make(map[string]map[string]int)   // key -> col.Name -> count

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
			avgSums[key] = make(map[string]float64)
			avgCounts[key] = make(map[string]int)
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
			case "avg":
				val, _ := strconv.ParseFloat(row[col.Name], 64)
				avgSums[key][col.Name] += val
				avgCounts[key][col.Name]++
			default:
				return nil, fmt.Errorf("unsupported aggregate: %s", col.Aggregate)
			}
		}
	}

	// After processing all rows, compute averages
	for key, row := range groupMap {
		for _, col := range aggCols {
			if col.Aggregate == "avg" {
				sum := avgSums[key][col.Name]
				count := avgCounts[key][col.Name]
				if count > 0 {
					row[col.Name] = sum / float64(count)
				} else {
					row[col.Name] = 0.0
				}
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

	// Apply limit if specified
	if cfg.Limit > 0 && len(resultList) > cfg.Limit {
		resultList = resultList[:cfg.Limit]
	}

	return resultList, nil
}

func generateParquetSchema(row AggregatedRow) (string, error) {
	fields := []string{}
	// To maintain a consistent order for columns in the schema
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, name := range keys {
		val := row[name]
		var parquetType string
		switch val.(type) {
		case string:
			parquetType = "BYTE_ARRAY"
		case int, int64:
			parquetType = "INT64"
		case float64, float32:
			parquetType = "DOUBLE"
		default:
			// Fallback for safety, though aggregation logic should prevent this
			parquetType = "BYTE_ARRAY"
		}
		fields = append(fields, fmt.Sprintf(`{"Tag": "name=%s, type=%s, repetitiontype=REQUIRED"}`, name, parquetType))
	}

	schema := fmt.Sprintf(`{
		"Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
		"Fields": [%s]
	}`, strings.Join(fields, ","))

	return schema, nil
}

func WriteParquet(outputPath string, data []AggregatedRow) error {
	if len(data) == 0 {
		log.Println("No data to write to parquet, skipping file creation.")
		return nil
	}

	log.Printf("Writing %d rows to parquet file: %s", len(data), outputPath)

	// Use direct parquet writer approach for better performance
	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// Build Arrow schema
	fields := make([]arrow.Field, 0, len(data[0]))
	for key, value := range data[0] {
		var dataType arrow.DataType
		switch value.(type) {
		case string:
			dataType = arrow.BinaryTypes.String
		case int, int64:
			dataType = arrow.PrimitiveTypes.Int64
		case float64:
			dataType = arrow.PrimitiveTypes.Float64
		default:
			dataType = arrow.BinaryTypes.String
		}
		fields = append(fields, arrow.Field{Name: key, Type: dataType})
	}

	// Sort fields for consistent ordering
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})

	schema := arrow.NewSchema(fields, nil)

	// Create parquet writer
	props := parquet.NewWriterProperties()
	arrowProps := pqarrow.DefaultWriterProps()

	pqWriter, err := pqarrow.NewFileWriter(schema, f, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer pqWriter.Close()

	// Write data in batches to avoid memory issues
	const batchSize = 10000
	pool := memory.NewGoAllocator()

	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		// Build record batch
		builders := make([]array.Builder, len(fields))
		for j, field := range fields {
			switch field.Type {
			case arrow.BinaryTypes.String:
				builders[j] = array.NewStringBuilder(pool)
			case arrow.PrimitiveTypes.Int64:
				builders[j] = array.NewInt64Builder(pool)
			case arrow.PrimitiveTypes.Float64:
				builders[j] = array.NewFloat64Builder(pool)
			}
		}

		// Add data to builders
		for rowIdx := i; rowIdx < end; rowIdx++ {
			row := data[rowIdx]
			for j, field := range fields {
				value := row[field.Name]
				switch builder := builders[j].(type) {
				case *array.StringBuilder:
					if str, ok := value.(string); ok {
						builder.Append(str)
					} else {
						builder.Append(fmt.Sprintf("%v", value))
					}
				case *array.Int64Builder:
					if i64, ok := value.(int64); ok {
						builder.Append(i64)
					} else if f64, ok := value.(float64); ok {
						builder.Append(int64(f64))
					} else {
						builder.Append(0)
					}
				case *array.Float64Builder:
					if f64, ok := value.(float64); ok {
						builder.Append(f64)
					} else {
						builder.Append(0)
					}
				}
			}
		}

		// Build arrays
		columns := make([]arrow.Array, len(builders))
		for j, builder := range builders {
			columns[j] = builder.NewArray()
		}

		// Create record
		record := array.NewRecord(schema, columns, int64(end-i))

		// Write record
		if err := pqWriter.Write(record); err != nil {
			record.Release()
			return fmt.Errorf("failed to write batch %d: %w", i/batchSize, err)
		}

		record.Release()
		for _, col := range columns {
			col.Release()
		}
		for _, builder := range builders {
			builder.Release()
		}
	}

	log.Printf("Successfully wrote %d rows to %s", len(data), outputPath)
	return nil
}

func Preprocess(cfg *config.Config) error {
	rows, err := LoadCSV(cfg.InputCSV)
	if err != nil {
		return fmt.Errorf("failed to load CSV: %w", err)
	}

	aggRows, err := Aggregate(rows, cfg)
	if err != nil {
		return fmt.Errorf("failed to aggregate: %w", err)
	}

	err = WriteParquet(cfg.OutputParquet, aggRows)
	if err != nil {
		return fmt.Errorf("failed to write parquet: %w", err)
	}

	return nil
}
