package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"analytics-in-go/src/config"
	"analytics-in-go/src/processor"
)

var (
	arrowTables = make(map[string]array.Table) // endpoint -> Arrow Table
	arrowMu     sync.RWMutex
)

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashFilePath(csvPath string) string {
	base := filepath.Base(csvPath)
	return filepath.Join("data/raw", base+".hash")
}

func readSavedHash(csvPath string) (string, error) {
	hashPath := hashFilePath(csvPath)
	data, err := os.ReadFile(hashPath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func saveHash(csvPath, hash string) error {
	hashPath := hashFilePath(csvPath)
	return os.WriteFile(hashPath, []byte(hash), 0644)
}

func LoadAllConfigs(configDir string) ([]*config.Config, error) {
	var configs []*config.Config
	err := filepath.WalkDir(configDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".yaml") {
			return nil
		}
		cfg, err := config.LoadConfig(path)
		if err != nil {
			log.Printf("Failed to load config %s: %v", path, err)
			return nil
		}
		configs = append(configs, cfg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return configs, nil
}

func loadParquetToArrow(parquetPath string) (arrow.Table, error) {
	fr, err := local.NewLocalFileReader(parquetPath)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	if num == 0 {
		return nil, fmt.Errorf("no rows in parquet file")
	}
	rows := make([]map[string]interface{}, num)
	if err := pr.Read(&rows); err != nil {
		return nil, err
	}

	pool := memory.NewGoAllocator()
	builders := make(map[string]*array.StringBuilder)
	for k := range rows[0] {
		builders[k] = array.NewStringBuilder(pool)
	}
	for _, row := range rows {
		for k, v := range row {
			builders[k].Append(fmt.Sprintf("%v", v))
		}
	}
	fields := make([]arrow.Field, 0, len(builders))
	arrs := make([]arrow.Array, 0, len(builders))
	for k, b := range builders {
		fields = append(fields, arrow.Field{Name: k, Type: arrow.BinaryTypes.String})
		arrs = append(arrs, b.NewArray())
		b.Release()
	}
	schema := arrow.NewSchema(fields, nil)
	tbl := array.NewTable(schema, arrs, int64(num))
	return tbl, nil
}

func serveAPI(configs []*config.Config) {
	// Load all Arrow tables into memory
	for _, cfg := range configs {
		tbl, err := loadParquetToArrow(cfg.OutputParquet)
		if err != nil {
			log.Printf("Failed to load Arrow table for %s: %v", cfg.TableName, err)
			continue
		}
		arrowMu.Lock()
		arrowTables[cfg.URLEndpoint] = tbl
		arrowMu.Unlock()
	}
	for _, cfg := range configs {
		endpoint := cfg.URLEndpoint
		http.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
			arrowMu.RLock()
			tbl, ok := arrowTables[endpoint]
			arrowMu.RUnlock()
			if !ok {
				http.Error(w, "Data not loaded", http.StatusInternalServerError)
				return
			}
			// Convert Arrow Table to JSON (array of maps)
			records := make([]map[string]interface{}, 0)
			for i := int64(0); i < tbl.NumRows(); i++ {
				row := make(map[string]interface{})
				for j, col := range tbl.Schema().Fields() {
					arr := tbl.Column(j).Data()
					row[col.Name] = arr.(*array.String).Value(int(i))
				}
				records = append(records, row)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(records)
		})
	}
	fmt.Println("Serving API on :8080 ...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func main() {
	serveFlag := flag.Bool("serve", false, "Run as web server (REST API)")
	flag.Parse()

	configDir := "config"
	configs, err := LoadAllConfigs(configDir)
	if err != nil {
		log.Fatalf("Error loading configs: %v", err)
	}

	if *serveFlag {
		serveAPI(configs)
		return
	}

	for _, cfg := range configs {
		parquetExists := false
		if _, err := os.Stat(cfg.OutputParquet); err == nil {
			parquetExists = true
		}
		currentHash, err := fileHash(cfg.InputCSV)
		if err != nil {
			log.Printf("Failed to hash CSV for config %s: %v", cfg.TableName, err)
			continue
		}
		savedHash, err := readSavedHash(cfg.InputCSV)
		if parquetExists && err == nil && savedHash == currentHash {
			fmt.Printf("Parquet up-to-date for %s, skipping.\n", cfg.TableName)
			continue
		}
		if err := processor.Preprocess(cfg); err != nil {
			log.Printf("Preprocessing failed for %s: %v", cfg.TableName, err)
			continue
		}
		if err := saveHash(cfg.InputCSV, currentHash); err != nil {
			log.Printf("Failed to save hash for %s: %v", cfg.TableName, err)
		}
		fmt.Printf("✅ Preprocessing complete. Output written to: %s\n", cfg.OutputParquet)
	}
}
