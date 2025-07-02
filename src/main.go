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
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"analytics-in-go/src/config"
	"analytics-in-go/src/processor"
)

type TableCache struct {
	Table      arrow.Table
	LoadedAt   time.Time
	AccessedAt time.Time
}

var (
	arrowTables = make(map[string]*TableCache) // endpoint -> TableCache
	arrowMu     sync.RWMutex
	maxAge      = 1 * time.Hour // Unload after 1 hour of no access
	verbose     bool
)

func verboseLog(format string, args ...interface{}) {
	if verbose {
		log.Printf("[VERBOSE] "+format, args...)
	}
}

func fileHash(path string) (string, error) {
	verboseLog("Computing hash for file: %s", path)
	f, err := os.Open(path)
	if err != nil {
		verboseLog("Failed to open file %s: %v", path, err)
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		verboseLog("Failed to read file %s: %v", path, err)
		return "", err
	}
	hash := hex.EncodeToString(h.Sum(nil))
	verboseLog("Computed hash for %s: %s", path, hash[:12]+"...")
	return hash, nil
}

func hashFilePath(csvPath string) string {
	base := filepath.Base(csvPath)
	dir := filepath.Dir(csvPath)
	hashPath := filepath.Join(dir, base+".hash")
	verboseLog("Hash file path for %s: %s", csvPath, hashPath)
	return hashPath
}

func readSavedHash(csvPath string) (string, error) {
	hashPath := hashFilePath(csvPath)
	verboseLog("Reading saved hash from: %s", hashPath)
	data, err := os.ReadFile(hashPath)
	if err != nil {
		verboseLog("No saved hash found for %s: %v", csvPath, err)
		return "", err
	}
	hash := strings.TrimSpace(string(data))
	verboseLog("Read saved hash for %s: %s", csvPath, hash[:12]+"...")
	return hash, nil
}

func saveHash(csvPath, hash string) error {
	hashPath := hashFilePath(csvPath)
	verboseLog("Saving hash for %s to %s", csvPath, hashPath)
	err := os.WriteFile(hashPath, []byte(hash), 0644)
	if err != nil {
		verboseLog("Failed to save hash: %v", err)
	} else {
		verboseLog("Hash saved successfully")
	}
	return err
}

func LoadAllConfigs(configDir string) ([]*config.Config, error) {
	verboseLog("Loading configs from directory: %s", configDir)
	var configs []*config.Config
	err := filepath.WalkDir(configDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			verboseLog("Error walking directory %s: %v", path, err)
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".yaml") {
			verboseLog("Skipping non-YAML file: %s", d.Name())
			return nil
		}
		verboseLog("Loading config file: %s", path)
		cfg, err := config.LoadConfig(path)
		if err != nil {
			log.Printf("Failed to load config %s: %v", path, err)
			return nil
		}
		log.Printf("Loaded config %s", path)
		verboseLog("Config details - Table: %s, Input: %s, Output: %s, Endpoint: %s",
			cfg.TableName, cfg.InputCSV, cfg.OutputParquet, cfg.URLEndpoint)
		configs = append(configs, cfg)
		return nil
	})
	if err != nil {
		verboseLog("Error loading configs: %v", err)
		return nil, err
	}
	verboseLog("Successfully loaded %d config files", len(configs))
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
	columns := make([]arrow.Column, 0, len(builders))
	for k, b := range builders {
		field := arrow.Field{Name: k, Type: arrow.BinaryTypes.String}
		fields = append(fields, field)
		arr := b.NewArray()
		chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{arr})
		col := arrow.NewColumn(field, chunked)
		columns = append(columns, *col)
		b.Release()
	}
	schema := arrow.NewSchema(fields, nil)
	tbl := array.NewTable(schema, columns, int64(num))
	return tbl, nil
}

func getTable(endpoint string, cfg *config.Config) (arrow.Table, error) {
	verboseLog("Getting table for endpoint: %s", endpoint)
	arrowMu.RLock()
	cache, exists := arrowTables[endpoint]
	arrowMu.RUnlock()

	if exists {
		verboseLog("Table found in cache for %s", endpoint)
		if time.Since(cache.AccessedAt) > maxAge {
			verboseLog("Table cache expired for %s, removing from memory", endpoint)
			arrowMu.Lock()
			delete(arrowTables, endpoint)
			arrowMu.Unlock()
		} else {
			verboseLog("Updating access time for cached table %s", endpoint)
			arrowMu.Lock()
			cache.AccessedAt = time.Now()
			arrowMu.Unlock()
			return cache.Table, nil
		}
	}

	verboseLog("Loading table into cache for %s", endpoint)
	tbl, err := loadParquetToArrow(cfg.OutputParquet)
	if err != nil {
		return nil, err
	}

	arrowMu.Lock()
	arrowTables[endpoint] = &TableCache{
		Table:      tbl,
		LoadedAt:   time.Now(),
		AccessedAt: time.Now(),
	}
	arrowMu.Unlock()
	verboseLog("Table cached successfully for %s", endpoint)

	return tbl, nil
}

func serveAPI(configs []*config.Config) {
	for _, cfg := range configs {
		endpoint := cfg.URLEndpoint
		http.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
			tbl, err := getTable(endpoint, cfg)
			if err != nil {
				http.Error(w, "Data not loaded: "+err.Error(), http.StatusInternalServerError)
				return
			}
			// Convert Arrow Table to JSON (array of maps)
			records := make([]map[string]interface{}, 0)
			for i := int64(0); i < tbl.NumRows(); i++ {
				row := make(map[string]interface{})
				for j, col := range tbl.Schema().Fields() {
					colData := tbl.Column(j)
					chunked := colData.Data()
					if chunked.Len() > 0 {
						arr := chunked.Chunk(0).(*array.String)
						row[col.Name] = arr.Value(int(i))
					}
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
	verboseFlag := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	verbose = *verboseFlag
	if verbose {
		log.Println("Verbose logging enabled")
	}

	configDir := "config"
	verboseLog("Starting application in directory: %s", configDir)
	configs, err := LoadAllConfigs(configDir)
	if err != nil {
		log.Fatalf("Error loading configs: %v", err)
	}

	if *serveFlag {
		log.Printf("Starting in server mode with %d endpoints", len(configs))
		serveAPI(configs)
		return
	}

	log.Printf("Starting preprocessing for %d configs", len(configs))
	for _, cfg := range configs {
		verboseLog("Processing config: %s", cfg.TableName)
		parquetExists := false
		if _, err := os.Stat(cfg.OutputParquet); err == nil {
			parquetExists = true
			verboseLog("Parquet file exists: %s", cfg.OutputParquet)
		} else {
			verboseLog("Parquet file does not exist: %s", cfg.OutputParquet)
		}

		currentHash, err := fileHash(cfg.InputCSV)
		if err != nil {
			log.Printf("Failed to hash CSV for config %s: %v", cfg.TableName, err)
			continue
		}

		savedHash, err := readSavedHash(cfg.InputCSV)
		if parquetExists && err == nil && savedHash == currentHash {
			fmt.Printf("Parquet up-to-date for %s, skipping.\n", cfg.TableName)
			verboseLog("Hash matches, skipping processing for %s", cfg.TableName)
			continue
		}

		verboseLog("Hash differs or file missing, processing %s", cfg.TableName)

		// Ensure output directory exists
		outputDir := filepath.Dir(cfg.OutputParquet)
		verboseLog("Creating output directory: %s", outputDir)
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Printf("Failed to create output directory for %s: %v", cfg.TableName, err)
			continue
		}

		verboseLog("Starting preprocessing for %s", cfg.TableName)
		processingStart := time.Now()
		if err := processor.Preprocess(cfg); err != nil {
			log.Printf("Preprocessing failed for %s: %v", cfg.TableName, err)
			continue
		}
		verboseLog("Preprocessing completed in %v for %s", time.Since(processingStart), cfg.TableName)

		if err := saveHash(cfg.InputCSV, currentHash); err != nil {
			log.Printf("Failed to save hash for %s: %v", cfg.TableName, err)
		}
		fmt.Printf("✅ Preprocessing complete. Output written to: %s\n", cfg.OutputParquet)
	}
	verboseLog("All preprocessing completed")
}
