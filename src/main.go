package main

import (
	"context"
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

	"analytics-in-go/src/config"
	"analytics-in-go/src/processor"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	pqarrow "github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/fsnotify/fsnotify"
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

	// Dynamic config management
	currentConfigs []*config.Config
	configsMu      sync.RWMutex
	httpMux        *http.ServeMux
	httpMuxMu      sync.RWMutex
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
	verboseLog("Loading parquet file: %s", parquetPath)
	// Open the parquet file
	f, err := os.Open(parquetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	// Create parquet file reader
	pf, err := file.NewParquetReader(f, file.WithReadProps(parquet.NewReaderProperties(memory.NewGoAllocator())))
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pf.Close()

	verboseLog("Parquet file metadata - num row groups: %d", pf.NumRowGroups())

	// Create Arrow file reader
	fileReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow file reader: %w", err)
	}

	// Read the entire file as a table
	tbl, err := fileReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	verboseLog("Loaded table with %d rows and %d columns", tbl.NumRows(), tbl.NumCols())
	verboseLog("Schema: %s", tbl.Schema())

	return tbl, nil
}

func getTable(endpoint string, cfg *config.Config) (arrow.Table, error) {
	defer func() {
		if r := recover(); r != nil {
			verboseLog("Panic in getTable for %s: %v", endpoint, r)
		}
	}()

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
	verboseLog("Config details: TableName=%s, OutputParquet=%s", cfg.TableName, cfg.OutputParquet)

	tbl, err := loadParquetToArrow(cfg.OutputParquet)
	if err != nil {
		verboseLog("Failed to load parquet to arrow for %s: %v", endpoint, err)
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
	// Initialize HTTP endpoints
	updateHTTPEndpoints(configs)

	// Create a dynamic handler that uses the current mux
	dynamicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpMuxMu.RLock()
		currentMux := httpMux
		httpMuxMu.RUnlock()

		if currentMux != nil {
			currentMux.ServeHTTP(w, r)
		} else {
			http.Error(w, "Server not ready", http.StatusServiceUnavailable)
		}
	})

	fmt.Println("Serving API on :8080 with real-time config watching...")
	log.Fatal(http.ListenAndServe(":8080", dynamicHandler))
}

func ensureParquetFiles(configs []*config.Config) {
	log.Printf("Ensuring parquet files are ready for %d configs", len(configs))
	for _, cfg := range configs {
		verboseLog("Checking config: %s", cfg.TableName)
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
	verboseLog("All parquet files ensured")
}

func updateConfigs(configDir string) error {
	verboseLog("Reloading configs due to file system change")
	newConfigs, err := LoadAllConfigs(configDir)
	if err != nil {
		log.Printf("Error reloading configs: %v", err)
		return err
	}

	configsMu.Lock()
	oldConfigs := currentConfigs
	currentConfigs = newConfigs
	configsMu.Unlock()

	// Clear cache for removed/changed endpoints
	arrowMu.Lock()
	oldEndpoints := make(map[string]bool)
	for _, cfg := range oldConfigs {
		oldEndpoints[cfg.URLEndpoint] = true
	}

	newEndpoints := make(map[string]bool)
	for _, cfg := range newConfigs {
		newEndpoints[cfg.URLEndpoint] = true
	}

	// Remove cache entries for deleted endpoints
	for endpoint := range oldEndpoints {
		if !newEndpoints[endpoint] {
			delete(arrowTables, endpoint)
			verboseLog("Removed cache for deleted endpoint: %s", endpoint)
		}
	}
	arrowMu.Unlock()

	// Update HTTP endpoints
	updateHTTPEndpoints(newConfigs)

	// Process new/changed configs
	ensureParquetFiles(newConfigs)

	log.Printf("Successfully reloaded %d configs", len(newConfigs))
	return nil
}

func updateHTTPEndpoints(configs []*config.Config) {
	httpMuxMu.Lock()
	defer httpMuxMu.Unlock()

	// Create new mux
	newMux := http.NewServeMux()

	// Add handlers for all current configs
	for _, cfg := range configs {
		endpoint := cfg.URLEndpoint
		currentCfg := cfg // Capture current config to avoid closure issue

		newMux.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if r := recover(); r != nil {
					verboseLog("Panic in handler for %s: %v", endpoint, r)
					http.Error(w, fmt.Sprintf("Internal error: %v", r), http.StatusInternalServerError)
				}
			}()

			verboseLog("Handling request for endpoint: %s", endpoint)
			tbl, err := getTable(endpoint, currentCfg)
			if err != nil {
				verboseLog("Failed to get table for %s: %v", endpoint, err)
				http.Error(w, "Data not loaded: "+err.Error(), http.StatusInternalServerError)
				return
			}

			verboseLog("Got table with %d rows and %d columns", tbl.NumRows(), tbl.NumCols())

			// Convert Arrow Table to JSON (array of maps)
			records := make([]map[string]interface{}, 0)
			for i := int64(0); i < tbl.NumRows(); i++ {
				row := make(map[string]interface{})
				for j, col := range tbl.Schema().Fields() {
					colData := tbl.Column(j)
					chunked := colData.Data()
					if chunked.Len() > 0 {
						chunk := chunked.Chunk(0)
						if int(i) < chunk.Len() {
							switch arr := chunk.(type) {
							case *array.String:
								row[col.Name] = arr.Value(int(i))
							case *array.Int64:
								row[col.Name] = arr.Value(int(i))
							case *array.Float64:
								row[col.Name] = arr.Value(int(i))
							default:
								// Fallback to string representation
								row[col.Name] = chunk.ValueStr(int(i))
							}
						} else {
							verboseLog("Index %d out of range for chunk length %d", i, chunk.Len())
							row[col.Name] = nil
						}
					} else {
						verboseLog("No chunks available for column %s", col.Name)
						row[col.Name] = nil
					}
				}
				records = append(records, row)
			}

			verboseLog("Successfully converted %d records to JSON", len(records))
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(records)
		})

		verboseLog("Registered endpoint: %s", endpoint)
	}

	httpMux = newMux
	verboseLog("Updated HTTP endpoints with %d handlers", len(configs))
}

func watchConfigDir(configDir string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("Failed to create file watcher: %v", err)
		return
	}
	defer watcher.Close()

	err = watcher.Add(configDir)
	if err != nil {
		log.Printf("Failed to watch config directory: %v", err)
		return
	}

	verboseLog("Started watching config directory: %s", configDir)

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			verboseLog("Config file system event: %s %s", event.Op, event.Name)

			// Only react to YAML file changes
			if strings.HasSuffix(event.Name, ".yaml") || strings.HasSuffix(event.Name, ".yml") {
				if event.Op&fsnotify.Write == fsnotify.Write ||
					event.Op&fsnotify.Create == fsnotify.Create ||
					event.Op&fsnotify.Remove == fsnotify.Remove {

					log.Printf("Config change detected: %s", event.Name)

					// Small delay to allow file operations to complete
					time.Sleep(100 * time.Millisecond)

					if err := updateConfigs(configDir); err != nil {
						log.Printf("Failed to update configs: %v", err)
					}
				}
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("Config watcher error: %v", err)
		}
	}
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

	// Initialize current configs
	configsMu.Lock()
	currentConfigs = configs
	configsMu.Unlock()

	// Always ensure parquet files exist (whether serving or preprocessing)
	ensureParquetFiles(configs)

	if *serveFlag {
		log.Printf("Starting in server mode with %d endpoints", len(configs))

		// Start config file watcher in background
		go watchConfigDir(configDir)

		// Start the API server (this blocks)
		serveAPI(configs)
		return
	}

	log.Printf("All preprocessing completed")
}
