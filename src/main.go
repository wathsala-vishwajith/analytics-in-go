package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"analytics-in-go/src/config"
	"analytics-in-go/src/processor"
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

func main() {
	configDir := "config"
	configs, err := LoadAllConfigs(configDir)
	if err != nil {
		log.Fatalf("Error loading configs: %v", err)
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
