package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// neighborhoodsFileHasData returns true if the neighborhoods file exists and contains a non-empty list.
// Supports both formats:
// 1) {"neighborhoods": [{"name": "...","lat": x,"lng": y}, ...]}
// 2) {"neighborhoods": ["Name1","Name2", ...]}
func neighborhoodsFileHasData(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return false
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return false
	}
	// Try enriched format
	var enriched struct {
		Neighborhoods []struct {
			Name string  `json:"name"`
			Lat  float64 `json:"lat"`
			Lng  float64 `json:"lng"`
		} `json:"neighborhoods"`
	}
	if err := json.Unmarshal(b, &enriched); err == nil && len(enriched.Neighborhoods) > 0 {
		return true
	}
	// Try names-only format
	var names struct {
		Neighborhoods []string `json:"neighborhoods"`
	}
	if err := json.Unmarshal(b, &names); err == nil && len(names.Neighborhoods) > 0 {
		return true
	}
	return false
}

func loadEnvFiles() {
	// Try current dir, parent, and content-generation .env
	paths := []string{
		".env",
		filepath.Join("..", "content-generation", ".env"),
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			_ = godotenv.Load(p)
		}
	}
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func getenvBool(key string, def bool) bool {
	v := strings.ToLower(getenv(key, ""))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes"
}

func getenvInt(key string, def int) int {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func getenvFloat(key string, def float64) float64 {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func main() {
	loadEnvFiles()

	city := getenv("CITY", "")
	if city == "" {
		log.Fatal("CITY is required in .env")
	}

	// Neighborhoods file path (shared by finder and collector)
	neighborhoodsFile := getenv("NEIGHBORHOODS_FILE", "./neighborhoods.json")

	// Determine whether to skip finder
	skipFinder := getenvBool("SKIP_FINDER", false)
	autoSkipIfExists := getenvBool("FINDER_SKIP_IF_EXISTS", true)
	hasData := neighborhoodsFileHasData(neighborhoodsFile)

	if skipFinder {
		if !hasData {
			log.Fatalf("SKIP_FINDER=true but %s is missing or empty. Provide a valid file or disable SKIP_FINDER.", neighborhoodsFile)
		}
		log.Printf("Skipping Finder (SKIP_FINDER=true). Using existing neighborhoods from %s", neighborhoodsFile)
	} else if autoSkipIfExists && hasData {
		log.Printf("Skipping Finder (FINDER_SKIP_IF_EXISTS=true and %s already populated).", neighborhoodsFile)
	} else {
		// 1) Run Finder to get neighborhoods (writes to NEIGHBORHOODS_FILE if set)
		// finderCfg := &FinderConfig{
		// 	City:           city,
		// 	Query:          getenv("FINDER_QUERY", ""),                      // optional custom query
		// 	LLM:            strings.ToLower(getenv("FINDER_LLM", "gemini")), // openai|gemini
		// 	MaxResults:     getenvInt("FINDER_MAX_RESULTS", 8),
		// 	SkipTavily:     getenvBool("FINDER_SKIP_TAVILY", false),
		// 	ExtractContent: getenvBool("FINDER_EXTRACT", true),
		// 	OutputFile:     neighborhoodsFile, // write neighborhoods to file for the next step
		// 	// New: include OSM-derived neighborhoods as context + merge preference
		// 	UseOSMContext: getenvBool("FINDER_USE_OSM", true),
		// }

		// out, err := RunFinder(finderCfg)
		// if err != nil {
		// 	log.Fatalf("finder failed: %v", err)
		// }
		cfg := &FinderAdvConfig{
			City:                "Edinburgh",
			LLM:                 "gemini", // or "gemini"
			MaxResults:          20,
			EnableMultiPhase:    true,
			PreferOSMBoundaries: true,
			MinConfidenceScore:  0.4,
			EnableValidation:    true,
			OutputFile:          "edi_neighborhoods.json",
			EnableCaching:       true,
		}

		out, err := RunAdvancedFinder(cfg)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Found %d neighborhoods with quality score: %.2f\n",
			out.Metadata.TotalFound, out.Metadata.QualityScore)

		// Always persist the finder output to NEIGHBORHOODS_FILE
		if neighborhoodsFile != "" {
			if err := func() error {
				if err := os.MkdirAll(filepath.Dir(neighborhoodsFile), 0755); err != nil && filepath.Dir(neighborhoodsFile) != "." {
					return err
				}
				b, _ := json.MarshalIndent(out, "", "  ")
				return os.WriteFile(neighborhoodsFile, b, 0644)
			}(); err != nil {
				log.Fatalf("failed to write neighborhoods file: %v", err)
			}
			log.Printf("Neighborhoods saved to %s", neighborhoodsFile)
		}
	}

	// 2) Build collector config (basic or advanced) from env
	adv := getenvBool("ADVANCED_MODE", true)

	outputFile := getenv("OUTPUT_FILE", "")
	if outputFile == "" {
		if getenvBool("COMPRESS_OUTPUT", true) {
			outputFile = fmt.Sprintf("%s_places.jsonl.gz", city)
		} else {
			outputFile = fmt.Sprintf("%s_places.jsonl", city)
		}
	}

	cfg := &Config{
		APIKey:              os.Getenv("GOOGLE_API_KEY"),
		City:                city,
		H3Resolution:        getenvInt("H3_RESOLUTION", 7),
		AdaptiveResolution:  getenvBool("ADAPTIVE_RESOLUTION", true),
		OutputFile:          outputFile,
		CompressOutput:      getenvBool("COMPRESS_OUTPUT", true),
		NeighborhoodsFile:   neighborhoodsFile,
		MaxConcurrency:      getenvInt("MAX_CONCURRENCY", 5),
		QueryRadiusMeters:   getenvInt("RADIUS", 0),
		ResumeFromIndex:     getenvInt("RESUME_INDEX", 0),
		ImportanceThreshold: getenvFloat("IMPORTANCE_THRESHOLD", 0.0),
		Verbosity:           getenvInt("VERBOSITY", 1),
		Urban:               getenvBool("URBAN", true),
		ProgressFile:        getenv("PROGRESS_FILE", "./progress.json"),
	}
	// Place types CSV -> slice
	if t := strings.TrimSpace(getenv("PLACE_TYPES", "")); t != "" {
		cfg.PlaceTypes = strings.Split(t, ",")
		for i := range cfg.PlaceTypes {
			cfg.PlaceTypes[i] = strings.TrimSpace(cfg.PlaceTypes[i])
		}
	}
	// New: primary and excluded type filters
	if t := strings.TrimSpace(getenv("PLACE_PRIMARY_TYPES", "")); t != "" {
		cfg.PlacePrimaryTypes = strings.Split(t, ",")
		for i := range cfg.PlacePrimaryTypes {
			cfg.PlacePrimaryTypes[i] = strings.TrimSpace(cfg.PlacePrimaryTypes[i])
		}
	}
	if t := strings.TrimSpace(getenv("PLACE_EXCLUDED_TYPES", "")); t != "" {
		cfg.ExcludedTypes = strings.Split(t, ",")
		for i := range cfg.ExcludedTypes {
			cfg.ExcludedTypes[i] = strings.TrimSpace(cfg.ExcludedTypes[i])
		}
	}
	if t := strings.TrimSpace(getenv("PLACE_EXCLUDED_PRIMARY_TYPES", "")); t != "" {
		cfg.ExcludedPrimaryTypes = strings.Split(t, ",")
		for i := range cfg.ExcludedPrimaryTypes {
			cfg.ExcludedPrimaryTypes[i] = strings.TrimSpace(cfg.ExcludedPrimaryTypes[i])
		}
	}
	// New: locale and ranking
	cfg.LanguageCode = strings.TrimSpace(getenv("LANGUAGE_CODE", ""))
	cfg.RegionCode = strings.TrimSpace(getenv("REGION_CODE", ""))
	cfg.RankPreference = strings.TrimSpace(getenv("RANK_PREFERENCE", "POPULARITY"))

	// Visualization
	cfg.MapOutputFile = strings.TrimSpace(getenv("MAP_OUTPUT_HTML", "")) // e.g., ./coverage_map.html
	cfg.MapDrawCircles = getenvBool("MAP_DRAW_CIRCLES", true)

	// Rate limit config
	cfg.RateLimit.QPS = getenvFloat("RATE_QPS", 1.0)
	cfg.RateLimit.MaxRetries = getenvInt("RATE_RETRIES", 5)
	cfg.RateLimit.RetryInterval = time.Duration(getenvInt("RATE_RETRY_INTERVAL_SEC", 2)) * time.Second

	// New: expose coverage controls
	cfg.CoverageRadiusMeters = getenvInt("COVERAGE_RADIUS_METERS", 0) // 0 => use defaults (urban/non-urban)
	cfg.MaxHexRings = getenvInt("MAX_HEX_RINGS", 0)                   // >0 overrides CoverageRadiusMeters

	if cfg.APIKey == "" {
		log.Fatal("GOOGLE_API_KEY is required in .env")
	}

	// 3) Run collector
	if adv {
		if err := RunAdvanced(cfg); err != nil {
			log.Fatalf("advanced collector failed: %v", err)
		}
	} else {
		if err := RunBasic(cfg); err != nil {
			log.Fatalf("basic collector failed: %v", err)
		}
	}
}
