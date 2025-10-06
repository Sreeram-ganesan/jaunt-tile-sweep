package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Constants (reusing from original finder.go)
// const tavilyExtractURL is already defined in finder.go

// ----------- Advanced Finder Configuration -----------

type FinderAdvConfig struct {
	City           string
	Query          string // optional custom query
	LLM            string // openai|gemini
	MaxResults     int
	SkipTavily     bool
	ExtractContent bool
	OutputFile     string // where to write the neighborhoods JSON (optional)

	// Advanced search configuration
	IncludeDomains     []string // prioritize or restrict to these domains
	ExcludeDomains     []string // drop these domains
	MinResultScore     float64  // drop Tavily results below this score
	MaxExtractChars    int      // truncate extracted content to this many characters
	ParallelExtractors int      // parallelism when chunking extraction

	// Enhanced OSM integration
	PreferOSMBoundaries bool     // prioritize OSM boundary data over LLM
	OSMAdminLevels      []string // specific admin levels to query (default: 8,9,10,11)
	OSMPlaceTypes       []string // specific place types to query
	MinBoundaryArea     float64  // minimum acceptable boundary area in km²

	// Multi-phase search strategy
	EnableMultiPhase bool     // enable multi-phase search (official first, then general)
	OfficialDomains  []string // domains considered official/authoritative

	// Confidence and validation
	MinConfidenceScore float64 // minimum confidence for neighborhood inclusion
	EnableValidation   bool    // enable geometric and semantic validation

	// Performance options
	EnableCaching  bool          // enable caching for expensive operations
	MaxConcurrency int           // maximum concurrent operations
	Timeout        time.Duration // operation timeout
}

// ----------- Enhanced Data Structures -----------

type NeighborhoodAdvanced struct {
	Name     string          `json:"name"`
	Lat      float64         `json:"lat"`
	Lng      float64         `json:"lng"`
	Boundary json.RawMessage `json:"boundary,omitempty"`

	// Enhanced metadata
	Source          string   `json:"source"`     // osm, llm, tavily, hybrid
	ConfidenceScore float64  `json:"confidence"` // 0.0 to 1.0
	Aliases         []string `json:"aliases,omitempty"`
	AdminLevel      string   `json:"admin_level,omitempty"`
	PlaceType       string   `json:"place_type,omitempty"`
	AreaKm2         float64  `json:"area_km2,omitempty"`
	OSMId           string   `json:"osm_id,omitempty"`

	// Validation flags
	HasValidBoundary bool `json:"has_valid_boundary"`
	GeometryValid    bool `json:"geometry_valid"`
}

type FinderAdvancedOut struct {
	Neighborhoods []NeighborhoodAdvanced `json:"neighborhoods"`
	Metadata      SearchMetadata         `json:"metadata"`
}

type SearchMetadata struct {
	City             string    `json:"city"`
	SearchTimestamp  time.Time `json:"search_timestamp"`
	TotalFound       int       `json:"total_found"`
	OSMCount         int       `json:"osm_count"`
	LLMCount         int       `json:"llm_count"`
	HybridCount      int       `json:"hybrid_count"`
	ProcessingTimeMs int64     `json:"processing_time_ms"`
	QualityScore     float64   `json:"quality_score"`
}

// ----------- Enhanced OSM Integration -----------

type OSMNeighborhoodAdvanced struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Lat         float64           `json:"lat"`
	Lng         float64           `json:"lng"`
	Tags        map[string]string `json:"tags"`
	AdminLevel  string            `json:"admin_level"`
	PlaceType   string            `json:"place_type"`
	BoundingBox struct {
		MinLat, MinLng, MaxLat, MaxLng float64
	} `json:"bbox"`
	AreaKm2  float64         `json:"area_km2"`
	Boundary json.RawMessage `json:"boundary,omitempty"`
}

// ----------- Multi-Phase Search Strategy -----------

type SearchPhase struct {
	Name        string
	Query       string
	Domains     []string
	MaxResults  int
	MinScore    float64
	Description string
}

// ----------- Main Advanced Finder Function -----------

func RunAdvancedFinder(cfg *FinderAdvConfig) (*FinderAdvancedOut, error) {
	startTime := time.Now()

	if cfg == nil {
		return nil, errors.New("advanced finder config is nil")
	}
	if cfg.City == "" {
		return nil, errors.New("advanced finder: City is required")
	}

	// Set intelligent defaults
	setAdvancedDefaults(cfg)

	// Initialize APIs
	tavilyKey := os.Getenv("TAVILY_API_KEY")
	openAIKey := os.Getenv("OPENAI_API_KEY")
	geminiKey := os.Getenv("GOOGLE_API_KEY")

	log.Println("Gemini key is pressent - ", geminiKey != "")

	log.Printf("🚀 Starting Advanced Finder for city: %s", cfg.City)

	var result FinderAdvancedOut
	result.Metadata.City = cfg.City
	result.Metadata.SearchTimestamp = startTime

	// Phase 1: Enhanced OSM Data Collection
	log.Printf("📍 Phase 1: Collecting OSM boundary data...")
	osmNeighborhoods, err := fetchEnhancedOSMData(cfg)
	if err != nil {
		log.Printf("⚠️ OSM data collection failed: %v", err)
	} else {
		log.Printf("✅ Found %d OSM neighborhoods with boundaries", len(osmNeighborhoods))
	}

	// Phase 2: Multi-Phase Tavily Search (if not skipped)
	var contextBundle string
	if !cfg.SkipTavily {
		log.Printf("🔍 Phase 2: Multi-phase search strategy...")
		contextBundle, err = executeMultiPhaseSearch(cfg, tavilyKey)
		if err != nil {
			log.Printf("⚠️ Multi-phase search failed: %v", err)
		}
	}

	// Phase 3: Enhanced LLM Processing with Structured Prompts
	log.Printf("🧠 Phase 3: LLM processing with enhanced prompts...")
	llmNeighborhoods, err := processWithEnhancedLLM(cfg, contextBundle, osmNeighborhoods, openAIKey, geminiKey)
	if err != nil {
		return nil, fmt.Errorf("LLM processing failed: %w", err)
	}

	// Phase 4: Intelligent Data Fusion
	log.Printf("🔄 Phase 4: Intelligent data fusion...")
	fusedNeighborhoods := fuseDataIntelligently(cfg, osmNeighborhoods, llmNeighborhoods)

	// Phase 5: Validation and Quality Assessment
	log.Printf("✅ Phase 5: Validation and quality assessment...")
	validatedNeighborhoods := validateAndScore(cfg, fusedNeighborhoods)

	// Phase 6: Final Processing and Output
	result.Neighborhoods = validatedNeighborhoods
	result.Metadata.TotalFound = len(validatedNeighborhoods)
	result.Metadata.ProcessingTimeMs = time.Since(startTime).Milliseconds()

	// Calculate quality metrics
	calculateQualityMetrics(&result)

	// Save output if specified
	if cfg.OutputFile != "" {
		if err := saveAdvancedOutput(&result, cfg.OutputFile); err != nil {
			log.Printf("⚠️ Failed to save output: %v", err)
		}
	}

	log.Printf("🎉 Advanced Finder completed: %d neighborhoods found (%.2fs)",
		result.Metadata.TotalFound, float64(result.Metadata.ProcessingTimeMs)/1000.0)

	return &result, nil
}

// ----------- Configuration Defaults -----------

func setAdvancedDefaults(cfg *FinderAdvConfig) {
	if cfg.MaxResults == 0 {
		cfg.MaxResults = 30
	}
	if cfg.MaxExtractChars == 0 {
		cfg.MaxExtractChars = 12000 // Increased for better context
	}
	if cfg.ParallelExtractors <= 0 {
		cfg.ParallelExtractors = 6
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 45 * time.Second // Increased timeout
	}
	if cfg.MinConfidenceScore == 0 {
		cfg.MinConfidenceScore = 0.3
	}
	if cfg.MinBoundaryArea == 0 {
		cfg.MinBoundaryArea = 0.1 // 0.1 km² minimum
	}

	// Default OSM configuration
	if len(cfg.OSMAdminLevels) == 0 {
		cfg.OSMAdminLevels = []string{"8", "9", "10", "11"}
	}
	if len(cfg.OSMPlaceTypes) == 0 {
		cfg.OSMPlaceTypes = []string{"neighbourhood", "neighborhood", "suburb", "quarter", "ward", "district"}
	}

	// Default official domains for multi-phase search
	if len(cfg.OfficialDomains) == 0 {
		cfg.OfficialDomains = []string{
			"gov.uk", "gov.au", "gov.ca", "gov.in", "gov.sg",
			"census.gov", "data.gov", "opendata",
			"city", "municipality", "council",
			"wikipedia.org", "openstreetmap.org",
		}
	}

	if strings.TrimSpace(cfg.Query) == "" {
		cfg.Query = fmt.Sprintf(
			"official administrative neighborhoods wards districts %s boundaries polygon GeoJSON shapefile municipal data",
			cfg.City)
	}
}

// ----------- Enhanced OSM Data Collection -----------

func fetchEnhancedOSMData(cfg *FinderAdvConfig) ([]OSMNeighborhoodAdvanced, error) {
	cityEsc := url.QueryEscape(cfg.City)
	cacheKey := strings.ToLower(strings.ReplaceAll(cityEsc, "%20", "_"))
	cacheDir := filepath.Join("cache", "osm")
	cacheFile := filepath.Join(cacheDir, cacheKey+".json")
	ua := "jaunt-tile-sweep/1.1 (advanced-finder)"

	// Try to load from cache first if caching is enabled
	if cfg.EnableCaching {
		cachedData, err := loadOSMFromCache(cacheFile)
		if err == nil {
			log.Printf("📋 Using cached OSM data for %s (%d items)", cfg.City, len(cachedData))
			return cachedData, nil
		}
	}

	// Build comprehensive Overpass query - Fixed syntax for better compatibility
	adminLevels := strings.Join(cfg.OSMAdminLevels, "|")
	placeTypes := strings.Join(cfg.OSMPlaceTypes, "|")

	// Improved query with proper area matching and error handling
	query := fmt.Sprintf(`
[out:json][timeout:45];
// Find the area for the city first
area[name~"%s"][admin_level~"^[2-8]$"];
// Then use that area to find administrative boundaries
(
  relation["boundary"="administrative"]["admin_level"~"(%s)"](area);
  relation["place"~"(%s)"](area);
);
// Get complete data with geometry and tags
out geom tags center;
`, regexp.QuoteMeta(cityEsc), adminLevels, placeTypes)

	form := url.Values{}
	form.Set("data", query)

	req, err := http.NewRequest(http.MethodPost, "https://overpass-api.de/api/interpreter",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", ua)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	req = req.WithContext(ctx)

	log.Printf("🌐 Fetching OSM data for %s from Overpass API...", cfg.City)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("overpass enhanced query failed: status=%d, body=%s",
			resp.StatusCode, string(bs[:min(200, len(bs))]))
	}

	// Parse the response
	neighborhoods, err := parseEnhancedOSMResponse(bs)
	if err != nil {
		return nil, err
	}

	// Cache the results if caching is enabled
	if cfg.EnableCaching && len(neighborhoods) > 0 {
		if err := saveOSMToCache(neighborhoods, cacheFile); err != nil {
			log.Printf("⚠️ Failed to cache OSM data: %v", err)
		} else {
			log.Printf("💾 Cached OSM data for %s (%d items)", cfg.City, len(neighborhoods))
		}
	}

	return neighborhoods, nil
}

// Helper functions for cache management
func loadOSMFromCache(cachePath string) ([]OSMNeighborhoodAdvanced, error) {
	// Check if the cache file exists
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("cache file does not exist: %s", cachePath)
	}

	// Read the cache file
	data, err := os.ReadFile(cachePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache file: %w", err)
	}

	// Check if the file is empty or too small
	if len(data) < 10 {
		return nil, fmt.Errorf("cache file is empty or corrupt")
	}

	// Parse the JSON data
	var neighborhoods []OSMNeighborhoodAdvanced
	if err := json.Unmarshal(data, &neighborhoods); err != nil {
		return nil, fmt.Errorf("failed to parse cached OSM data: %w", err)
	}

	return neighborhoods, nil
}

func saveOSMToCache(neighborhoods []OSMNeighborhoodAdvanced, cachePath string) error {
	// Ensure cache directory exists
	cacheDir := filepath.Dir(cachePath)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Marshal the data to JSON with pretty formatting
	data, err := json.MarshalIndent(neighborhoods, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal OSM data: %w", err)
	}

	// Write to the cache file
	if err := os.WriteFile(cachePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	return nil
}

func parseEnhancedOSMResponse(data []byte) ([]OSMNeighborhoodAdvanced, error) {
	var overpass struct {
		Elements []struct {
			Type   string            `json:"type"`
			ID     int64             `json:"id"`
			Tags   map[string]string `json:"tags"`
			Center *struct {
				Lat float64 `json:"lat"`
				Lon float64 `json:"lon"`
			} `json:"center,omitempty"`
			Bounds *struct {
				MinLat float64 `json:"minlat"`
				MinLon float64 `json:"minlon"`
				MaxLat float64 `json:"maxlat"`
				MaxLon float64 `json:"maxlon"`
			} `json:"bounds,omitempty"`
			Geometry []struct {
				Lat float64 `json:"lat"`
				Lon float64 `json:"lon"`
			} `json:"geometry,omitempty"`
		} `json:"elements"`
	}

	if err := json.Unmarshal(data, &overpass); err != nil {
		return nil, err
	}

	var neighborhoods []OSMNeighborhoodAdvanced
	for _, elem := range overpass.Elements {
		name := elem.Tags["name"]
		if name == "" {
			continue
		}

		// Skip if name contains unwanted patterns
		if containsUnwantedPattern(name) {
			continue
		}

		neighborhood := OSMNeighborhoodAdvanced{
			ID:   fmt.Sprintf("%s%d", strings.ToUpper(elem.Type[:1]), elem.ID),
			Name: name,
			Tags: elem.Tags,
		}

		// Extract admin level and place type
		if adminLevel := elem.Tags["admin_level"]; adminLevel != "" {
			neighborhood.AdminLevel = adminLevel
		}
		if placeType := elem.Tags["place"]; placeType != "" {
			neighborhood.PlaceType = placeType
		}

		// Set coordinates
		if elem.Center != nil {
			neighborhood.Lat = elem.Center.Lat
			neighborhood.Lng = elem.Center.Lon
		}

		// Set bounding box and calculate area
		if elem.Bounds != nil {
			neighborhood.BoundingBox.MinLat = elem.Bounds.MinLat
			neighborhood.BoundingBox.MinLng = elem.Bounds.MinLon
			neighborhood.BoundingBox.MaxLat = elem.Bounds.MaxLat
			neighborhood.BoundingBox.MaxLng = elem.Bounds.MaxLon
			neighborhood.AreaKm2 = calculateBboxArea(elem.Bounds.MinLat, elem.Bounds.MinLon,
				elem.Bounds.MaxLat, elem.Bounds.MaxLon)
		}

		// Convert geometry to GeoJSON if available
		if len(elem.Geometry) > 0 {
			// Convert the geometry format
			geometry := make([]struct{ Lat, Lon float64 }, len(elem.Geometry))
			for i, point := range elem.Geometry {
				geometry[i] = struct{ Lat, Lon float64 }{Lat: point.Lat, Lon: point.Lon}
			}
			boundary, err := convertToGeoJSON(geometry)
			if err == nil {
				neighborhood.Boundary = boundary
			}
		}

		neighborhoods = append(neighborhoods, neighborhood)
	}

	return neighborhoods, nil
}

// Helper functions for OSM processing
func containsUnwantedPattern(name string) bool {
	unwanted := []string{"unnamed", "unknown", "temp", "test", "import"}
	nameLower := strings.ToLower(name)
	for _, pattern := range unwanted {
		if strings.Contains(nameLower, pattern) {
			return true
		}
	}
	return false
}

func calculateBboxArea(minLat, minLng, maxLat, maxLng float64) float64 {
	// Simple bounding box area calculation (approximation)
	latDiff := maxLat - minLat
	lngDiff := maxLng - minLng
	// Convert to km² (very rough approximation)
	return latDiff * lngDiff * 12321 // approximately 111km per degree squared
}

func convertToGeoJSON(geometry []struct{ Lat, Lon float64 }) (json.RawMessage, error) {
	if len(geometry) < 3 {
		return nil, errors.New("insufficient geometry points")
	}

	// Build coordinates array for Polygon
	coords := make([][]float64, len(geometry))
	for i, point := range geometry {
		coords[i] = []float64{point.Lon, point.Lat}
	}

	// Close the polygon if needed
	if len(coords) > 0 {
		first := coords[0]
		last := coords[len(coords)-1]
		if first[0] != last[0] || first[1] != last[1] {
			coords = append(coords, first)
		}
	}

	polygon := map[string]interface{}{
		"type":        "MultiPolygon",
		"coordinates": [][][]float64{coords},
	}

	return json.Marshal(polygon)
}

// ----------- Multi-Phase Search Implementation -----------

func executeMultiPhaseSearch(cfg *FinderAdvConfig, tavilyKey string) (string, error) {
	if tavilyKey == "" {
		return "", errors.New("TAVILY_API_KEY is required for search")
	}

	var allResults []tavilySearchResult
	var contextParts []string

	// Define search phases
	phases := []SearchPhase{
		{
			Name:        "Official Sources",
			Query:       fmt.Sprintf("official administrative divisions %s neighborhoods wards districts site:(gov OR municipal OR city OR council)", cfg.City),
			Domains:     cfg.OfficialDomains,
			MaxResults:  10,
			MinScore:    0.7,
			Description: "Search official government and municipal sources",
		},
		{
			Name:        "Geographic Databases",
			Query:       fmt.Sprintf("%s neighborhoods boundaries GeoJSON shapefile polygon site:(openstreetmap.org OR wikipedia.org OR geonames.org)", cfg.City),
			Domains:     []string{"openstreetmap.org", "wikipedia.org", "geonames.org", "wikidata.org"},
			MaxResults:  8,
			MinScore:    0.6,
			Description: "Search geographic and mapping databases",
		},
		{
			Name:        "General Search",
			Query:       cfg.Query,
			Domains:     nil, // Use configured domains
			MaxResults:  cfg.MaxResults,
			MinScore:    cfg.MinResultScore,
			Description: "General search with user query",
		},
	}

	if !cfg.EnableMultiPhase {
		// If multi-phase is disabled, just do the general search
		phases = phases[2:]
	}

	log.Printf("🔍 Executing %d search phases...", len(phases))

	for i, phase := range phases {
		log.Printf("  Phase %d/%d: %s", i+1, len(phases), phase.Description)

		results, err := performTavilySearchAdv(tavilyKey, phase.Query, phase.MaxResults,
			phase.Domains, cfg.ExcludeDomains)
		if err != nil {
			log.Printf("    ⚠️ Phase %d failed: %v", i+1, err)
			continue
		}

		// Filter results by minimum score
		filtered := make([]tavilySearchResult, 0)
		for _, result := range results.Results {
			if result.Score >= phase.MinScore {
				filtered = append(filtered, result)
			}
		}

		log.Printf("    ✅ Phase %d: %d results (score >= %.1f)", i+1, len(filtered), phase.MinScore)
		allResults = append(allResults, filtered...)
	}

	// Deduplicate and prioritize results
	deduped := deduplicateAndPrioritize(allResults, cfg)
	log.Printf("📋 Total unique results after deduplication: %d", len(deduped))

	// Extract content if enabled
	if cfg.ExtractContent && len(deduped) > 0 {
		log.Printf("📄 Extracting content from %d URLs...", len(deduped))
		extracted := extractContentParallel(tavilyKey, deduped, cfg)
		contextParts = append(contextParts, extracted...)
	} else {
		// Use just the titles and content from search results
		for _, result := range deduped {
			if result.Content != "" {
				contextParts = append(contextParts,
					fmt.Sprintf("Source: %s\nTitle: %s\nContent: %s\n---\n",
						result.URL, result.Title, result.Content))
			}
		}
	}

	context := strings.Join(contextParts, "\n\n")
	if len(context) > cfg.MaxExtractChars {
		context = context[:cfg.MaxExtractChars] + "... [truncated]"
	}

	log.Printf("📝 Context bundle created: %d characters", len(context))
	return context, nil
}

// ----------- Enhanced LLM Processing -----------

func processWithEnhancedLLM(cfg *FinderAdvConfig, context string, osm []OSMNeighborhoodAdvanced, openAIKey, geminiKey string) ([]NeighborhoodAdvanced, error) {
	// Build enhanced context with OSM data
	enhancedContext := buildEnhancedContext(cfg.City, context, osm)

	// Create structured prompt for better results
	systemPrompt := createStructuredSystemPrompt()
	userPrompt := createEnhancedUserPrompt(cfg.City, enhancedContext)

	log.Printf("🧠 Calling LLM with enhanced prompts (context: %d chars)", len(enhancedContext))

	var rawResponse string
	var err error

	switch cfg.LLM {
	case "gemini":
		if geminiKey == "" {
			return nil, errors.New("GOOGLE_API_KEY required for Gemini")
		}
		rawResponse, err = callGeminiEnhanced(geminiKey, systemPrompt, userPrompt)
	case "openai":
		if openAIKey == "" {
			return nil, errors.New("OPENAI_API_KEY required for OpenAI")
		}
		rawResponse, err = callOpenAIEnhanced(openAIKey, systemPrompt, userPrompt)
	default:
		if geminiKey == "" {
			return nil, errors.New("GOOGLE_API_KEY required for Gemini")
		}
		rawResponse, err = callGeminiEnhanced(geminiKey, systemPrompt, userPrompt)
	}

	if err != nil {
		return nil, fmt.Errorf("LLM call failed: %w", err)
	}

	// Parse and validate LLM response
	neighborhoods, err := parseEnhancedLLMResponse(rawResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LLM response: %w", err)
	}

	log.Printf("✅ LLM extracted %d neighborhoods", len(neighborhoods))
	return neighborhoods, nil
}

func buildEnhancedContext(city string, webContext string, osmData []OSMNeighborhoodAdvanced) string {
	var parts []string

	// Add web context if available
	if webContext != "" {
		parts = append(parts, "=== WEB SOURCES ===")
		parts = append(parts, webContext)
		parts = append(parts, "")
	}

	// Add OSM context in structured format
	if len(osmData) > 0 {
		parts = append(parts, "=== OPENSTREETMAP DATA ===")
		parts = append(parts, fmt.Sprintf("Found %d administrative areas in %s:", len(osmData), city))

		for _, osm := range osmData {
			osmDesc := fmt.Sprintf("- %s (ID: %s, Type: %s",
				osm.Name, osm.ID, osm.PlaceType)
			if osm.AdminLevel != "" {
				osmDesc += fmt.Sprintf(", Admin Level: %s", osm.AdminLevel)
			}
			if osm.AreaKm2 > 0 {
				osmDesc += fmt.Sprintf(", Area: %.1f km²", osm.AreaKm2)
			}
			osmDesc += ")"
			parts = append(parts, osmDesc)
		}
		parts = append(parts, "")
	}

	return strings.Join(parts, "\n")
}

func createStructuredSystemPrompt() string {
	return `You are an expert geographic data analyst specializing in administrative boundaries and neighborhood identification.

Your task is to extract and structure neighborhood/ward data from provided sources.

CRITICAL REQUIREMENTS:
1. Return ONLY valid JSON in the specified format
2. Extract official administrative neighborhoods/wards/districts
3. Prefer authoritative/official sources over informal mentions  
4. Include confidence scores based on source reliability
5. Use existing OSM data when available (higher confidence)
6. Ensure coordinates are in WGS84 decimal degrees
7. Validate that boundaries are proper GeoJSON MultiPolygons

OUTPUT FORMAT (return exactly this structure):
{
  "neighborhoods": [
    {
      "name": "Official Neighborhood Name",
      "lat": 0.0,
      "lng": 0.0,
      "confidence": 0.95,
      "source": "osm|web|hybrid",
      "aliases": ["Alternative Name 1"],
      "boundary": {"type": "MultiPolygon", "coordinates": [...]}
    }
  ]
}

CONFIDENCE SCORING:
- 0.9-1.0: Official government sources, OSM with boundaries
- 0.7-0.9: Wikipedia, established geographic databases  
- 0.5-0.7: News articles, local websites with geographic references
- 0.3-0.5: General mentions, unclear sources
- Below 0.3: Exclude from results`
}

func createEnhancedUserPrompt(city string, context string) string {
	return fmt.Sprintf(`Extract all official neighborhoods/wards/districts for %s from the provided data sources.

CONTEXT DATA:
%s

Focus on:
- Official administrative divisions (wards, districts, neighborhoods)  
- Geographic boundaries when available
- Centroid coordinates for each area
- Cross-reference OSM data with web sources for validation
- Assign appropriate confidence scores based on source quality

Return only the JSON structure as specified in the system prompt.`, city, context)
}

func callOpenAIEnhanced(apiKey, systemPrompt, userPrompt string) (string, error) {
	headers := map[string]string{
		"Authorization": "Bearer " + apiKey,
	}

	payload := map[string]interface{}{
		"model": "gpt-4o-mini",
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userPrompt},
		},
		"temperature": 0.1, // Low temperature for consistency
		"max_tokens":  4000,
	}

	raw, err := postJSONWithRetry("https://api.openai.com/v1/chat/completions",
		payload, headers, 3, 2*time.Second)
	if err != nil {
		return "", err
	}

	var resp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", err
	}

	if len(resp.Choices) == 0 {
		return "", errors.New("no response from OpenAI")
	}

	return resp.Choices[0].Message.Content, nil
}

func callGeminiEnhanced(apiKey, systemPrompt, userPrompt string) (string, error) {
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=%s", apiKey)

	combinedPrompt := systemPrompt + "\n\n" + userPrompt

	payload := map[string]interface{}{
		"contents": []map[string]interface{}{
			{
				"parts": []map[string]string{
					{"text": combinedPrompt},
				},
			},
		},
		"generationConfig": map[string]interface{}{
			"temperature":     0.1,
			"maxOutputTokens": 4000,
		},
	}

	raw, err := postJSONWithRetry(url, payload, nil, 3, 2*time.Second)
	if err != nil {
		return "", err
	}

	var resp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}

	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", err
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", errors.New("no response from Gemini")
	}

	return resp.Candidates[0].Content.Parts[0].Text, nil
}

func parseEnhancedLLMResponse(rawResponse string) ([]NeighborhoodAdvanced, error) {
	// Clean the response - remove markdown code blocks, etc.
	cleaned := cleanLLMResponse(rawResponse)

	var parsed struct {
		Neighborhoods []NeighborhoodAdvanced `json:"neighborhoods"`
	}

	if err := json.Unmarshal([]byte(cleaned), &parsed); err != nil {
		// Try to extract JSON from the response
		jsonStr := extractJSONFromText(cleaned)
		if jsonStr == "" {
			return nil, fmt.Errorf("no valid JSON found in response: %s", cleaned[:min(200, len(cleaned))])
		}
		if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
			return nil, fmt.Errorf("failed to parse extracted JSON: %w", err)
		}
	}

	return parsed.Neighborhoods, nil
}

func cleanLLMResponse(response string) string {
	// Remove markdown code blocks
	response = regexp.MustCompile("```(?:json)?\\s*").ReplaceAllString(response, "")
	response = regexp.MustCompile("```").ReplaceAllString(response, "")

	// Remove common prefixes
	lines := strings.Split(response, "\n")
	var cleaned []string
	foundJSON := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !foundJSON && (strings.HasPrefix(line, "{") || strings.HasPrefix(line, "[")) {
			foundJSON = true
		}
		if foundJSON {
			cleaned = append(cleaned, line)
		}
	}

	return strings.Join(cleaned, "\n")
}

func extractJSONFromText(text string) string {
	// Find JSON object boundaries
	start := strings.Index(text, "{")
	if start == -1 {
		return ""
	}

	braceCount := 0
	for i := start; i < len(text); i++ {
		if text[i] == '{' {
			braceCount++
		} else if text[i] == '}' {
			braceCount--
			if braceCount == 0 {
				return text[start : i+1]
			}
		}
	}

	return ""
}

// ----------- Helper Functions -----------

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// Note: Using shared Tavily types and HTTP helpers from original finder.go to avoid duplication

func performTavilySearchAdv(apiKey, query string, maxResults int, includeDomains, excludeDomains []string) (*tavilySearchResponse, error) {
	// Use the original tavilySearch function but adapt the call
	return tavilySearch(apiKey, query, maxResults, includeDomains, excludeDomains)
}

func deduplicateAndPrioritize(results []tavilySearchResult, cfg *FinderAdvConfig) []tavilySearchResult {
	seen := make(map[string]bool)
	var deduplicated []tavilySearchResult

	// Sort by score (descending)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	for _, result := range results {
		normalizedURL := normalizeURL(result.URL)
		if !seen[normalizedURL] {
			seen[normalizedURL] = true
			deduplicated = append(deduplicated, result)
		}
	}

	return deduplicated
}

func extractContentParallel(apiKey string, results []tavilySearchResult, cfg *FinderAdvConfig) []string {
	if len(results) == 0 {
		return nil
	}

	// Limit concurrent extractions
	maxWorkers := min(cfg.ParallelExtractors, len(results))
	semaphore := make(chan struct{}, maxWorkers)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var extracted []string

	for _, result := range results {
		wg.Add(1)
		go func(r tavilySearchResult) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			content, err := extractSingleURL(apiKey, r.URL, cfg.MaxExtractChars)
			if err != nil {
				log.Printf("⚠️ Failed to extract %s: %v", r.URL, err)
				return
			}

			mu.Lock()
			extracted = append(extracted, fmt.Sprintf("Source: %s\nTitle: %s\nContent: %s\n---\n",
				r.URL, r.Title, content))
			mu.Unlock()
		}(result)
	}

	wg.Wait()
	return extracted
}

func extractSingleURL(apiKey, url string, maxChars int) (string, error) {
	headers := map[string]string{
		"X-API-Key": apiKey,
	}

	payload := map[string]string{
		"url": url,
	}

	raw, err := postJSONWithRetry(tavilyExtractURL, payload, headers, 2, 1*time.Second)
	if err != nil {
		return "", err
	}

	var response struct {
		RawContent string `json:"raw_content"`
	}

	if err := json.Unmarshal(raw, &response); err != nil {
		return "", err
	}

	content := response.RawContent
	if len(content) > maxChars {
		content = content[:maxChars] + "... [truncated]"
	}

	return content, nil
}

// ----------- Example Usage -----------

// ExampleRunAdvancedFinder demonstrates how to use the advanced finder
func ExampleRunAdvancedFinder() {
	// Example configuration for London neighborhoods
	cfg := &FinderAdvConfig{
		City:                "Edinburgh",
		LLM:                 "gemini", // or "gemini"
		MaxResults:          20,
		EnableMultiPhase:    true,
		PreferOSMBoundaries: true,
		MinConfidenceScore:  0.4,
		EnableValidation:    true,
		OutputFile:          "neighborhoods.json",

		// Enhanced OSM settings
		OSMAdminLevels:  []string{"8", "9", "10"},
		MinBoundaryArea: 0.5, // 0.5 km² minimum

		// Official domains to prioritize
		OfficialDomains: []string{
			"gov.uk", "london.gov.uk", "city.london.gov.uk",
			"wikipedia.org", "openstreetmap.org",
		},
	}

	log.Printf("🚀 Starting Advanced Finder example for %s", cfg.City)

	result, err := RunAdvancedFinder(cfg)
	if err != nil {
		log.Fatalf("❌ Advanced finder failed: %v", err)
	}

	log.Printf("✅ Success! Found %d neighborhoods", result.Metadata.TotalFound)
	log.Printf("📊 Quality Score: %.2f", result.Metadata.QualityScore)
	log.Printf("📈 Source Distribution: OSM=%d, LLM=%d, Hybrid=%d",
		result.Metadata.OSMCount, result.Metadata.LLMCount, result.Metadata.HybridCount)

	// Show top 5 results
	log.Printf("🏘️ Top neighborhoods by confidence:")
	for i, neigh := range result.Neighborhoods {
		if i >= 5 {
			break
		}
		boundaryStatus := "❌"
		if neigh.HasValidBoundary {
			boundaryStatus = "✅"
		}
		log.Printf("  %d. %s (%.2f confidence, %s source, %s boundary)",
			i+1, neigh.Name, neigh.ConfidenceScore, neigh.Source, boundaryStatus)
	}
}

// ----------- Intelligent Data Fusion -----------

func fuseDataIntelligently(cfg *FinderAdvConfig, osm []OSMNeighborhoodAdvanced, llm []NeighborhoodAdvanced) []NeighborhoodAdvanced {
	log.Printf("🔄 Fusing data: %d OSM + %d LLM neighborhoods", len(osm), len(llm))

	var result []NeighborhoodAdvanced
	used := make(map[string]bool)

	// Phase 1: Start with OSM data (high confidence boundaries)
	for _, osmNeigh := range osm {
		if osmNeigh.AreaKm2 < cfg.MinBoundaryArea {
			continue // Skip too small areas
		}

		advanced := NeighborhoodAdvanced{
			Name:             osmNeigh.Name,
			Lat:              osmNeigh.Lat,
			Lng:              osmNeigh.Lng,
			Boundary:         osmNeigh.Boundary,
			Source:           "osm",
			ConfidenceScore:  0.85, // High confidence for OSM with boundaries
			AdminLevel:       osmNeigh.AdminLevel,
			PlaceType:        osmNeigh.PlaceType,
			AreaKm2:          osmNeigh.AreaKm2,
			OSMId:            osmNeigh.ID,
			HasValidBoundary: len(osmNeigh.Boundary) > 0,
			GeometryValid:    true, // Assume OSM geometry is valid
		}

		result = append(result, advanced)
		used[normalizeNameForMatching(osmNeigh.Name)] = true
	}

	// Phase 2: Add LLM data, merging with OSM where names match
	for _, llmNeigh := range llm {
		normalizedName := normalizeNameForMatching(llmNeigh.Name)

		if used[normalizedName] {
			// Find matching OSM entry and enhance it with LLM data
			for i := range result {
				if normalizeNameForMatching(result[i].Name) == normalizedName {
					// Enhance OSM data with LLM insights
					if len(llmNeigh.Aliases) > 0 {
						result[i].Aliases = llmNeigh.Aliases
					}
					// Update confidence if LLM provides additional validation
					if llmNeigh.ConfidenceScore > 0.7 {
						result[i].ConfidenceScore = minFloat(0.95, result[i].ConfidenceScore+0.1)
						result[i].Source = "hybrid"
					}
					break
				}
			}
			continue
		}

		// Add new LLM-only neighborhood if confidence is high enough
		if llmNeigh.ConfidenceScore >= cfg.MinConfidenceScore {
			if llmNeigh.Source == "" {
				llmNeigh.Source = "llm"
			}
			result = append(result, llmNeigh)
			used[normalizedName] = true
		}
	}

	// Phase 3: Quality filtering and validation
	filtered := make([]NeighborhoodAdvanced, 0, len(result))
	for _, neigh := range result {
		if isValidNeighborhood(neigh, cfg) {
			filtered = append(filtered, neigh)
		}
	}

	// Sort by confidence score (descending) and then by name
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].ConfidenceScore != filtered[j].ConfidenceScore {
			return filtered[i].ConfidenceScore > filtered[j].ConfidenceScore
		}
		return filtered[i].Name < filtered[j].Name
	})

	log.Printf("✅ Data fusion complete: %d final neighborhoods", len(filtered))
	return filtered
}

func normalizeNameForMatching(name string) string {
	// Normalize name for fuzzy matching
	normalized := strings.ToLower(strings.TrimSpace(name))
	// Remove common suffixes/prefixes
	suffixes := []string{" ward", " district", " neighborhood", " neighbourhood", " area"}
	for _, suffix := range suffixes {
		normalized = strings.TrimSuffix(normalized, suffix)
	}
	// Remove special characters
	normalized = regexp.MustCompile(`[^\w\s]`).ReplaceAllString(normalized, "")
	// Normalize whitespace
	normalized = regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")
	return strings.TrimSpace(normalized)
}

func isValidNeighborhood(neigh NeighborhoodAdvanced, cfg *FinderAdvConfig) bool {
	// Basic validation checks
	if neigh.Name == "" {
		return false
	}
	if neigh.ConfidenceScore < cfg.MinConfidenceScore {
		return false
	}
	if neigh.Lat == 0 && neigh.Lng == 0 {
		return false // Invalid coordinates
	}
	// Check coordinate ranges (basic sanity check)
	if neigh.Lat < -90 || neigh.Lat > 90 || neigh.Lng < -180 || neigh.Lng > 180 {
		return false
	}
	return true
}

// ----------- Validation and Quality Assessment -----------

func validateAndScore(cfg *FinderAdvConfig, neighborhoods []NeighborhoodAdvanced) []NeighborhoodAdvanced {
	log.Printf("✅ Validating %d neighborhoods...", len(neighborhoods))

	if !cfg.EnableValidation {
		return neighborhoods
	}

	validated := make([]NeighborhoodAdvanced, 0, len(neighborhoods))

	for _, neigh := range neighborhoods {
		// Geometric validation
		if len(neigh.Boundary) > 0 {
			neigh.GeometryValid = validateGeoJSONGeometry(neigh.Boundary)
			neigh.HasValidBoundary = neigh.GeometryValid

			if !neigh.GeometryValid && cfg.PreferOSMBoundaries {
				// Try to fetch boundary from OSM if validation fails
				if boundary := tryFetchOSMBoundary(neigh.Name, cfg); boundary != nil {
					neigh.Boundary = boundary
					neigh.GeometryValid = true
					neigh.HasValidBoundary = true
				}
			}
		}

		// Confidence adjustment based on validation
		originalConfidence := neigh.ConfidenceScore

		if neigh.GeometryValid && neigh.HasValidBoundary {
			neigh.ConfidenceScore += 0.1
		}

		if neigh.Source == "hybrid" {
			neigh.ConfidenceScore += 0.05 // Bonus for multiple source validation
		}

		// Cap at 1.0
		if neigh.ConfidenceScore > 1.0 {
			neigh.ConfidenceScore = 1.0
		}

		// Only include if still meets minimum confidence after adjustment
		if neigh.ConfidenceScore >= cfg.MinConfidenceScore {
			validated = append(validated, neigh)
			if neigh.ConfidenceScore != originalConfidence {
				log.Printf("  📊 %s: confidence adjusted %.2f → %.2f",
					neigh.Name, originalConfidence, neigh.ConfidenceScore)
			}
		}
	}

	log.Printf("✅ Validation complete: %d/%d neighborhoods passed", len(validated), len(neighborhoods))
	return validated
}

func calculateQualityMetrics(result *FinderAdvancedOut) {
	if len(result.Neighborhoods) == 0 {
		result.Metadata.QualityScore = 0.0
		return
	}

	var totalConfidence float64
	var osmCount, llmCount, hybridCount int
	var withBoundaries int

	for _, neigh := range result.Neighborhoods {
		totalConfidence += neigh.ConfidenceScore

		switch neigh.Source {
		case "osm":
			osmCount++
		case "llm":
			llmCount++
		case "hybrid":
			hybridCount++
		}

		if neigh.HasValidBoundary {
			withBoundaries++
		}
	}

	result.Metadata.OSMCount = osmCount
	result.Metadata.LLMCount = llmCount
	result.Metadata.HybridCount = hybridCount

	avgConfidence := totalConfidence / float64(len(result.Neighborhoods))
	boundaryRatio := float64(withBoundaries) / float64(len(result.Neighborhoods))

	// Quality score combines average confidence and boundary coverage
	result.Metadata.QualityScore = (avgConfidence * 0.7) + (boundaryRatio * 0.3)

	log.Printf("📊 Quality Metrics:")
	log.Printf("  Average Confidence: %.2f", avgConfidence)
	log.Printf("  Boundary Coverage: %.1f%% (%d/%d)", boundaryRatio*100, withBoundaries, len(result.Neighborhoods))
	log.Printf("  Source Distribution: OSM=%d, LLM=%d, Hybrid=%d", osmCount, llmCount, hybridCount)
	log.Printf("  Overall Quality Score: %.2f", result.Metadata.QualityScore)
}

func validateGeoJSONGeometry(boundary json.RawMessage) bool {
	var geom map[string]interface{}
	if err := json.Unmarshal(boundary, &geom); err != nil {
		return false
	}

	geometryType, ok := geom["type"].(string)
	if !ok {
		return false
	}

	coordinates, ok := geom["coordinates"]
	if !ok {
		return false
	}

	// Basic validation for MultiPolygon
	if geometryType == "MultiPolygon" {
		if coordArray, ok := coordinates.([]interface{}); ok {
			// Check if we have actual coordinate data
			return len(coordArray) > 0 && hasCoordinateData(coordArray)
		}
	}

	// Also support regular Polygon type
	if geometryType == "Polygon" {
		if coordArray, ok := coordinates.([]interface{}); ok {
			// Check if we have actual coordinate data
			return len(coordArray) > 0 && hasCoordinateData(coordArray)
		}
	}

	return false
}

func hasCoordinateData(coordArray []interface{}) bool {
	if len(coordArray) == 0 {
		return false
	}

	// For MultiPolygon, first element should be an array of polygons
	if rings, ok := coordArray[0].([]interface{}); ok {
		if len(rings) == 0 {
			return false
		}

		// For the first ring, check if it contains coordinates
		if coordinates, ok := rings[0].([]interface{}); ok {
			return len(coordinates) >= 4 // A valid polygon has at least 4 coordinates (closed)
		}
	}

	return false
}

func tryFetchOSMBoundary(name string, cfg *FinderAdvConfig) json.RawMessage {
	cityEsc := url.QueryEscape(cfg.City)
	nameEsc := regexp.QuoteMeta(name)
	ua := "jaunt-tile-sweep/1.1 (advanced-finder-fallback)"

	// Query for a specific relation/way with the given name inside the city area.
	query := fmt.Sprintf(`
[out:json][timeout:20];
area[name~"%s"][admin_level~"^[2-8]$"]->.searchArea;
(
  relation[name~"(?i)^%s$"](area.searchArea);
);
out geom;
`, cityEsc, nameEsc)

	form := url.Values{}
	form.Set("data", query)

	req, err := http.NewRequest(http.MethodPost, "https://overpass-api.de/api/interpreter", strings.NewReader(form.Encode()))
	if err != nil {
		log.Printf("tryFetchOSMBoundary: failed to create request for '%s': %v", name, err)
		return nil
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", ua)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("tryFetchOSMBoundary: request failed for '%s': %v", name, err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}

	// Use the existing parser to extract the geometry
	parsed, err := parseEnhancedOSMResponse(data)
	if err != nil || len(parsed) == 0 {
		return nil
	}

	// Return the boundary of the first valid result
	for _, p := range parsed {
		if len(p.Boundary) > 0 {
			return p.Boundary
		}
	}

	return nil
}

// ----------- Output Management -----------

func saveAdvancedOutput(result *FinderAdvancedOut, filename string) error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}

	// Write formatted JSON
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}

	log.Printf("💾 Output saved to: %s", filename)
	return nil
}
