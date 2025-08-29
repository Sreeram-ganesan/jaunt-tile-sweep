package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/uber/h3-go/v4"
	"golang.org/x/time/rate"
)

// Neighborhood represents a district or area in a city
type Neighborhood struct {
	Name            string               `json:"name"`
	Center          LatLng               `json:"center,omitempty"`
	Processed       bool                 `json:"processed,omitempty"`
	H3Indices       []string             `json:"h3_indices,omitempty"`
	Boundary        *GeoJSONMultiPolygon `json:"-"`
	BoundaryGeoJSON json.RawMessage      `json:"-"`
}

// NeighborhoodList is a list of neighborhoods in a city (names only)
type NeighborhoodList struct {
	Neighborhoods []string `json:"neighborhoods"`
}

// LatLng represents geographic coordinates
type LatLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Place represents a location from Google Maps Places API with all details
type Place struct {
	PlaceID           string      `json:"place_id"`
	Name              string      `json:"name"`
	Address           string      `json:"address"`
	Location          LatLng      `json:"location"`
	Types             []string    `json:"types"`
	Rating            float64     `json:"rating,omitempty"`
	UserRatings       int         `json:"user_ratings,omitempty"`
	PriceLevel        int         `json:"price_level,omitempty"`
	Photos            []Photo     `json:"photos,omitempty"`
	OpenHours         []string    `json:"open_hours,omitempty"`
	Website           string      `json:"website,omitempty"`
	PhoneNumber       string      `json:"phone_number,omitempty"`
	Neighborhood      string      `json:"neighborhood"`
	H3Index           string      `json:"h3_index"`
	FetchedAt         time.Time   `json:"fetched_at"`
	ImportanceScore   float64     `json:"importance_score,omitempty"`
	EstimatedDensity  string      `json:"estimated_density,omitempty"`
	AdditionalDetails interface{} `json:"additional_details,omitempty"`
	Primary           bool        `json:"primary"`
}

// Photo represents a photo reference from Places API
type Photo struct {
	Reference string `json:"reference"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
}

// Config holds runtime configuration
type Config struct {
	APIKey              string
	City                string
	H3Resolution        int
	AdaptiveResolution  bool
	OutputFile          string
	CompressOutput      bool
	NeighborhoodsFile   string
	MaxConcurrency      int
	QueryRadiusMeters   int
	PlaceTypes          []string
	ResumeFromIndex     int
	ImportanceThreshold float64
	Verbosity           int
	RateLimit           struct {
		QPS           float64
		MaxRetries    int
		RetryInterval time.Duration
	}
	Urban                bool
	ProgressFile         string
	PlacePrimaryTypes    []string
	ExcludedTypes        []string
	ExcludedPrimaryTypes []string
	LanguageCode         string
	RegionCode           string
	RankPreference       string // POPULARITY | DISTANCE
	// Visualization
	MapOutputFile  string // optional HTML path for coverage map (e.g., ./coverage_map.html)
	MapDrawCircles bool   // draw per-hex radius circles on the map

	// Limits H3 coverage around neighborhood center.
	// We only generate hexagons whose centers are within this radius (meters).
	CoverageRadiusMeters int
	// Optional hard override for number of H3 rings from the neighborhood center.
	// If >0, this takes precedence over CoverageRadiusMeters.
	MaxHexRings int
}

// HexagonTask represents a task to process a hexagon
type HexagonTask struct {
	HexIndex     string
	Lat          float64
	Lng          float64
	Radius       int
	Neighborhood string
}

// PlaceResult represents the result of processing a place
type PlaceResult struct {
	Place Place
	Error error
}

// Stats tracks statistics during the run
type Stats struct {
	mu                   sync.Mutex
	PlacesFound          int
	DuplicatesSkipped    int
	APICallsMade         int
	APICallsSearchNearby int
	APICallsPlaceDetails int
	ErrorCount           int
	StartTime            time.Time
	ProcessedHexagons    int
	TotalHexagons        int
	ErrorKinds           map[string]int
	RecentErrors         []string
}

// Worker represents a worker in the worker pool
type Worker struct {
	id      int
	tasks   <-chan HexagonTask
	results chan<- PlaceResult
	seenIDs *sync.Map
	limiter *rate.Limiter
	stats   *Stats
	cfg     *Config
	ctx     context.Context
	wg      *sync.WaitGroup
}

// VizCollector stores geometry needed for visualization.
type VizCollector struct {
	mu      sync.Mutex
	Centers []struct {
		Name string
		Lat  float64
		Lng  float64
	}
	Hexes []struct {
		H3           string
		Neighborhood string
		Lat          float64
		Lng          float64
		Radius       int
	}
	// New: collected places for plotting
	Places []struct {
		Neighborhood string
		Name         string
		Lat          float64
		Lng          float64
	}
	// New: boundaries per neighborhood (raw GeoJSON)
	Boundaries []struct {
		Name    string
		GeoJSON json.RawMessage
	}
}

func (v *VizCollector) addNeighborhood(name string, c LatLng) {
	v.mu.Lock()
	v.Centers = append(v.Centers, struct {
		Name string
		Lat  float64
		Lng  float64
	}{Name: name, Lat: c.Lat, Lng: c.Lng})
	v.mu.Unlock()
}

func (v *VizCollector) addHex(hexIndex string, lat, lng float64, radius int, neighborhood string) {
	v.mu.Lock()
	v.Hexes = append(v.Hexes, struct {
		H3           string
		Neighborhood string
		Lat          float64
		Lng          float64
		Radius       int
	}{H3: hexIndex, Neighborhood: neighborhood, Lat: lat, Lng: lng, Radius: radius})
	v.mu.Unlock()
}

// New: record a place for later plotting
func (v *VizCollector) addPlace(neighborhood, name string, lat, lng float64) {
	v.mu.Lock()
	v.Places = append(v.Places, struct {
		Neighborhood string
		Name         string
		Lat          float64
		Lng          float64
	}{Neighborhood: neighborhood, Name: name, Lat: lat, Lng: lng})
	v.mu.Unlock()
}

// New: record a neighborhood boundary geojson for plotting
func (v *VizCollector) addBoundary(name string, gj json.RawMessage) {
	v.mu.Lock()
	v.Boundaries = append(v.Boundaries, struct {
		Name    string
		GeoJSON json.RawMessage
	}{Name: name, GeoJSON: gj})
	v.mu.Unlock()
}

// run is the main loop for a worker.
func (w *Worker) run() {
	defer w.wg.Done()
	for task := range w.tasks {
		w.stats.mu.Lock()
		w.stats.ProcessedHexagons++
		w.stats.mu.Unlock()

		places, err := w.searchPlacesInHexagon(task)
		if err != nil {
			w.results <- PlaceResult{Error: fmt.Errorf("failed to search in hexagon %s: %w", task.HexIndex, err)}
			continue
		}

		for _, place := range places {
			w.results <- PlaceResult{Place: place}
		}
	}
}

// RunBasic runs the basic collector by dialing down advanced options.
func RunBasic(cfg *Config) error {
	if cfg.QueryRadiusMeters == 0 {
		cfg.QueryRadiusMeters = 250
	}
	if cfg.AdaptiveResolution {
		if cfg.H3Resolution > 7 {
			cfg.H3Resolution = 7
		}
	}
	if cfg.MaxConcurrency < 1 {
		cfg.MaxConcurrency = 1
	}
	if cfg.RateLimit.QPS <= 0 {
		cfg.RateLimit.QPS = 1.0
	}
	if cfg.RateLimit.MaxRetries <= 0 {
		cfg.RateLimit.MaxRetries = 5
	}
	if cfg.RateLimit.RetryInterval <= 0 {
		cfg.RateLimit.RetryInterval = 2 * time.Second
	}
	// Default coverage radius if not provided: keep neighborhood-local
	if cfg.CoverageRadiusMeters <= 0 {
		if cfg.Urban {
			cfg.CoverageRadiusMeters = 2000 // ~2 km for dense urban neighborhoods
		} else {
			cfg.CoverageRadiusMeters = 6000
		}
	}
	return runPipeline(cfg)
}

// RunAdvanced runs the advanced collector.
func RunAdvanced(cfg *Config) error {
	if cfg.QueryRadiusMeters == 0 {
		if cfg.Urban {
			cfg.QueryRadiusMeters = 200
		} else {
			cfg.QueryRadiusMeters = 500
		}
	}
	if cfg.H3Resolution == 7 && cfg.AdaptiveResolution && cfg.Urban {
		cfg.H3Resolution = 8
	}
	if cfg.MaxConcurrency < 1 {
		cfg.MaxConcurrency = 5
	}
	if cfg.RateLimit.QPS <= 0 {
		cfg.RateLimit.QPS = 1.0
	}
	if cfg.RateLimit.MaxRetries <= 0 {
		cfg.RateLimit.MaxRetries = 5
	}
	if cfg.RateLimit.RetryInterval <= 0 {
		cfg.RateLimit.RetryInterval = 2 * time.Second
	}
	// Default coverage radius if not provided
	if cfg.CoverageRadiusMeters <= 0 {
		if cfg.Urban {
			cfg.CoverageRadiusMeters = 2000
		} else {
			cfg.CoverageRadiusMeters = 6000
		}
	}
	return runPipeline(cfg)
}

// runPipeline contains the shared execution pipeline
func runPipeline(cfg *Config) error {
	setupLogging(cfg.Verbosity)

	stats := &Stats{StartTime: time.Now()}

	neighborhoods, err := loadNeighborhoods(cfg)
	if err != nil {
		return fmt.Errorf("failed to load neighborhoods: %w", err)
	}
	if len(neighborhoods) == 0 {
		return fmt.Errorf("no neighborhoods found")
	}

	stats.TotalHexagons = calculateTotalHexagons(neighborhoods, cfg)
	log.Printf("Starting search for places in %s with H3 resolution %d", cfg.City, cfg.H3Resolution)
	log.Printf("Found %d neighborhoods, estimating %d hexagons total", len(neighborhoods), stats.TotalHexagons)

	seenIDs := &sync.Map{}
	loadExistingPlaces(cfg, seenIDs)

	writer, closer, err := createOutputWriter(cfg)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %w", err)
	}
	defer closer()

	tasks := make(chan HexagonTask, max(1, cfg.MaxConcurrency*10))
	results := make(chan PlaceResult, max(1, cfg.MaxConcurrency*10))
	viz := &VizCollector{}

	var wg sync.WaitGroup
	for i := 0; i < cfg.MaxConcurrency; i++ {
		limiter := rate.NewLimiter(rate.Limit(cfg.RateLimit.QPS/float64(max(1, cfg.MaxConcurrency))), 1)
		worker := &Worker{
			id:      i,
			tasks:   tasks,
			results: results,
			seenIDs: seenIDs,
			limiter: limiter,
			stats:   stats,
			cfg:     cfg,
			ctx:     context.Background(),
			wg:      &wg,
		}
		wg.Add(1)
		go worker.run()
	}

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	// Pass viz to results processor so we can collect place points for maps
	go processResults(results, writer, stats, cfg, &resultWg, viz)

	// Pass viz to task generator
	go generateTasks(neighborhoods, tasks, cfg, stats, viz)

	wg.Wait()
	close(results)
	resultWg.Wait()

	// Emit map if enabled
	if strings.TrimSpace(cfg.MapOutputFile) != "" {
		if err := writeLeafletH3PerNeighborhood(cfg, viz); err != nil {
			log.Printf("Warning: failed to write per-neighborhood maps: %v", err)
		}
	}

	duration := time.Since(stats.StartTime)
	log.Printf("Completed search for places in %s", cfg.City)
	log.Printf("Found %d unique places (skipped %d duplicates)", stats.PlacesFound, stats.DuplicatesSkipped)
	// New: print split API call counts
	stats.mu.Lock()
	totalCalls := stats.APICallsMade
	searchCalls := stats.APICallsSearchNearby
	detailCalls := stats.APICallsPlaceDetails
	errs := stats.ErrorCount
	stats.mu.Unlock()
	log.Printf("API calls - searchNearby: %d, placeDetails: %d, total: %d; errors: %d", searchCalls, detailCalls, totalCalls, errs)
	log.Printf("Total time: %s", duration)
	log.Printf("Results saved to %s", cfg.OutputFile)
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// setupLogging configures logging based on verbosity
func setupLogging(verbosity int) {
	switch verbosity {
	case 0:
		log.SetOutput(io.Discard)
	case 1:
		log.SetFlags(log.Ldate | log.Ltime)
	case 2:
		log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	case 3:
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	}
}

// loadNeighborhoods loads neighborhoods either from file (names-only or enriched) or via searchText on city.
func loadNeighborhoods(cfg *Config) ([]Neighborhood, error) {
	if cfg.NeighborhoodsFile != "" {
		data, err := os.ReadFile(cfg.NeighborhoodsFile)
		if err == nil {
			var nameList NeighborhoodList
			if err := json.Unmarshal(data, &nameList); err == nil && len(nameList.Neighborhoods) > 0 {
				var neighborhoods []Neighborhood
				for _, name := range nameList.Neighborhoods {
					loc, err := geocodeNeighborhood(cfg, name, cfg.City)
					if err != nil {
						log.Printf("Warning: geocode failed for %s: %v", name, err)
						continue
					}
					neighborhoods = append(neighborhoods, Neighborhood{
						Name: name,
						Center: LatLng{
							Lat: loc.Lat,
							Lng: loc.Lng,
						},
					})
				}
				return neighborhoods, nil
			}

			var finderOut FinderNeighborhoodsOut
			if err := json.Unmarshal(data, &finderOut); err == nil && len(finderOut.Neighborhoods) > 0 {
				neighborhoods := make([]Neighborhood, 0, len(finderOut.Neighborhoods))
				for _, n := range finderOut.Neighborhoods {
					// New: parse boundary (MultiPolygon or Polygon)
					mp, normRaw := parseBoundaryRaw(n.Boundary)

					var center LatLng
					if n.Lat != 0 || n.Lng != 0 {
						center = LatLng{Lat: n.Lat, Lng: n.Lng}
					} else {
						loc, err := geocodeNeighborhood(cfg, n.Name, cfg.City)
						if err != nil {
							log.Printf("Warning: geocode failed for %s: %v", n.Name, err)
							continue
						}
						center = *loc
					}
					neighborhoods = append(neighborhoods, Neighborhood{
						Name:            n.Name,
						Center:          center,
						Boundary:        mp,
						BoundaryGeoJSON: normRaw,
					})
				}
				return neighborhoods, nil
			}
		}
	}

	location, err := geocodeNeighborhood(cfg, cfg.City, "")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve city center: %w", err)
	}

	return []Neighborhood{
		{
			Name: cfg.City,
			Center: LatLng{
				Lat: location.Lat,
				Lng: location.Lng,
			},
		},
	}, nil
}

// geocodeNeighborhood resolves a neighborhood to coordinates using Places v1 searchText
func geocodeNeighborhood(cfg *Config, neighborhood, city string) (*LatLng, error) {
	query := neighborhood
	if city != "" && neighborhood != city {
		query = fmt.Sprintf("%s, %s", neighborhood, city)
	}
	searchURL := placesV1Base + "/places:searchText"
	body := map[string]any{
		"textQuery": query,
	}
	fieldMask := "places.location"
	raw, err := v1DoSimple(context.Background(), cfg.APIKey, http.MethodPost, searchURL, fieldMask, body)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Places []v1Place `json:"places"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("places v1 searchText parse: %w", err)
	}
	if len(resp.Places) == 0 {
		return nil, fmt.Errorf("no results found for %s", query)
	}
	return &LatLng{
		Lat: resp.Places[0].Location.Latitude,
		Lng: resp.Places[0].Location.Longitude,
	}, nil
}

// searchPlacesInHexagon now uses Places API (New) v1 only
func (w *Worker) searchPlacesInHexagon(task HexagonTask) ([]Place, error) {
	return w.fallbackPlacesV1(task)
}

// calculateTotalHexagons estimates the total number of hexagons to be processed
func calculateTotalHexagons(neighborhoods []Neighborhood, cfg *Config) int {
	// New: if boundary is present, estimate using polyfill count at configured resolution
	estimateFor := func(n Neighborhood) int {
		if n.Boundary != nil && cfg.H3Resolution > 0 {
			return len(generateH3HexagonsWithinBoundary(n.Boundary, cfg.H3Resolution))
		}
		switch cfg.H3Resolution {
		case 6:
			return 40
		case 7:
			return 150
		case 8:
			return 500
		default:
			return 150
		}
	}

	count := 0
	for i, n := range neighborhoods {
		if i >= cfg.ResumeFromIndex && !n.Processed {
			count += estimateFor(n)
		}
	}
	return count
}

// loadExistingPlaces loads existing places from the output file to avoid duplicates
func loadExistingPlaces(cfg *Config, seenIDs *sync.Map) {
	file, err := os.Open(cfg.OutputFile)
	if err != nil {
		return
	}
	defer file.Close()

	var scanner *bufio.Scanner

	if strings.HasSuffix(cfg.OutputFile, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			log.Printf("Warning: Failed to open gzip file: %v", err)
			return
		}
		defer gzReader.Close()
		scanner = bufio.NewScanner(gzReader)
	} else {
		scanner = bufio.NewScanner(file)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var place Place
		if err := json.Unmarshal([]byte(line), &place); err != nil {
			log.Printf("Warning: Failed to parse line in existing file: %v", err)
			continue
		}
		seenIDs.Store(place.PlaceID, true)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Warning: Error reading existing file: %v", err)
	}
}

// createOutputWriter creates a writer for the output file
func createOutputWriter(cfg *Config) (*bufio.Writer, func(), error) {
	var file *os.File
	var err error
	var closer func()

	dir := filepath.Dir(cfg.OutputFile)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, nil, fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	file, err = os.OpenFile(cfg.OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open output file: %w", err)
	}

	if cfg.CompressOutput {
		gzWriter := gzip.NewWriter(file)
		bufWriter := bufio.NewWriter(gzWriter)
		closer = func() {
			bufWriter.Flush()
			gzWriter.Close()
			file.Close()
		}
		return bufWriter, closer, nil
	}

	bufWriter := bufio.NewWriter(file)
	closer = func() {
		bufWriter.Flush()
		file.Close()
	}
	return bufWriter, closer, nil
}

// generateTasks creates hexagon tasks and sends them to the tasks channel
func generateTasks(neighborhoods []Neighborhood, tasks chan<- HexagonTask, cfg *Config, stats *Stats, viz *VizCollector) {
	defer close(tasks)

	// Precompute all neighborhoods' hexes to set an accurate TotalHexagons before enqueueing.
	type nbWork struct {
		Neighborhood Neighborhood
		Resolution   int
		Hexes        []string
	}
	var queue []nbWork

	for i, neighborhood := range neighborhoods {
		if i < cfg.ResumeFromIndex || neighborhood.Processed {
			continue
		}
		resolution := cfg.H3Resolution
		if cfg.AdaptiveResolution {
			resolution = estimateOptimalResolution(neighborhood, cfg.City, cfg.Urban, cfg.QueryRadiusMeters)
		}
		var hexagons []string
		if neighborhood.Boundary != nil {
			hexagons = generateH3HexagonsWithinBoundary(neighborhood.Boundary, resolution)
			log.Printf("Generated %d hexagons for %s at resolution %d within boundary", len(hexagons), neighborhood.Name, resolution)
		} else {
			hexagons = generateH3HexagonsWithin(neighborhood.Center, resolution, cfg)
			log.Printf("Generated %d hexagons for %s at resolution %d within %dm", len(hexagons), neighborhood.Name, resolution, cfg.CoverageRadiusMeters)
		}
		hexagons = optimizeHexagonOrder(hexagons, neighborhood.Center)
		queue = append(queue, nbWork{Neighborhood: neighborhood, Resolution: resolution, Hexes: hexagons})
	}

	// Set accurate total before workers start consuming.
	stats.mu.Lock()
	stats.TotalHexagons = 0
	for _, w := range queue {
		stats.TotalHexagons += len(w.Hexes)
	}
	stats.mu.Unlock()

	// Now enqueue tasks and populate viz.
	for idx, w := range queue {
		log.Printf("[%d/%d] Processing neighborhood: %s", idx+1, len(queue), w.Neighborhood.Name)

		if viz != nil {
			viz.addNeighborhood(w.Neighborhood.Name, w.Neighborhood.Center)
			if len(w.Neighborhood.BoundaryGeoJSON) > 0 {
				viz.addBoundary(w.Neighborhood.Name, w.Neighborhood.BoundaryGeoJSON)
			}
		}

		for _, hexIndex := range w.Hexes {
			lat, lng := h3CenterCoordinates(hexIndex)
			radius := calculateOptimalRadius(lat, lng, w.Resolution, cfg.Urban)
			if cfg.QueryRadiusMeters > 0 {
				radius = cfg.QueryRadiusMeters
			}
			if viz != nil {
				viz.addHex(hexIndex, lat, lng, radius, w.Neighborhood.Name)
			}
			tasks <- HexagonTask{
				HexIndex:     hexIndex,
				Lat:          lat,
				Lng:          lng,
				Radius:       radius,
				Neighborhood: w.Neighborhood.Name,
			}
		}
	}
}

// estimateOptimalResolution determines the best H3 resolution based on area
func estimateOptimalResolution(neighborhood Neighborhood, city string, urban bool, radiusMeters int) int {
	// If caller provided a positive query radius, pick the resolution whose hex inradius
	// (center-to-edge distance) is closest to that radius at the neighborhood's center.
	if radiusMeters > 0 {
		bestRes := 7
		bestDiff := math.MaxFloat64
		for r := 5; r <= 12; r++ {
			cr := hexCircumradiusMeters(neighborhood.Center, r) // center -> vertex
			inr := cr * (math.Sqrt(3) / 2.0)                    // approx center -> edge
			diff := math.Abs(inr - float64(radiusMeters))
			if diff < bestDiff {
				bestDiff = diff
				bestRes = r
			}
		}
		return bestRes
	}
	// Fallback when no radius given: coarse defaults
	if urban {
		return 7
	}
	return 6
}

// hexCircumradiusMeters returns the distance (in meters) from the cell center to a vertex at the given resolution.
func hexCircumradiusMeters(center LatLng, res int) float64 {
	idx := h3.LatLngToCell(h3.LatLng{Lat: center.Lat, Lng: center.Lng}, res)
	b := h3.CellToBoundary(idx)
	if len(b) == 0 {
		return 0
	}
	v := b[0]
	return haversineMeters(center.Lat, center.Lng, v.Lat, v.Lng)
}

// haversineMeters computes great-circle distance in meters.
func haversineMeters(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6371000.0
	phi1 := lat1 * math.Pi / 180
	phi2 := lat2 * math.Pi / 180
	dphi := (lat2 - lat1) * math.Pi / 180
	dlam := (lng2 - lng1) * math.Pi / 180
	a := math.Sin(dphi/2)*math.Sin(dphi/2) + math.Cos(phi1)*math.Cos(phi2)*math.Sin(dlam/2)*math.Sin(dlam/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

// generateH3HexagonsWithin creates H3 hexagons centered around 'center', limited by either
// cfg.MaxHexRings (if >0) or cfg.CoverageRadiusMeters. Hexes farther than CoverageRadiusMeters
// from 'center' are filtered out.
func generateH3HexagonsWithin(center LatLng, resolution int, cfg *Config) []string {
	centerIndex := h3.LatLngToCell(h3.LatLng{Lat: center.Lat, Lng: center.Lng}, resolution)

	// Determine ring count
	rings := cfg.MaxHexRings
	if rings <= 0 {
		// Estimate inradius to translate meters -> rings
		cr := hexCircumradiusMeters(center, resolution) // center->vertex
		inr := cr * (math.Sqrt(3) / 2.0)                // center->edge (approx step)
		if inr <= 0 {
			inr = 100 // fallback safety, ~100 m
		}
		cov := cfg.CoverageRadiusMeters
		if cov <= 0 {
			// fallback if not configured; mirror Run* defaults
			if cfg.Urban {
				cov = 2000
			} else {
				cov = 6000
			}
		}
		rings = int(math.Ceil(float64(cov) / inr))
		if rings < 1 {
			rings = 1
		}
	}

	disk := h3.GridDisk(centerIndex, rings)

	// Filter by exact great-circle distance to keep within coverage radius
	out := make([]string, 0, len(disk))
	for _, cell := range disk {
		latlng := h3.CellToLatLng(cell)
		if cfg.CoverageRadiusMeters > 0 {
			if haversineMeters(center.Lat, center.Lng, latlng.Lat, latlng.Lng) > float64(cfg.CoverageRadiusMeters) {
				continue
			}
		}
		out = append(out, cell.String())
	}
	return out
}

// New: generate H3 cells strictly within a GeoJSON MultiPolygon boundary using h3 polyfill.
func generateH3HexagonsWithinBoundary(mp *GeoJSONMultiPolygon, resolution int) []string {
	if mp == nil || strings.ToLower(mp.Type) != "multipolygon" || len(mp.Coordinates) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	for _, poly := range mp.Coordinates {
		// poly: [ring][coord][2] (lon,lat)
		if len(poly) == 0 || len(poly[0]) < 3 {
			continue
		}
		// Ensure exterior CCW, holes CW (by signed area on lon/lat)
		exterior := normalizeRingOrientation(poly[0], wantCCW(true))
		holes := make([][][]float64, 0, max(0, len(poly)-1))
		for i := 1; i < len(poly); i++ {
			holes = append(holes, normalizeRingOrientation(poly[i], wantCCW(false)))
		}
		// Convert to h3 types
		toLatLngs := func(r [][]float64) []h3.LatLng {
			// Remove duplicate last point if ring closed
			if len(r) > 1 && approxEq(r[0][0], r[len(r)-1][0]) && approxEq(r[0][1], r[len(r)-1][1]) {
				r = r[:len(r)-1]
			}
			out := make([]h3.LatLng, 0, len(r))
			for _, p := range r {
				if len(p) < 2 {
					continue
				}
				lon, lat := p[0], p[1]
				out = append(out, h3.LatLng{Lat: lat, Lng: lon})
			}
			return out
		}
		exteriorLoop := h3.GeoLoop(toLatLngs(exterior))
		holeLoops := make([]h3.GeoLoop, 0, len(holes))
		for _, hr := range holes {
			holeLoops = append(holeLoops, h3.GeoLoop(toLatLngs(hr)))
		}
		gp := h3.GeoPolygon{
			GeoLoop: exteriorLoop,
			Holes:   holeLoops,
		}
		cells := h3.PolygonToCells(gp, resolution)
		for _, c := range cells {
			seen[h3.Cell(c).String()] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

// Helpers for ring orientation
func wantCCW(exterior bool) bool {
	// Exterior rings CCW, holes CW
	return exterior
}
func ringSignedAreaLonLat(r [][]float64) float64 {
	// Shoelace on lon-lat
	if len(r) < 3 {
		return 0
	}
	area := 0.0
	for i := 0; i < len(r); i++ {
		j := (i + 1) % len(r)
		area += r[i][0]*r[j][1] - r[j][0]*r[i][1] // x_i*y_{i+1} - x_{i+1}*y_i
	}
	return area / 2
}
func normalizeRingOrientation(r [][]float64, ccw bool) [][]float64 {
	if len(r) < 3 {
		return r
	}
	a := ringSignedAreaLonLat(r)
	isCCW := a > 0
	if isCCW != ccw {
		// reverse copy
		out := make([][]float64, len(r))
		for i := range r {
			out[i] = r[len(r)-1-i]
		}
		return out
	}
	return r
}
func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-12 }

// optimizeHexagonOrder reorders hexagons for efficient processing
func optimizeHexagonOrder(hexagons []string, center LatLng) []string {
	if len(hexagons) <= 20 {
		return hexagons
	}
	var core, periphery []string
	for i, hex := range hexagons {
		if i%5 == 0 {
			core = append(core, hex)
		} else {
			periphery = append(periphery, hex)
		}
	}
	return append(core, periphery...)
}

// h3CenterCoordinates returns the center coordinates of an H3 hexagon
func h3CenterCoordinates(hexIndex string) (float64, float64) {
	cell := h3.IndexFromString(hexIndex)
	center := h3.CellToLatLng(h3.Cell(cell))
	return center.Lat, center.Lng
}

// calculateOptimalRadius determines the best search radius for a location
func calculateOptimalRadius(lat, lng float64, resolution int, urban bool) int {
	baseRadius := 0
	switch resolution {
	case 6:
		baseRadius = 500
	case 7:
		baseRadius = 250
	case 8:
		baseRadius = 150
	default:
		baseRadius = 250
	}
	if urban {
		baseRadius = int(float64(baseRadius) * 0.8)
	}
	return baseRadius
}

// processResults handles the results channel and writes to output
func processResults(
	results <-chan PlaceResult,
	writer *bufio.Writer,
	stats *Stats,
	cfg *Config,
	wg *sync.WaitGroup,
	viz *VizCollector, // New param: collect places for plotting
) {
	defer wg.Done()
	var buffer []byte
	flushInterval := time.NewTicker(5 * time.Second)
	defer flushInterval.Stop()

	for {
		select {
		case result, ok := <-results:
			if !ok {
				writer.Flush()
				if cfg != nil && strings.TrimSpace(cfg.ProgressFile) != "" {
					writeProgressSnapshot(cfg, stats, 0)
				}
				return
			}
			if result.Error != nil {
				log.Printf("Error processing result: %v", result.Error)
				stats.mu.Lock()
				stats.ErrorCount++
				stats.mu.Unlock()
				continue
			}
			placeJSON, err := json.Marshal(result.Place)
			if err != nil {
				log.Printf("Failed to marshal place %s: %v", result.Place.Name, err)
				continue
			}
			if _, err := writer.Write(placeJSON); err != nil {
				log.Printf("Failed to write place to file: %v", err)
			}
			if _, err := writer.WriteString("\n"); err != nil {
				log.Printf("Failed to write newline to file: %v", err)
			}
			stats.mu.Lock()
			stats.PlacesFound++
			stats.mu.Unlock()

			// New: add to viz so we can draw markers on the map
			if viz != nil {
				viz.addPlace(
					result.Place.Neighborhood,
					result.Place.Name,
					result.Place.Location.Lat,
					result.Place.Location.Lng,
				)
			}

			buffer = append(buffer, placeJSON...)
			buffer = append(buffer, '\n')
			if len(buffer) > 1024*1024 {
				writer.Flush()
				buffer = buffer[:0]
			}
		case <-flushInterval.C:
			if len(buffer) > 0 {
				writer.Flush()
				buffer = buffer[:0]
			}
			stats.mu.Lock()
			progress := float64(stats.ProcessedHexagons) / float64(max(1, stats.TotalHexagons)) * 100
			elapsed := time.Since(stats.StartTime)
			var eta time.Duration
			if stats.ProcessedHexagons > 0 {
				eta = time.Duration(float64(elapsed) / float64(stats.ProcessedHexagons) * float64(stats.TotalHexagons-stats.ProcessedHexagons))
			}
			// New: use split API counters
			searchCalls := stats.APICallsSearchNearby
			detailCalls := stats.APICallsPlaceDetails
			totalCalls := stats.APICallsMade
			stats.mu.Unlock()

			log.Printf("Progress: %.1f%% - Found %d places - API calls: search=%d details=%d total=%d - ETA: %v",
				progress, stats.PlacesFound, searchCalls, detailCalls, totalCalls, eta.Round(time.Second))

			if cfg != nil && strings.TrimSpace(cfg.ProgressFile) != "" {
				writeProgressSnapshot(cfg, stats, eta)
			}
		}
	}
}

// writeProgressSnapshot builds a small JSON status object and writes it atomically.
func writeProgressSnapshot(cfg *Config, stats *Stats, eta time.Duration) {
	stats.mu.Lock()
	errKinds := make(map[string]int, len(stats.ErrorKinds))
	for k, v := range stats.ErrorKinds {
		errKinds[k] = v
	}
	recent := append([]string(nil), stats.RecentErrors...)
	// capture counters under lock
	apiTotal := stats.APICallsMade
	apiSearch := stats.APICallsSearchNearby
	apiDetails := stats.APICallsPlaceDetails
	processed := stats.ProcessedHexagons
	totalHex := stats.TotalHexagons
	start := stats.StartTime
	errs := stats.ErrorCount
	stats.mu.Unlock()

	s := struct {
		Timestamp            time.Time      `json:"timestamp"`
		City                 string         `json:"city"`
		OutputFile           string         `json:"output_file"`
		PlacesFound          int            `json:"places_found"`
		APICalls             int            `json:"api_calls"`
		APICallsSearchNearby int            `json:"api_calls_search_nearby"`
		APICallsPlaceDetails int            `json:"api_calls_place_details"`
		Errors               int            `json:"errors"`
		ErrorKinds           map[string]int `json:"error_kinds,omitempty"`
		RecentErrors         []string       `json:"recent_errors,omitempty"`
		ProcessedHexagons    int            `json:"processed_hexagons"`
		TotalHexagons        int            `json:"total_hexagons"`
		ProgressPercent      float64        `json:"progress_percent"`
		ElapsedSeconds       int64          `json:"elapsed_seconds"`
		ETARemainingSec      int64          `json:"eta_remaining_seconds"`
	}{
		Timestamp:            time.Now().UTC(),
		City:                 cfg.City,
		OutputFile:           cfg.OutputFile,
		PlacesFound:          stats.PlacesFound,
		APICalls:             apiTotal,
		APICallsSearchNearby: apiSearch,
		APICallsPlaceDetails: apiDetails,
		Errors:               errs,
		ErrorKinds:           errKinds,
		RecentErrors:         recent,
		ProcessedHexagons:    processed,
		TotalHexagons:        totalHex,
		ProgressPercent:      float64(processed) / float64(max(1, totalHex)) * 100.0,
		ElapsedSeconds:       int64(time.Since(start).Seconds()),
		ETARemainingSec:      int64(eta.Seconds()),
	}

	if b, err := json.MarshalIndent(s, "", "  "); err == nil {
		_ = writeJSONAtomic(cfg.ProgressFile, b)
	}
}

// writeJSONAtomic writes to a temp file and renames to target, ensuring readers see complete JSON.
func writeJSONAtomic(path string, data []byte) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// recordAPIError updates stats with error details.
func recordAPIError(stats *Stats, err error) {
	if err == nil {
		return
	}
	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.ErrorCount++

	errStr := err.Error()
	var errKey string

	// Simple error categorization
	if strings.Contains(errStr, "status=4") {
		errKey = "client_error_4xx"
	} else if strings.Contains(errStr, "status=5") {
		errKey = "server_error_5xx"
	} else if strings.Contains(errStr, "context deadline exceeded") {
		errKey = "timeout"
	} else {
		errKey = "other_network_error"
	}

	if stats.ErrorKinds == nil {
		stats.ErrorKinds = make(map[string]int)
	}
	stats.ErrorKinds[errKey]++

	if len(stats.RecentErrors) > 10 {
		stats.RecentErrors = stats.RecentErrors[1:]
	}
	stats.RecentErrors = append(stats.RecentErrors, errStr)
}

// ===== Places API (New) v1 helpers =====

const placesV1Base = "https://places.googleapis.com/v1"

type v1DisplayName struct {
	Text string `json:"text"`
}
type v1LatLng struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}
type v1Photo struct {
	Name     string `json:"name"`
	WidthPx  int    `json:"widthPx"`
	HeightPx int    `json:"heightPx"`
}
type v1OpeningHours struct {
	WeekdayDescriptions []string `json:"weekdayDescriptions"`
}
type v1Place struct {
	ID                       string          `json:"id"`
	Name                     string          `json:"name"`
	DisplayName              v1DisplayName   `json:"displayName"`
	Types                    []string        `json:"types"`
	PrimaryType              string          `json:"primaryType"` // added
	Location                 v1LatLng        `json:"location"`
	FormattedAddress         string          `json:"formattedAddress"`
	Rating                   float64         `json:"rating"`
	UserRatingCount          int             `json:"userRatingCount"`
	PriceLevel               string          `json:"priceLevel"`
	Photos                   []v1Photo       `json:"photos"`
	WebsiteURI               string          `json:"websiteUri"`
	InternationalPhoneNumber string          `json:"internationalPhoneNumber"`
	RegularOpeningHours      *v1OpeningHours `json:"regularOpeningHours"`
}

type v1SearchNearbyResp struct {
	Places        []v1Place `json:"places"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}

// v1DoSimple is a package-level helper for non-worker calls (geocoding)
func v1DoSimple(ctx context.Context, apiKey, method, url, fieldMask string, body any) ([]byte, error) {
	var req *http.Request
	var err error
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
		req, err = http.NewRequestWithContext(ctx, method, url, &buf)
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	}
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Goog-Api-Key", apiKey)
	if fieldMask != "" {
		req.Header.Set("X-Goog-FieldMask", fieldMask)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("places v1 %s %s: status=%d body=%s", method, url, resp.StatusCode, string(bs))
	}
	return bs, nil
}

// v1Do is a method on Worker for making API calls, using the worker's config and context.
func (w *Worker) v1Do(ctx context.Context, method, url, fieldMask string, body any) ([]byte, error) {
	var req *http.Request
	var err error
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
		req, err = http.NewRequestWithContext(ctx, method, url, &buf)
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	}
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Goog-Api-Key", w.cfg.APIKey)
	if fieldMask != "" {
		req.Header.Set("X-Goog-FieldMask", fieldMask)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("places v1 %s %s: status=%d body=%s", method, url, resp.StatusCode, string(bs))
	}
	return bs, nil
}

func mapV1PriceLevel(s string) int {
	switch strings.ToUpper(s) {
	case "PRICE_LEVEL_FREE":
		return 0
	case "PRICE_LEVEL_INEXPENSIVE":
		return 1
	case "PRICE_LEVEL_MODERATE":
		return 2
	case "PRICE_LEVEL_EXPENSIVE":
		return 3
	case "PRICE_LEVEL_VERY_EXPENSIVE":
		return 4
	default:
		return 0
	}
}

func (w *Worker) v1ImportanceScore(v v1Place) float64 {
	score := 0.5
	if v.Rating > 0 {
		score += (v.Rating / 5.0) * 0.3
	}
	if v.UserRatingCount > 0 {
		ratingScore := math.Min(math.Log10(float64(v.UserRatingCount))/3.0, 1.0) * 0.3
		score += ratingScore
	}
	importantTypes := map[string]bool{
		"tourist_attraction": true,
		"museum":             true,
		"park":               true,
		"point_of_interest":  true,
	}
	for _, t := range v.Types {
		if importantTypes[t] {
			score += 0.1
			break
		}
	}
	return math.Min(score, 1.0)
}

func (w *Worker) fallbackPlacesV1(task HexagonTask) ([]Place, error) {
	log.Printf("Worker %d: Places API v1 search for hexagon %s", w.id, task.HexIndex)

	if err := w.limiter.Wait(w.ctx); err != nil {
		return nil, err
	}
	// Count SearchNearby call
	w.stats.mu.Lock()
	w.stats.APICallsSearchNearby++
	w.stats.APICallsMade++
	w.stats.mu.Unlock()

	searchURL := placesV1Base + "/places:searchNearby"

	// Build type filters from config (normalize aliases, drop Table B/generic tokens)
	incAnySet := expandConfiguredTypes(w.cfg.PlaceTypes)
	incPrimSet := expandConfiguredTypes(w.cfg.PlacePrimaryTypes)
	excAnySet := expandConfiguredTypes(w.cfg.ExcludedTypes)
	excPrimSet := expandConfiguredTypes(w.cfg.ExcludedPrimaryTypes)

	includedTypes := buildIncludedTypesFromAllowed(incAnySet)
	includedPrimaryTypes := buildIncludedTypesFromAllowed(incPrimSet) // same builder works; both require Table A
	excludedTypes := buildIncludedTypesFromAllowed(excAnySet)
	excludedPrimaryTypes := buildIncludedTypesFromAllowed(excPrimSet)

	// Resolve conflicts to avoid INVALID_ARGUMENT
	includedTypes, excludedTypes = resolveConflicts(includedTypes, excludedTypes)
	includedPrimaryTypes, excludedPrimaryTypes = resolveConflicts(includedPrimaryTypes, excludedPrimaryTypes)

	// Request body
	body := map[string]any{
		"locationRestriction": map[string]any{
			"circle": map[string]any{
				"center": map[string]any{
					"latitude":  task.Lat,
					"longitude": task.Lng,
				},
				"radius": task.Radius,
			},
		},
		"maxResultCount": 20,
		"rankPreference": rankPrefOrDefault(w.cfg.RankPreference),
	}
	if len(includedTypes) > 0 {
		body["includedTypes"] = includedTypes
	}
	if len(includedPrimaryTypes) > 0 {
		body["includedPrimaryTypes"] = includedPrimaryTypes
	}
	if len(excludedTypes) > 0 {
		body["excludedTypes"] = excludedTypes
	}
	if len(excludedPrimaryTypes) > 0 {
		body["excludedPrimaryTypes"] = excludedPrimaryTypes
	}
	if lc := strings.TrimSpace(w.cfg.LanguageCode); lc != "" {
		body["languageCode"] = lc
	}
	if rc := strings.TrimSpace(w.cfg.RegionCode); rc != "" {
		body["regionCode"] = rc
	}

	// field mask selects fields; it does not filter; keep both types and primaryType per API requirement
	fieldMask := "places.id,places.displayName,places.types,places.primaryType,places.location,places.rating,places.userRatingCount,places.priceLevel"
	raw, err := w.v1Do(w.ctx, http.MethodPost, searchURL, fieldMask, body)
	if err != nil {
		recordAPIError(w.stats, err)
		return nil, err
	}

	var sresp v1SearchNearbyResp
	if err := json.Unmarshal(raw, &sresp); err != nil {
		return nil, fmt.Errorf("places v1 searchNearby parse: %w", err)
	}

	// Additionally filter client-side (in case config contained broader aliases)
	// allowedClient := mergeSets(incAnySet, incPrimSet)
	var out []Place
	for _, p := range sresp.Places {
		// if !matchesConfiguredTypes(p, allowedClient) {
		// 	continue
		// }
		if _, seen := w.seenIDs.Load(p.ID); seen {
			w.stats.mu.Lock()
			w.stats.DuplicatesSkipped++
			w.stats.mu.Unlock()
			continue
		}
		w.seenIDs.Store(p.ID, true)

		imp := w.v1ImportanceScore(p)
		if imp < w.cfg.ImportanceThreshold {
			continue
		}

		if err := w.limiter.Wait(w.ctx); err != nil {
			return out, err
		}
		// Count Place Details call
		w.stats.mu.Lock()
		w.stats.APICallsPlaceDetails++
		w.stats.APICallsMade++
		w.stats.mu.Unlock()

		placeName := placesV1Base + "/places/" + neturl.PathEscape(p.ID)
		detailMask := strings.Join([]string{
			"id",
			"displayName",
			"formattedAddress",
			"location",
			"types",
			"primaryType",
			"rating",
			"userRatingCount",
			"priceLevel",
			"photos",
			"websiteUri",
			"internationalPhoneNumber",
			"regularOpeningHours.weekdayDescriptions",
		}, ",")
		detailRaw, derr := w.v1Do(w.ctx, http.MethodGet, placeName, detailMask, nil)
		if derr != nil {
			recordAPIError(w.stats, derr)
			log.Printf("Worker %d: v1 details failed for %s: %v", w.id, p.ID, derr)
			continue
		}
		var det v1Place
		if err := json.Unmarshal(detailRaw, &det); err != nil {
			log.Printf("Worker %d: v1 details parse failed for %s: %v", w.id, p.ID, err)
			continue
		}

		photos := make([]Photo, len(det.Photos))
		for i, ph := range det.Photos {
			photos[i] = Photo{
				Reference: ph.Name,
				Width:     ph.WidthPx,
				Height:    ph.HeightPx,
			}
		}

		var openHours []string
		if det.RegularOpeningHours != nil {
			openHours = det.RegularOpeningHours.WeekdayDescriptions
		}

		density := "medium"
		if imp > 0.7 {
			density = "high"
		} else if imp < 0.3 {
			density = "low"
		}

		primary := isPrimaryByPopularity(imp, det.Rating, det.UserRatingCount, det.Types)

		out = append(out, Place{
			PlaceID: det.ID,
			Name:    det.DisplayName.Text,
			Address: det.FormattedAddress,
			Location: LatLng{
				Lat: det.Location.Latitude,
				Lng: det.Location.Longitude,
			},
			Types:            det.Types,
			Rating:           det.Rating,
			UserRatings:      det.UserRatingCount,
			PriceLevel:       mapV1PriceLevel(det.PriceLevel),
			Photos:           photos,
			OpenHours:        openHours,
			Website:          det.WebsiteURI,
			PhoneNumber:      det.InternationalPhoneNumber,
			Neighborhood:     task.Neighborhood,
			H3Index:          task.HexIndex,
			FetchedAt:        time.Now(),
			ImportanceScore:  imp,
			EstimatedDensity: density,
			Primary:          primary,
		})
	}

	return out, nil
}

// normalizeTypeToken lowercases and standardizes tokens for comparison
func normalizeTypeToken(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, "&", "and") // allow "Entertainment & Recreation"
	s = strings.ReplaceAll(s, "-", " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

// expandConfiguredTypes builds an allowed set from config, with light aliasing.
// Note: field mask does not filter; we filter client-side using this set.
func expandConfiguredTypes(raw []string) map[string]struct{} {
	allowed := make(map[string]struct{})
	for _, r := range raw {
		t := normalizeTypeToken(r)
		switch t {
		case "places_of_worship", "place_of_worship", "worship", "religious", "religious_places":
			for _, a := range []string{"church", "hindu_temple", "mosque", "synagogue"} {
				allowed[a] = struct{}{}
			}
		case "culture", "cultural":
			// Culture (Table A) concrete types
			for _, a := range []string{
				"art_gallery",
				"art_studio",
				"auditorium",
				"cultural_landmark",
				"historical_place",
				"monument",
				"museum",
				"performing_arts_theater",
				"sculpture",
			} {
				allowed[a] = struct{}{}
			}
		case "entertainment_and_recreation", "entertainment", "recreation":
			// Entertainment and Recreation (Table A) concrete types
			for _, a := range []string{
				"adventure_sports_center",
				"amphitheatre",
				"amusement_center",
				"amusement_park",
				"aquarium",
				"banquet_hall",
				"barbecue_area",
				"botanical_garden",
				"bowling_alley",
				"casino",
				"childrens_camp",
				"comedy_club",
				"community_center",
				"concert_hall",
				"convention_center",
				"cultural_center",
				"cycling_park",
				"dance_hall",
				"dog_park",
				"event_venue",
				"ferris_wheel",
				"garden",
				"hiking_area",
				"historical_landmark",
				"internet_cafe",
				"karaoke",
				"marina",
				"movie_rental",
				"movie_theater",
				"national_park",
				"night_club",
				"observation_deck",
				"off_roading_area",
				"opera_house",
				"park",
				"philharmonic_hall",
				"picnic_ground",
				"planetarium",
				"plaza",
				"roller_coaster",
				"skateboard_park",
				"state_park",
				"tourist_attraction",
				"video_arcade",
				"visitor_center",
				"water_park",
				"wedding_venue",
				"wildlife_park",
				"wildlife_refuge",
				"zoo",
			} {
				allowed[a] = struct{}{}
			}
		default:
			if t != "" {
				allowed[t] = struct{}{}
			}
		}
	}
	return allowed
}

// matchesConfiguredTypes returns true if place matches any configured type.
// If allowed is empty, no filtering is applied.
func matchesConfiguredTypes(p v1Place, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return true
	}
	if _, ok := allowed[normalizeTypeToken(p.PrimaryType)]; ok && p.PrimaryType != "" {
		return true
	}
	for _, t := range p.Types {
		if _, ok := allowed[normalizeTypeToken(t)]; ok {
			return true
		}
	}
	return false
}

// buildIncludedTypesFromAllowed converts the allowed set into a slice suitable for includedTypes,
// removing generic/Table B tokens that are not valid in Nearby Search requests.
func buildIncludedTypesFromAllowed(allowed map[string]struct{}) []string {
	if len(allowed) == 0 {
		return nil
	}
	// Common Table B/generic tokens to exclude from requests
	block := map[string]struct{}{
		"point_of_interest":           {},
		"establishment":               {},
		"place_of_worship":            {},
		"food":                        {},
		"health":                      {},
		"finance":                     {},
		"general_contractor":          {},
		"geocode":                     {},
		"landmark":                    {},
		"natural_feature":             {},
		"neighborhood":                {},
		"political":                   {},
		"country":                     {},
		"locality":                    {},
		"postal_code":                 {},
		"administrative_area_level_1": {},
		"administrative_area_level_2": {},
		"administrative_area_level_3": {},
		"administrative_area_level_4": {},
		"administrative_area_level_5": {},
		"administrative_area_level_6": {},
		"administrative_area_level_7": {},
	}
	out := make([]string, 0, len(allowed))
	for t := range allowed {
		if _, bad := block[t]; bad {
			continue
		}
		out = append(out, t)
	}
	return out
}

// resolveConflicts removes tokens present in both includes and excludes to avoid INVALID_ARGUMENT.
// Preference: keep includes and drop from excludes.
func resolveConflicts(includes, excludes []string) ([]string, []string) {
	if len(includes) == 0 || len(excludes) == 0 {
		return includes, excludes
	}
	inc := make(map[string]struct{}, len(includes))
	for _, v := range includes {
		inc[v] = struct{}{}
	}
	var exOut []string
	for _, v := range excludes {
		if _, clash := inc[v]; !clash {
			exOut = append(exOut, v)
		}
	}
	return includes, exOut
}

// rankPrefOrDefault validates rank preference.
func rankPrefOrDefault(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DISTANCE":
		return "DISTANCE"
	default:
		return "POPULARITY"
	}
}

// mergeSets returns a union of two string sets.
func mergeSets(a, b map[string]struct{}) map[string]struct{} {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}

// isPrimaryByPopularity determines if a place is a primary point of interest based on popularity.
func isPrimaryByPopularity(importanceScore float64, rating float64, userRatings int, types []string) bool {
	if importanceScore > 0.8 {
		return true
	}
	if rating > 4.5 && userRatings > 1000 {
		return true
	}
	if rating > 4.2 && userRatings > 5000 {
		return true
	}

	// Check for specific important types that might be primary even with lower scores
	for _, t := range types {
		switch t {
		case "tourist_attraction", "museum", "amusement_park", "stadium", "landmark":
			if rating > 4.0 && userRatings > 500 {
				return true
			}
		}
	}

	return false
}

// writeLeafletH3PerNeighborhood writes one Leaflet+h3-js HTML per neighborhood into cfg.MapOutputFile directory.
// Each page shows H3 coverage polygons and optional radius circles.
func writeLeafletH3PerNeighborhood(cfg *Config, viz *VizCollector) error {
	if cfg == nil || viz == nil {
		return nil
	}
	out := strings.TrimSpace(cfg.MapOutputFile)
	if out == "" {
		return nil
	}

	// Determine output directory: if a file-like path is provided, use its directory; else treat as dir.
	outDir := out
	if strings.HasSuffix(strings.ToLower(outDir), ".html") {
		outDir = filepath.Dir(outDir)
	}
	if outDir == "" || outDir == "." {
		outDir = "."
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("create map output dir: %w", err)
	}

	// Build lookup: neighborhood -> center, and neighborhood -> hexes
	type hexRec struct {
		H3     string
		Lat    float64
		Lng    float64
		Radius int
	}
	type placeRec struct {
		Name string
		Lat  float64
		Lng  float64
	}

	nbCenter := map[string]LatLng{}
	nbHexes := map[string][]hexRec{}
	// New: neighborhood -> places
	nbPlaces := map[string][]placeRec{}
	// New: neighborhood -> boundary GeoJSON
	nbBoundary := map[string]json.RawMessage{}

	viz.mu.Lock()
	for _, c := range viz.Centers {
		nbCenter[c.Name] = LatLng{Lat: c.Lat, Lng: c.Lng}
	}
	for _, h := range viz.Hexes {
		if strings.TrimSpace(h.Neighborhood) == "" || strings.TrimSpace(h.H3) == "" {
			continue
		}
		nbHexes[h.Neighborhood] = append(nbHexes[h.Neighborhood], hexRec{
			H3:     h.H3,
			Lat:    h.Lat,
			Lng:    h.Lng,
			Radius: h.Radius,
		})
	}
	// New: group places by neighborhood
	for _, p := range viz.Places {
		if strings.TrimSpace(p.Neighborhood) == "" {
			continue
		}
		nbPlaces[p.Neighborhood] = append(nbPlaces[p.Neighborhood], placeRec{
			Name: p.Name,
			Lat:  p.Lat,
			Lng:  p.Lng,
		})
	}
	// New: boundaries
	for _, b := range viz.Boundaries {
		nbBoundary[b.Name] = b.GeoJSON
	}
	viz.mu.Unlock()

	// Prefer Mapbox if token present
	mapboxToken := strings.TrimSpace(os.Getenv("MAPBOX_TOKEN"))
	useMapbox := mapboxToken != ""

	// Track filenames to avoid collisions when sanitized names are identical
	usedNames := make(map[string]int)

	for nb, hexes := range nbHexes {
		safe := sanitizeFilename(nb)
		if safe == "" {
			safe = "neighborhood"
		}
		if n := usedNames[safe]; n > 0 {
			usedNames[safe] = n + 1
			safe = fmt.Sprintf("%s-%d", safe, n+1)
		} else {
			usedNames[safe] = 1
		}
		path := filepath.Join(outDir, safe+".html")

		// Prepare JSON for JS embedding
		type jsHex struct {
			H3     string  `json:"h3"`
			Lat    float64 `json:"lat"`
			Lng    float64 `json:"lng"`
			Radius int     `json:"radius"`
		}
		jsHexes := make([]jsHex, len(hexes))
		for i, h := range hexes {
			jsHexes[i] = jsHex(h)
		}
		hexesJSON, _ := json.Marshal(jsHexes)

		// New: places for this neighborhood
		type jsPlace struct {
			Name string  `json:"name"`
			Lat  float64 `json:"lat"`
			Lng  float64 `json:"lng"`
		}
		pls := nbPlaces[nb]
		jsPlaces := make([]jsPlace, len(pls))
		for i, p := range pls {
			jsPlaces[i] = jsPlace(p)
		}
		placesJSON, _ := json.Marshal(jsPlaces)

		// New: embed boundary GeoJSON if present
		boundaryJSON := nbBoundary[nb]
		if len(boundaryJSON) == 0 {
			boundaryJSON = []byte("null")
		}

		center := nbCenter[nb]
		// If no stored center, try to use first hex center
		if center.Lat == 0 && center.Lng == 0 && len(jsHexes) > 0 {
			center = LatLng{Lat: jsHexes[0].Lat, Lng: jsHexes[0].Lng}
		}
		if center.Lat == 0 && center.Lng == 0 {
			center = LatLng{Lat: 0, Lng: 0}
		}

		var sb strings.Builder
		sb.WriteString("<!doctype html>\n<html>\n<head>\n<meta charset=\"utf-8\" />\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />\n")
		sb.WriteString("<title>Coverage Map • " + htmlEscape(nb) + "</title>\n")
		sb.WriteString("<link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" />\n")
		sb.WriteString("<style>html,body,#map{height:100%;margin:0} .legend{background:white;padding:8px;line-height:1.4;}</style>\n")
		sb.WriteString("</head>\n<body>\n<div id=\"map\"></div>\n")
		sb.WriteString("<script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>\n")
		sb.WriteString("<script src=\"https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js\"></script>\n")
		sb.WriteString("<script>\n")
		sb.WriteString("const nbName = " + toJSString(nb) + ";\n")
		sb.WriteString(fmt.Sprintf("const center = {lat:%f,lng:%f};\n", center.Lat, center.Lng))
		sb.WriteString("const drawCircles = " + boolToJS(cfg.MapDrawCircles) + ";\n")
		sb.WriteString("const hexes = ")
		sb.Write(hexesJSON)
		sb.WriteString(";\n")
		// New: embed places array
		sb.WriteString("const places = ")
		sb.Write(placesJSON)
		sb.WriteString(";\n")
		// New: embed boundary GeoJSON
		sb.WriteString("const boundary = ")
		sb.Write(boundaryJSON)
		sb.WriteString(";\n")

		// Initialize map
		sb.WriteString("const map = L.map('map').setView([center.lat, center.lng], 12);\n")
		if useMapbox {
			sb.WriteString("L.tileLayer('https://api.mapbox.com/styles/v1/mapbox/streets-v11/tiles/{z}/{x}/{y}?access_token=" + jsEscape(mapboxToken) + "', {tileSize:512, zoomOffset:-1, maxZoom:19, attribution:'© Mapbox © OpenStreetMap contributors'}).addTo(map);\n")
		} else {
			sb.WriteString("L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:19, attribution:'© OpenStreetMap contributors'}).addTo(map);\n")
		}

		sb.WriteString("const hexLayer = L.layerGroup().addTo(map);\n")
		sb.WriteString("const circleLayer = L.layerGroup().addTo(map);\n")
		// New: layer for place markers
		sb.WriteString("const placeLayer = L.layerGroup().addTo(map);\n")
		// New: boundary layer
		sb.WriteString("const boundaryLayer = L.layerGroup().addTo(map);\n")

		// Draw polygons
		sb.WriteString("hexes.forEach(h => {\n")
		sb.WriteString("  const boundary = h3.cellToBoundary(h.h3, false);\n") // was true; Leaflet needs [lat,lng]
		sb.WriteString("  const poly = L.polygon(boundary, {color:'#1f78b4', weight:1, fillOpacity:0.25}).bindTooltip(nbName + ' • ' + h.h3, {sticky:true});\n")
		sb.WriteString("  poly.addTo(hexLayer);\n")
		sb.WriteString("});\n")

		// Draw circles if enabled
		sb.WriteString("if (drawCircles) {\n")
		sb.WriteString("  hexes.forEach(h => {\n")
		sb.WriteString("    L.circle([h.lat, h.lng], {radius:h.radius, color:'#ff7f0e', weight:1, fillOpacity:0.05}).bindTooltip('r=' + h.radius + 'm').addTo(circleLayer);\n")
		sb.WriteString("  });\n")
		sb.WriteString("}\n")

		// New: draw boundary if provided
		sb.WriteString("if (boundary) {\n")
		sb.WriteString("  const gj = L.geoJSON(boundary, {style:{color:'#2ca02c', weight:2, fillOpacity:0.08}}).addTo(boundaryLayer);\n")
		sb.WriteString("}\n")

		// New: draw place markers with hover tooltip (name, lat, lng)
		sb.WriteString("places.forEach(p => {\n")
		sb.WriteString("  const m = L.circleMarker([p.lat, p.lng], {radius:4, color:'#d62728', weight:1, fillOpacity:0.8});\n")
		sb.WriteString("  m.bindTooltip(`${p.name}<br/>(${p.lat.toFixed(5)}, ${p.lng.toFixed(5)})`, {sticky:true});\n")
		sb.WriteString("  m.addTo(placeLayer);\n")
		sb.WriteString("});\n")

		// Fit bounds (include both hex vertices and place points)
		sb.WriteString("if (hexes.length > 0 || places.length > 0) {\n")
		sb.WriteString("  const all = [];\n")
		sb.WriteString("  hexes.forEach(h => { const b = h3.cellToBoundary(h.h3, false); b.forEach(p => all.push(p)); });\n")
		sb.WriteString("  places.forEach(p => all.push([p.lat, p.lng]));\n")
		sb.WriteString("  const lats = all.map(p => p[0]); const lngs = all.map(p => p[1]);\n")
		sb.WriteString("  const minLat = Math.min.apply(null, lats), maxLat = Math.max.apply(null, lats);\n")
		sb.WriteString("  const minLng = Math.min.apply(null, lngs), maxLng = Math.max.apply(null, lngs);\n")
		sb.WriteString("  map.fitBounds([[minLat, minLng],[maxLat, maxLng]], {padding:[20,20]});\n")
		sb.WriteString("}\n")

		// Legend
		sb.WriteString("const legend = L.control({position:'bottomleft'});\n")
		sb.WriteString("legend.onAdd = function(){const div=L.DomUtil.create('div','legend'); div.innerHTML = ")
		sb.WriteString(toJSString(
			"<div><b>" + htmlEscape(nb) + "</b></div>" +
				"<div><span style=\"display:inline-block;width:12px;height:12px;background:#1f78b4;opacity:0.5;margin-right:6px;border:1px solid #1f78b4\"></span>H3 Coverage</div>" +
				"<div><span style=\"display:inline-block;width:12px;height:12px;background:#ff7f0e;opacity:0.3;margin-right:6px;border:1px solid #ff7f0e\"></span>Search Radius</div>" +
				"<div><span style=\"display:inline-block;width:12px;height:12px;background:#2ca02c;opacity:0.15;margin-right:6px;border:2px solid #2ca02c\"></span>Boundary</div>" +
				"<div><span style=\"display:inline-block;width:12px;height:12px;background:#d62728;opacity:0.9;margin-right:6px;border:1px solid #d62728;border-radius:50%\"></span>Places</div>",
		))
		sb.WriteString("; return div; };\n")
		sb.WriteString("legend.addTo(map);\n")

		sb.WriteString("</script>\n</body>\n</html>\n")

		if err := os.WriteFile(path, []byte(sb.String()), 0644); err != nil {
			log.Printf("failed to write map for %s: %v", nb, err)
			continue
		}
		log.Printf("Neighborhood map written: %s", path)
	}
	return nil
}

// sanitizeFilename creates a filesystem-safe lowercase stem for filenames.
func sanitizeFilename(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = strings.ReplaceAll(s, " ", "_")
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		}
	}
	return strings.Trim(b.String(), "._-")
}

// htmlEscape escapes HTML special characters.
func htmlEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#39;").Replace(s)
}

// toJSString safely wraps a string as a JS string literal.
func toJSString(s string) string {
	return `"` + jsEscape(s) + `"`
}

// jsEscape escapes content for JS string literal context.
func jsEscape(s string) string {
	repl := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`, "</", "<\\/", "\t", `\t`)
	return repl.Replace(s)
}

// boolToJS converts a Go bool to a JS boolean literal string.
func boolToJS(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// parseBoundaryRaw attempts to parse boundary from raw JSON into a normalized MultiPolygon and returns both
// the parsed struct and a normalized raw JSON (MultiPolygon). Returns (nil, nil) if not present.
func parseBoundaryRaw(raw json.RawMessage) (*GeoJSONMultiPolygon, json.RawMessage) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "" {
		return nil, nil
	}
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(head.Type)) {
	case "multipolygon":
		var mp GeoJSONMultiPolygon
		if err := json.Unmarshal(raw, &mp); err != nil || len(mp.Coordinates) == 0 {
			return nil, nil
		}
		norm, _ := json.Marshal(mp)
		return &mp, norm
	case "polygon":
		var pg GeoJSONPolygon
		if err := json.Unmarshal(raw, &pg); err != nil || len(pg.Coordinates) == 0 {
			return nil, nil
		}
		mp := GeoJSONMultiPolygon{
			Type:        "MultiPolygon",
			Coordinates: [][][][]float64{pg.Coordinates},
		}
		norm, _ := json.Marshal(mp)
		return &mp, norm
	default:
		return nil, nil
	}
}

// New: minimal GeoJSON types (WGS84; coordinates are [lng,lat])
type GeoJSONMultiPolygon struct {
	Type        string          `json:"type"`
	Coordinates [][][][]float64 `json:"coordinates"` // [poly][ring][coord][2]
}
type GeoJSONPolygon struct {
	Type        string        `json:"type"`
	Coordinates [][][]float64 `json:"coordinates"` // [ring][coord][2]
}
