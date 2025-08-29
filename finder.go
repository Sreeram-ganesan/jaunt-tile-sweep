package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ----------- Config -----------

const (
	tavilySearchURL  = "https://api.tavily.com/search"
	tavilyExtractURL = "https://api.tavily.com/extract"

	openAIChatURL = "https://api.openai.com/v1/chat/completions"
	openAIModel   = "gpt-4o-mini"

	geminiURL   = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
	geminiModel = "gemini-1.5-flash"

	httpTimeout = 30 * time.Second
)

// ----------- Tavily models -----------

type tavilySearchRequest struct {
	Query          string   `json:"query"`
	SearchDepth    string   `json:"search_depth,omitempty"`
	IncludeAnswer  bool     `json:"include_answer,omitempty"`
	MaxResults     int      `json:"max_results,omitempty"`
	IncludeDomains []string `json:"include_domains,omitempty"`
	ExcludeDomains []string `json:"exclude_domains,omitempty"`
}

type tavilySearchResult struct {
	Title   string  `json:"title"`
	URL     string  `json:"url"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`
}

type tavilySearchResponse struct {
	Answer  string               `json:"answer"`
	Results []tavilySearchResult `json:"results"`
}

// tavilyExtractRequest represents the request to extract content from a URL
type tavilyExtractRequest struct {
	URLs []string `json:"urls"` // Changed from URL to URLs
}

// tavilyExtractResponse represents the response from the extract API
type tavilyExtractResponse struct {
	Results []struct {
		URL        string `json:"url"`
		Title      string `json:"title"`
		Content    string `json:"content,omitempty"`     // sometimes present
		RawContent string `json:"raw_content,omitempty"` // alternative field
		Text       string `json:"text,omitempty"`        // alternative field
		HTML       string `json:"html,omitempty"`        // alternative field
	} `json:"results"`
}

// ----------- LLM request/response -----------

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatRequest struct {
	Model          string          `json:"model"`
	Messages       []openAIMessage `json:"messages"`
	Temperature    float64         `json:"temperature,omitempty"`
	ResponseFormat struct {
		Type string `json:"type"`
	} `json:"response_format,omitempty"`
}

type openAIChoice struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
}

type openAIChatResponse struct {
	Choices []openAIChoice `json:"choices"`
}

type geminiRequest struct {
	Contents []struct {
		Parts []struct {
			Text string `json:"text"`
		} `json:"parts"`
	} `json:"contents"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

// ----------- Config for Finder -----------

type FinderConfig struct {
	City           string
	Query          string // optional custom query
	LLM            string // openai|gemini
	MaxResults     int
	SkipTavily     bool
	ExtractContent bool
	OutputFile     string // where to write the neighborhoods JSON (optional)

	// Search/extract tuning (all optional)
	IncludeDomains     []string // prioritize or restrict to these domains
	ExcludeDomains     []string // drop these domains
	MinResultScore     float64  // drop Tavily results below this score
	MaxExtractChars    int      // truncate extracted content to this many characters
	ParallelExtractors int      // parallelism when chunking extraction
	UseOSMContext      bool     // include OSM neighborhoods as an additional source/context and prefer them during merge
}

// ----------- Final JSON shape -----------

type FinderNeighborhood struct {
	Name string  `json:"name"`
	Lat  float64 `json:"lat"`
	Lng  float64 `json:"lng"`
	// Optional GeoJSON geometry; expected to be a MultiPolygon in WGS84.
	// Stored as raw JSON to avoid enforcing schema here.
	Boundary json.RawMessage `json:"boundary,omitempty"`
}

type FinderNeighborhoodsOut struct {
	Neighborhoods []FinderNeighborhood `json:"neighborhoods"`
}

// ----------- Helpers -----------

func postJSON(url string, body any, headers map[string]string) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("POST %s: status=%d body=%s", url, resp.StatusCode, string(bs))
	}
	return bs, nil
}

// generic POST with retry/backoff for Tavily calls
func postJSONWithRetry(url string, body any, headers map[string]string, maxRetries int, backoff time.Duration) ([]byte, error) {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if backoff <= 0 {
		backoff = 1 * time.Second
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		raw, err := postJSON(url, body, headers)
		if err == nil {
			return raw, nil
		}
		lastErr = err
		time.Sleep(backoff * time.Duration(1<<attempt))
	}
	return nil, fmt.Errorf("postJSONWithRetry: %w", lastErr)
}

// strip HTML tags, collapse whitespace, and trim
func cleanText(s string) string {
	if s == "" {
		return s
	}
	// quick strip tags
	re := regexp.MustCompile(`(?s)<[^>]+>`)
	s = re.ReplaceAllString(s, " ")
	// unescape some common HTML entities (keep minimal to avoid extra deps)
	reEntities := strings.NewReplacer("&nbsp;", " ", "&amp;", "&", "&lt;", "<", "&gt;", ">", "&#39;", "'", "&quot;", `"`)
	s = reEntities.Replace(s)
	// collapse whitespace
	ws := regexp.MustCompile(`\s+`)
	s = ws.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func truncate(s string, max int) string {
	if max > 0 && len(s) > max {
		return s[:max]
	}
	return s
}

func domainOf(u string) string {
	pu, err := url.Parse(u)
	if err != nil {
		return ""
	}
	return strings.ToLower(pu.Hostname())
}

func normalizeURL(u string) string {
	pu, err := url.Parse(u)
	if err != nil {
		return u
	}
	pu.Fragment = ""
	pu.RawQuery = ""
	// remove trailing slash
	pu.Path = strings.TrimRight(pu.Path, "/")
	return pu.String()
}

// filter, prioritize, and deduplicate Tavily results
func filterTavilyResults(in []tavilySearchResult, cfg *FinderConfig) []tavilySearchResult {
	minScore := cfg.MinResultScore
	include := make(map[string]struct{})
	exclude := make(map[string]struct{})
	for _, d := range cfg.IncludeDomains {
		include[strings.ToLower(strings.TrimSpace(d))] = struct{}{}
	}
	for _, d := range cfg.ExcludeDomains {
		exclude[strings.ToLower(strings.TrimSpace(d))] = struct{}{}
	}

	seenURL := make(map[string]struct{})
	seenDomain := make(map[string]int) // keep count per domain
	out := make([]tavilySearchResult, 0, len(in))

	for _, r := range in {
		if minScore > 0 && r.Score < minScore {
			continue
		}
		u := normalizeURL(r.URL)
		if _, ok := seenURL[u]; ok {
			continue
		}
		d := domainOf(u)
		if _, bad := exclude[d]; bad {
			continue
		}
		// if include list present, restrict to it
		if len(include) > 0 {
			if _, ok := include[d]; !ok {
				continue
			}
		}
		seenURL[u] = struct{}{}
		seenDomain[d]++
		out = append(out, r)
	}

	// sort: prefer included domains first, then by score desc, then by fewer duplicates domain usage
	sort.SliceStable(out, func(i, j int) bool {
		di := domainOf(out[i].URL)
		dj := domainOf(out[j].URL)
		ini := len(include) == 0
		inj := len(include) == 0
		if !ini {
			_, ini = include[di]
		}
		if !inj {
			_, inj = include[dj]
		}
		if ini != inj {
			return ini // included domain first
		}
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return seenDomain[di] < seenDomain[dj]
	})

	return out
}

// ----------- Tavily -----------

func tavilySearch(apiKey, query string, maxResults int, include, exclude []string) (*tavilySearchResponse, error) {
	payload := tavilySearchRequest{
		Query:          query,
		SearchDepth:    "advanced",
		IncludeAnswer:  true,
		MaxResults:     maxResults,
		IncludeDomains: include,
		ExcludeDomains: exclude,
	}
	headers := map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
	}

	raw, err := postJSONWithRetry(tavilySearchURL, payload, headers, 3, 1*time.Second)
	if err != nil {
		return nil, err
	}
	var out tavilySearchResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// batch extract multiple URLs in one call
func tavilyExtractBatch(apiKey string, urls []string) (*tavilyExtractResponse, error) {
	payload := tavilyExtractRequest{URLs: urls}
	headers := map[string]string{
		"Authorization": "Bearer " + apiKey,
		"Content-Type":  "application/json",
	}
	raw, err := postJSONWithRetry(tavilyExtractURL, payload, headers, 3, 1*time.Second)
	if err != nil {
		return nil, err
	}
	var out tavilyExtractResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("tavily extract unmarshal error: %w, raw response: %s", err, string(raw))
	}
	return &out, nil
}

// pickExtractedContent chooses the first non-empty field from extract response.
func pickExtractedContent(extract *tavilyExtractResponse) string {
	if extract == nil || len(extract.Results) == 0 {
		return ""
	}
	r := extract.Results[0]
	switch {
	case strings.TrimSpace(r.Text) != "":
		return r.Text
	case strings.TrimSpace(r.RawContent) != "":
		return r.RawContent
	case strings.TrimSpace(r.Content) != "":
		return r.Content
	case strings.TrimSpace(r.HTML) != "":
		// Very simple HTML fallback; keep as-is to avoid adding extra deps.
		return r.HTML
	default:
		return ""
	}
}

// ----------- LLM calls -----------

func callOpenAI(apiKey, systemPrompt, userPrompt string) (string, error) {
	req := openAIChatRequest{
		Model: openAIModel,
		Messages: []openAIMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		Temperature: 0.2,
	}
	req.ResponseFormat.Type = "json_object"
	raw, err := postJSON(openAIChatURL, req, map[string]string{
		"Authorization": "Bearer " + apiKey,
	})
	if err != nil {
		return "", err
	}
	var out openAIChatResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", err
	}
	if len(out.Choices) == 0 {
		return "", errors.New("openai: no choices")
	}
	return strings.TrimSpace(out.Choices[0].Message.Content), nil
}

func callGemini(apiKey, systemPrompt, userPrompt string) (string, error) {
	fullPrompt := systemPrompt + "\n\n" + userPrompt
	fmt.Fprintln(os.Stderr, "Full Gemini Prompt:\n"+fullPrompt)
	req := map[string]interface{}{
		"contents": []map[string]interface{}{
			{
				"parts": []map[string]interface{}{
					{"text": fullPrompt},
				},
			},
		},
	}

	url := geminiURL + "?key=" + apiKey
	raw, err := postJSON(url, req, nil)
	if err != nil {
		return "", err
	}
	var out geminiResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("gemini unmarshal error: %w, raw response: %s", err, string(raw))
	}
	if len(out.Candidates) == 0 || len(out.Candidates[0].Content.Parts) == 0 {
		return "", errors.New("gemini: no candidates")
	}
	return strings.TrimSpace(out.Candidates[0].Content.Parts[0].Text), nil
}

// fillMissingBoundariesWithLLM prompts the configured LLM (no Tavily) to return GeoJSON MultiPolygon
// boundaries for the given city and the subset of neighborhoods missing boundary data.
// It returns an updated slice with newly filled boundaries where available.
func fillMissingBoundariesWithLLM(cfg *FinderConfig, city string, items []FinderNeighborhood) []FinderNeighborhood {
	// Collect missing
	var missing []string
	for _, n := range items {
		if len(strings.TrimSpace(string(n.Boundary))) == 0 || string(n.Boundary) == "null" {
			if strings.TrimSpace(n.Name) != "" {
				missing = append(missing, n.Name)
			}
		}
	}
	if len(missing) == 0 {
		return items
	}

	systemPrompt := `You are a geospatial assistant.
Return ONLY valid JSON in this exact format:
{"boundaries":[{"name":"...","boundary":{"type":"MultiPolygon","coordinates":[...]}} , ...]}
Rules:
- Coordinate reference system must be WGS84 (EPSG:4326).
- "boundary" must be a GeoJSON MultiPolygon. If only a Polygon is available, return it wrapped as a MultiPolygon.
- If you are not reasonably confident or boundary is unavailable, set "boundary" to null for that item.
- Return nothing except the JSON object.`
	userPrompt := fmt.Sprintf(`City: %s
Neighborhoods needing boundaries (names only): %s

For each name, return a record:
{"name":"<name>","boundary": <GeoJSON MultiPolygon or null>}

Output:
{"boundaries":[... as described ...]}`, city, strings.Join(missing, ", "))

	var llmOut string
	var err error
	switch strings.ToLower(cfg.LLM) {
	case "gemini":
		llmOut, err = callGemini(os.Getenv("GOOGLE_API_KEY"), systemPrompt, userPrompt)
	default:
		llmOut, err = callOpenAI(os.Getenv("OPENAI_API_KEY"), systemPrompt, userPrompt)
	}
	if err != nil || strings.TrimSpace(llmOut) == "" {
		// Leave as-is on failure
		return items
	}

	// Parse response
	var parsed struct {
		Boundaries []struct {
			Name     string          `json:"name"`
			Boundary json.RawMessage `json:"boundary"`
		} `json:"boundaries"`
	}
	clean := strings.TrimSpace(llmOut)
	if json.Unmarshal([]byte(clean), &parsed) != nil {
		// attempt to trim to outermost JSON object if LLM added noise
		if !strings.HasPrefix(clean, "{") {
			if i := strings.Index(clean, "{"); i >= 0 {
				clean = clean[i:]
			}
		}
		if !strings.HasSuffix(clean, "}") {
			if j := strings.LastIndex(clean, "}"); j >= 0 {
				clean = clean[:j+1]
			}
		}
		_ = json.Unmarshal([]byte(clean), &parsed)
	}

	// Normalize and apply
	if len(parsed.Boundaries) == 0 {
		return items
	}
	byName := make(map[string]json.RawMessage, len(parsed.Boundaries))
	for _, b := range parsed.Boundaries {
		if strings.TrimSpace(b.Name) == "" || len(b.Boundary) == 0 {
			continue
		}
		// Normalize to MultiPolygon (and validate)
		if mp, norm := parseBoundaryRaw(b.Boundary); mp != nil && len(norm) > 0 {
			byName[strings.ToLower(strings.TrimSpace(b.Name))] = norm
		}
	}

	for i := range items {
		key := strings.ToLower(strings.TrimSpace(items[i].Name))
		if norm, ok := byName[key]; ok {
			items[i].Boundary = norm
		}
	}
	return items
}

// fillMissingBoundariesWithOSM uses OpenStreetMap Nominatim to fetch MultiPolygon GeoJSON
// boundaries for neighborhoods that still have missing boundary data.
// It queries: https://nominatim.openstreetmap.org/search?format=geojson&polygon_geojson=1&limit=1&q=<name, city>
func fillMissingBoundariesWithOSM(cfg *FinderConfig, city string, items []FinderNeighborhood) []FinderNeighborhood {
	type fc struct {
		Type     string `json:"type"`
		Features []struct {
			Geometry json.RawMessage `json:"geometry"`
		} `json:"features"`
	}

	// polite UA per Nominatim usage policy
	ua := strings.TrimSpace(os.Getenv("NOMINATIM_USER_AGENT"))
	if ua == "" {
		contact := strings.TrimSpace(os.Getenv("CONTACT_EMAIL"))
		if contact == "" {
			contact = "contact@example.com"
		}
		ua = "jaunt-neighborhood-finder/1.0 (" + contact + ")"
	}

	client := &http.Client{Timeout: httpTimeout}

	needs := make([]int, 0, len(items))
	for i, n := range items {
		if len(strings.TrimSpace(string(n.Boundary))) == 0 || string(n.Boundary) == "null" {
			if strings.TrimSpace(n.Name) != "" {
				needs = append(needs, i)
			}
		}
	}
	if len(needs) == 0 {
		return items
	}

	for _, idx := range needs {
		name := items[idx].Name
		q := name
		if strings.TrimSpace(city) != "" && !strings.Contains(strings.ToLower(name), strings.ToLower(city)) {
			q = fmt.Sprintf("%s, %s", name, city)
		}
		values := url.Values{}
		values.Set("format", "geojson")
		values.Set("polygon_geojson", "1")
		values.Set("addressdetails", "0")
		values.Set("limit", "1")
		values.Set("q", q)

		req, err := http.NewRequest(http.MethodGet, "https://nominatim.openstreetmap.org/search?"+values.Encode(), nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", ua)
		ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
		req = req.WithContext(ctx)

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			continue
		}
		bs, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 || len(bs) == 0 {
			continue
		}

		var out fc
		if json.Unmarshal(bs, &out) != nil || strings.ToLower(out.Type) != "featurecollection" || len(out.Features) == 0 {
			continue
		}
		geom := out.Features[0].Geometry
		// Normalize to MultiPolygon using shared helper
		if mp, norm := parseBoundaryRaw(geom); mp != nil && len(norm) > 0 {
			items[idx].Boundary = norm
		}

		// be polite to Nominatim
		time.Sleep(500 * time.Millisecond)
	}
	return items
}

// fetchOSMNeighborhoods queries Overpass for likely neighborhood/ward/suburb relations within the city.
// It returns a minimal list (name + centroid) and a human-readable bundle for LLM context.
func fetchOSMNeighborhoods(city string) ([]FinderNeighborhood, string) {
	if strings.TrimSpace(city) == "" {
		return nil, ""
	}

	// Polite UA per Nominatim/Overpass policies
	ua := strings.TrimSpace(os.Getenv("NOMINATIM_USER_AGENT"))
	if ua == "" {
		contact := strings.TrimSpace(os.Getenv("CONTACT_EMAIL"))
		if contact == "" {
			contact = "contact@example.com"
		}
		ua = "jaunt-neighborhood-finder/1.0 (" + contact + ")"
	}

	// Overpass QL: find admin area by name, then fetch neighborhood-like relations inside it.
	cityEsc := strings.ReplaceAll(city, `"`, `\"`)
	query := fmt.Sprintf(`
[out:json][timeout:25];
area[name="%s"]["boundary"="administrative"]->.a;
(
  relation["boundary"="administrative"]["admin_level"~"^(8|9|10|11)$"](area.a);
  relation["place"~"^(neighbourhood|neighborhood|suburb|quarter|ward)$"](area.a);
);
out tags center;
`, cityEsc)

	form := url.Values{}
	form.Set("data", query)

	req, err := http.NewRequest(http.MethodPost, "https://overpass-api.de/api/interpreter", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, ""
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", ua)

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, ""
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || len(bs) == 0 {
		return nil, ""
	}

	var overpass struct {
		Elements []struct {
			Type   string            `json:"type"`
			ID     int64             `json:"id"`
			Tags   map[string]string `json:"tags"`
			Center *struct {
				Lat float64 `json:"lat"`
				Lon float64 `json:"lon"`
			} `json:"center,omitempty"`
		} `json:"elements"`
	}
	if json.Unmarshal(bs, &overpass) != nil {
		return nil, ""
	}

	var out []FinderNeighborhood
	var lines []string
	seen := map[string]struct{}{}
	for _, el := range overpass.Elements {
		name := strings.TrimSpace(el.Tags["name"])
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		var lat, lng float64
		if el.Center != nil {
			lat, lng = el.Center.Lat, el.Center.Lon
		}
		out = append(out, FinderNeighborhood{
			Name:     name,
			Lat:      lat,
			Lng:      lng,
			Boundary: nil, // boundary fill happens in later passes
		})
		lines = append(lines, fmt.Sprintf("- %s | centroid: %.6f, %.6f", name, lat, lng))
	}

	if len(lines) == 0 {
		return out, ""
	}
	return out, "OSM neighborhoods (Overpass):\n" + strings.Join(lines, "\n")
}

// mergePreferOSM merges LLM neighborhoods with OSM-derived ones, preferring OSM when LLM data is missing.
// - Adds OSM entries not present in LLM by name (case-insensitive).
// - If LLM entry exists but lacks centroid (0,0), and OSM has it, fill from OSM.
// - If LLM boundary missing, keep as-is; boundary fill runs later via LLM/OSM helpers.
func mergePreferOSM(llm []FinderNeighborhood, osm []FinderNeighborhood) []FinderNeighborhood {
	if len(osm) == 0 {
		return llm
	}
	idx := make(map[string]int, len(llm))
	for i, n := range llm {
		k := strings.ToLower(strings.TrimSpace(n.Name))
		if k != "" {
			idx[k] = i
		}
	}
	for _, on := range osm {
		k := strings.ToLower(strings.TrimSpace(on.Name))
		if k == "" {
			continue
		}
		if i, ok := idx[k]; ok {
			// Fill missing centroid if LLM has zero and OSM has non-zero
			if (llm[i].Lat == 0 && llm[i].Lng == 0) && (on.Lat != 0 || on.Lng != 0) {
				llm[i].Lat, llm[i].Lng = on.Lat, on.Lng
			}
			// Leave boundary to dedicated fill passes
			continue
		}
		// Not present in LLM; add OSM entry
		llm = append(llm, on)
	}
	return llm
}

// ----------- OSM Anonymous Boundary Mapping -----------

type OSMAnonBoundary struct {
	ID      string  // e.g., "R123456"
	Lat     float64 // centroid lat
	Lng     float64 // centroid lng
	MinLat  float64 // bbox
	MinLng  float64
	MaxLat  float64
	MaxLng  float64
	AreaKm2 float64 // approx from bbox
}

func fetchOSMAnonymousBoundariesForCity(city string) ([]OSMAnonBoundary, error) {
	if strings.TrimSpace(city) == "" {
		return nil, nil
	}
	ua := strings.TrimSpace(os.Getenv("NOMINATIM_USER_AGENT"))
	if ua == "" {
		contact := strings.TrimSpace(os.Getenv("CONTACT_EMAIL"))
		if contact == "" {
			contact = "contact@example.com"
		}
		ua = "jaunt-neighborhood-finder/1.0 (" + contact + ")"
	}

	// Overpass: city admin area -> relations that look like neighborhoods/wards (no names required)
	cityEsc := strings.ReplaceAll(city, `"`, `\"`)
	query := fmt.Sprintf(`
[out:json][timeout:25];
area[name="%s"]["boundary"="administrative"]->.a;
(
  relation["boundary"="administrative"]["admin_level"~"^(8|9|10|11)$"](area.a);
  relation["place"~"^(neighbourhood|neighborhood|suburb|quarter|ward)$"](area.a);
);
out ids center bb;
`, cityEsc)

	form := url.Values{}
	form.Set("data", query)

	req, err := http.NewRequest(http.MethodPost, "https://overpass-api.de/api/interpreter", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", ua)

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || len(bs) == 0 {
		return nil, fmt.Errorf("overpass anonymous boundaries: status=%d", resp.StatusCode)
	}

	var overpass struct {
		Elements []struct {
			Type   string `json:"type"`
			ID     int64  `json:"id"`
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
		} `json:"elements"`
	}
	if err := json.Unmarshal(bs, &overpass); err != nil {
		return nil, err
	}

	var out []OSMAnonBoundary
	seen := map[string]struct{}{}
	for _, el := range overpass.Elements {
		if el.Type != "relation" {
			continue
		}
		id := fmt.Sprintf("R%d", el.ID)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		var lat, lng float64
		if el.Center != nil {
			lat, lng = el.Center.Lat, el.Center.Lon
		}
		var minLat, minLng, maxLat, maxLng float64
		if el.Bounds != nil {
			minLat, minLng, maxLat, maxLng = el.Bounds.MinLat, el.Bounds.MinLon, el.Bounds.MaxLat, el.Bounds.MaxLon
		}
		// Rough area estimate from bbox (meters) -> km^2
		var areaKm2 float64
		if minLat != 0 || minLng != 0 || maxLat != 0 || maxLng != 0 {
			h := haversineMeters(minLat, minLng, maxLat, minLng)
			w := haversineMeters(minLat, minLng, minLat, maxLng)
			areaKm2 = (h * w) / 1e6
		}
		out = append(out, OSMAnonBoundary{
			ID: id, Lat: lat, Lng: lng,
			MinLat: minLat, MinLng: minLng, MaxLat: maxLat, MaxLng: maxLng,
			AreaKm2: areaKm2,
		})
	}
	return out, nil
}

func mapNamesToOSMByLLM(cfg *FinderConfig, city string, names []string, candidates []OSMAnonBoundary) map[string]string {
	if len(names) == 0 || len(candidates) == 0 {
		return nil
	}
	type cand struct {
		ID       string `json:"id"`
		Centroid struct {
			Lat float64 `json:"lat"`
			Lng float64 `json:"lng"`
		} `json:"centroid"`
		BBox struct {
			MinLat float64 `json:"min_lat"`
			MinLng float64 `json:"min_lng"`
			MaxLat float64 `json:"max_lat"`
			MaxLng float64 `json:"max_lng"`
		} `json:"bbox"`
		AreaKm2 float64 `json:"area_km2,omitempty"`
	}
	cands := make([]cand, 0, len(candidates))
	for _, c := range candidates {
		var v cand
		v.ID = c.ID
		v.Centroid.Lat, v.Centroid.Lng = c.Lat, c.Lng
		v.BBox.MinLat, v.BBox.MinLng, v.BBox.MaxLat, v.BBox.MaxLng = c.MinLat, c.MinLng, c.MaxLat, c.MaxLng
		v.AreaKm2 = c.AreaKm2
		cands = append(cands, v)
	}
	payload := struct {
		City       string   `json:"city"`
		Names      []string `json:"names"`
		Candidates []cand   `json:"candidates"`
	}{
		City:       city,
		Names:      names,
		Candidates: cands,
	}
	js, _ := json.MarshalIndent(payload, "", "  ")

	system := `You are a geospatial normalizer.
Given a city, a list of neighborhood names, and a set of anonymous OSM boundary candidates (no names),
choose the best matching boundary id for each name.
Guidelines:
- Prefer spatial consistency (relative positions, clustering, centroid locations, bbox extent).
- If multiple are plausible, pick the most reasonable by area/extent for that name.
- If not reasonably confident, set boundary_id to null.
Return ONLY a JSON object in this exact format:
{"mapping":[{"name":"...","boundary_id":"R12345" | null}, ...]}`
	user := "City and data:\n" + string(js)

	var out string
	var err error
	switch strings.ToLower(cfg.LLM) {
	case "gemini":
		out, err = callGemini(os.Getenv("GOOGLE_API_KEY"), system, user)
	default:
		out, err = callOpenAI(os.Getenv("OPENAI_API_KEY"), system, user)
	}
	if err != nil || strings.TrimSpace(out) == "" {
		return nil
	}

	var parsed struct {
		Mapping []struct {
			Name       string  `json:"name"`
			BoundaryID *string `json:"boundary_id"`
		} `json:"mapping"`
	}
	clean := strings.TrimSpace(out)
	if json.Unmarshal([]byte(clean), &parsed) != nil {
		if !strings.HasPrefix(clean, "{") {
			if i := strings.Index(clean, "{"); i >= 0 {
				clean = clean[i:]
			}
		}
		if !strings.HasSuffix(clean, "}") {
			if j := strings.LastIndex(clean, "}"); j >= 0 {
				clean = clean[:j+1]
			}
		}
		_ = json.Unmarshal([]byte(clean), &parsed)
	}
	if len(parsed.Mapping) == 0 {
		return nil
	}
	result := make(map[string]string)
	for _, m := range parsed.Mapping {
		if m.BoundaryID == nil || strings.TrimSpace(*m.BoundaryID) == "" || strings.EqualFold(*m.BoundaryID, "null") {
			continue
		}
		result[strings.ToLower(strings.TrimSpace(m.Name))] = strings.TrimSpace(*m.BoundaryID)
	}
	return result
}

func fetchOSMBoundariesByIDs(ids []string) map[string]json.RawMessage {
	if len(ids) == 0 {
		return nil
	}
	ua := strings.TrimSpace(os.Getenv("NOMINATIM_USER_AGENT"))
	if ua == "" {
		contact := strings.TrimSpace(os.Getenv("CONTACT_EMAIL"))
		if contact == "" {
			contact = "contact@example.com"
		}
		ua = "jaunt-neighborhood-finder/1.0 (" + contact + ")"
	}
	client := &http.Client{Timeout: httpTimeout}

	out := make(map[string]json.RawMessage)
	// Nominatim supports up to a reasonable number of ids per request; batch to be safe.
	const batch = 25
	for i := 0; i < len(ids); i += batch {
		j := i + batch
		if j > len(ids) {
			j = len(ids)
		}
		values := url.Values{}
		values.Set("format", "geojson")
		values.Set("polygon_geojson", "1")
		values.Set("osm_ids", strings.Join(ids[i:j], ","))
		req, err := http.NewRequest(http.MethodGet, "https://nominatim.openstreetmap.org/lookup?"+values.Encode(), nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", ua)
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		bs, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 || len(bs) == 0 {
			continue
		}
		var fc struct {
			Type     string `json:"type"`
			Features []struct {
				Properties struct {
					OSMID   int64  `json:"osm_id"`
					OSMType string `json:"osm_type"` // "relation"|"way"|"node"
				} `json:"properties"`
				Geometry json.RawMessage `json:"geometry"`
			} `json:"features"`
		}
		if json.Unmarshal(bs, &fc) != nil || strings.ToLower(fc.Type) != "featurecollection" {
			continue
		}
		for _, f := range fc.Features {
			prefix := ""
			switch strings.ToLower(f.Properties.OSMType) {
			case "relation":
				prefix = "R"
			case "way":
				prefix = "W"
			case "node":
				prefix = "N"
			}
			key := fmt.Sprintf("%s%d", prefix, f.Properties.OSMID)
			// Normalize to MultiPolygon if possible
			if mp, norm := parseBoundaryRaw(f.Geometry); mp != nil && len(norm) > 0 {
				out[key] = norm
			}
		}
		// be polite
		time.Sleep(500 * time.Millisecond)
	}
	return out
}

func fillBoundariesByLLMMappedOSM(cfg *FinderConfig, city string, items []FinderNeighborhood) []FinderNeighborhood {
	// collect names needing boundary
	var names []string
	for _, n := range items {
		if len(strings.TrimSpace(string(n.Boundary))) == 0 || string(n.Boundary) == "null" {
			if s := strings.TrimSpace(n.Name); s != "" {
				names = append(names, s)
			}
		}
	}
	if len(names) == 0 {
		return items
	}
	cands, err := fetchOSMAnonymousBoundariesForCity(city)
	if err != nil || len(cands) == 0 {
		return items
	}
	mapping := mapNamesToOSMByLLM(cfg, city, names, cands)
	if len(mapping) == 0 {
		return items
	}
	// unique ids
	uniq := make(map[string]struct{})
	var ids []string
	for _, id := range mapping {
		if _, ok := uniq[id]; !ok && strings.TrimSpace(id) != "" {
			uniq[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	id2geom := fetchOSMBoundariesByIDs(ids)
	if len(id2geom) == 0 {
		return items
	}

	// apply by name
	for i := range items {
		key := strings.ToLower(strings.TrimSpace(items[i].Name))
		if id, ok := mapping[key]; ok {
			if geom, ok2 := id2geom[id]; ok2 && len(geom) > 0 {
				items[i].Boundary = geom
			}
		}
	}
	return items
}

// ----------- Main runner (env-driven) -----------

func RunFinder(cfg *FinderConfig) (*FinderNeighborhoodsOut, error) {
	if cfg == nil {
		return nil, errors.New("finder config is nil")
	}
	if cfg.City == "" {
		return nil, errors.New("finder: City is required")
	}

	tavilyKey := os.Getenv("TAVILY_API_KEY")
	openAIKey := os.Getenv("OPENAI_API_KEY")
	geminiKey := os.Getenv("GOOGLE_API_KEY")

	if strings.TrimSpace(cfg.Query) == "" {
		// Ask specifically for official wards and their polygon boundaries.
		cfg.Query = fmt.Sprintf("official high-level administrative wards of %s with polygon boundaries (GeoJSON MultiPolygon or shapefile) from authoritative sources", cfg.City)
	}

	// sensible defaults for new fields
	if cfg.MaxExtractChars == 0 {
		cfg.MaxExtractChars = 8000
	}
	if cfg.ParallelExtractors <= 0 {
		cfg.ParallelExtractors = 4
	}

	var contextBundle string
	if !cfg.SkipTavily {
		if tavilyKey == "" {
			return nil, errors.New("finder: missing TAVILY_API_KEY or set FINDER_SKIP_TAVILY=true")
		}

		// Strengthen the search query to bias toward boundary resources.
		searchQuery := cfg.Query + " boundary geojson multipolygon shapefile site:openstreetmap.org OR site:wikidata.org OR site:*.gov"

		sr, err := tavilySearch(tavilyKey, searchQuery, cfg.MaxResults, cfg.IncludeDomains, cfg.ExcludeDomains)
		if err != nil {
			return nil, fmt.Errorf("tavily search error: %w (use FINDER_SKIP_TAVILY=true to skip)", err)
		}

		// Filter and de-duplicate results
		filtered := filterTavilyResults(sr.Results, cfg)

		var bundles []string
		if cfg.ExtractContent && len(filtered) > 0 {
			// Chunk URLs and batch-extract to minimize calls
			const chunkSize = 8
			urls := make([]string, 0, len(filtered))
			titles := make(map[string]string, len(filtered))
			fallback := make(map[string]string, len(filtered))
			for _, r := range filtered {
				u := normalizeURL(r.URL)
				urls = append(urls, u)
				titles[u] = r.Title
				fallback[u] = r.Content
			}

			type chunk struct{ from, to int }
			var chunks []chunk
			for i := 0; i < len(urls); i += chunkSize {
				j := i + chunkSize
				if j > len(urls) {
					j = len(urls)
				}
				chunks = append(chunks, chunk{from: i, to: j})
			}

			// Parallelize over chunks
			type chunkResult struct {
				m map[string]string
			}
			resultsCh := make(chan chunkResult, len(chunks))
			sem := make(chan struct{}, cfg.ParallelExtractors)
			for _, c := range chunks {
				sem <- struct{}{}
				c := c
				go func() {
					defer func() { <-sem }()
					m := make(map[string]string)
					out, err := tavilyExtractBatch(tavilyKey, urls[c.from:c.to])
					if err == nil && out != nil {
						for _, rr := range out.Results {
							content := pickExtractedContent(&tavilyExtractResponse{Results: []struct {
								URL        string `json:"url"`
								Title      string `json:"title"`
								Content    string `json:"content,omitempty"`
								RawContent string `json:"raw_content,omitempty"`
								Text       string `json:"text,omitempty"`
								HTML       string `json:"html,omitempty"`
							}{rr}})
							// sanitize and truncate
							cc := truncate(cleanText(content), cfg.MaxExtractChars)
							m[normalizeURL(rr.URL)] = cc
						}
					}
					resultsCh <- chunkResult{m: m}
				}()
			}
			// wait for all
			for i := 0; i < len(chunks); i++ {
				cr := <-resultsCh
				for k, v := range cr.m {
					// merge
					fallback[k] = v
				}
			}

			// Build bundles using extracted content (or fallback snippet)
			for _, u := range urls {
				content := fallback[u]
				if strings.TrimSpace(content) == "" {
					content = "(no extract; using snippet) " + truncate(cleanText(content), 512)
				}
				bundles = append(bundles, fmt.Sprintf("SOURCE: %s\nTITLE: %s\nCONTENT:\n%s\n", u, titles[u], content))
			}
		} else {
			// No extraction; use Tavily snippets
			for _, r := range filtered {
				content := truncate(cleanText(r.Content), cfg.MaxExtractChars)
				bundles = append(bundles, fmt.Sprintf("SOURCE: %s\nTITLE: %s\nCONTENT:\n%s\n", r.URL, r.Title, content))
			}
		}

		contextBundle = strings.Join(bundles, "\n\n---\n\n")
	}

	var systemPrompt, userPrompt string

	if cfg.SkipTavily {
		systemPrompt = `You are a geographic data expert.
Return ONLY valid JSON in the exact format:
{"neighborhoods": [{"name": "...", "lat": 0.0, "lng": 0.0, "boundary": { "type": "MultiPolygon", "coordinates": [...] }}, ...]}.
- The "boundary" must be a GeoJSON MultiPolygon in WGS84 (EPSG:4326). If the boundary is unavailable, set "boundary" to null or omit it.
- "lat" and "lng" are the center/label point of the neighborhood (centroid).
- Use authoritative sources. Return nothing except the JSON object.`
		userPrompt = fmt.Sprintf(`City: %s
List all official high-level neighborhoods/wards for %s.
For each neighborhood, provide:
- name
- lat, lng (centroid in WGS84)
- boundary as a GeoJSON MultiPolygon if available; otherwise null/omit.

Return ONLY valid JSON:
{"neighborhoods": [{"name": "Neighborhood1", "lat": 51.1234, "lng": -0.5678, "boundary": {"type":"MultiPolygon","coordinates":[...]}}, ...]}`, cfg.City, cfg.City)
	} else {
		systemPrompt = `You are a geographic data normalizer.
Return ONLY JSON in this exact format:
{"neighborhoods": [{"name": "...", "lat": 0.0, "lng": 0.0, "boundary": { "type": "MultiPolygon", "coordinates": [...] }}, ...]}.
- Extract official high-level neighborhoods/wards for the target city from the provided sources.
- "lat" and "lng" must be the centroid (WGS84).
- "boundary" must be a GeoJSON MultiPolygon (WGS84). If unavailable, set it to null or omit it.
- Prefer authoritative/official sources.`
		userPrompt = fmt.Sprintf(`City: %s
Primary intent: Identify official neighborhoods/wards and their polygon boundaries.

Query (focus on boundary data): %s

Sources:
%s

Task:
- List neighborhoods with accurate centroid lat/lng.
- Include boundary as GeoJSON MultiPolygon when available (WGS84). If only a single polygon exists, still return it as a MultiPolygon (wrap as [[[...]]]).
- If boundary isn't available, set it to null or omit it.

Return ONLY valid JSON:
{"neighborhoods": [{"name": "Neighborhood1", "lat": 51.1234, "lng": -0.5678, "boundary": {"type":"MultiPolygon","coordinates":[...]}}, ...]}`, cfg.City, cfg.Query, contextBundle)
	}

	// New: optionally fetch OSM neighborhoods and append to Sources.
	// var osmList []FinderNeighborhood
	if cfg.UseOSMContext {
		if list, bundle := fetchOSMNeighborhoods(cfg.City); len(list) > 0 || bundle != "" {
			// osmList = list
			if strings.TrimSpace(contextBundle) == "" {
				contextBundle = bundle
			} else {
				contextBundle = contextBundle + "\n\n" + bundle
			}
			// If Tavily was skipped but we have OSM context, we need to adjust the prompt.
			if cfg.SkipTavily && strings.TrimSpace(contextBundle) != "" {
				systemPrompt = `You are a geographic data normalizer.
	Return ONLY JSON in this exact format:
	{"neighborhoods": [{"name": "...", "lat": 0.0, "lng": 0.0, "boundary": { "type": "MultiPolygon", "coordinates": [...] }}, ...]}.
	- Extract official high-level neighborhoods/wards for the target city from the provided sources.
	- "lat" and "lng" must be the centroid (WGS84).
		- "boundary" must be a GeoJSON MultiPolygon (WGS84). If unavailable, set it to null or omit it.
		- Prefer authoritative/official sources.`
				userPrompt = fmt.Sprintf(`City: %s
		Primary intent: Identify official neighborhoods/wards and their polygon boundaries.
		
		Sources:
		%s
		
		Task:
		- List neighborhoods with accurate centroid lat/lng.
		- Include boundary as GeoJSON MultiPolygon when available (WGS84). If only a single polygon exists, still return it as a MultiPolygon (wrap as [[[...]]]).
		- If boundary isn't available, set it to null or omit it.
		
		Return ONLY valid JSON:
		{"neighborhoods": [{"name": "Neighborhood1", "lat": 51.1234, "lng": -0.5678, "boundary": {"type":"MultiPolygon","coordinates":[...]}}, ...]}`, cfg.City, contextBundle)
			} else if !cfg.SkipTavily {
				// Update user prompt with combined context
				userPrompt = fmt.Sprintf(`City: %s
	Primary intent: Identify official neighborhoods/wards and their polygon boundaries.
	
	Query (focus on boundary data): %s
	
	Sources:
	%s
	
	Task:
	- List neighborhoods with accurate centroid lat/lng.
	- Include boundary as GeoJSON MultiPolygon when available (WGS84). If only a single polygon exists, still return it as a MultiPolygon (wrap as [[[...]]]).
	- If boundary isn't available, set it to null or omit it.
	
	Return ONLY valid JSON:
	{"neighborhoods": [{"name": "Neighborhood1", "lat": 51.1234, "lng": -0.5678, "boundary": {"type":"MultiPolygon","coordinates":[...]}}, ...]}`, cfg.City, cfg.Query, contextBundle)
			}
		}
	}

	var llmOut string
	var err error
	switch strings.ToLower(cfg.LLM) {
	case "gemini":
		if geminiKey == "" {
			return nil, errors.New("finder: missing GOOGLE_API_KEY for Gemini")
		}
		llmOut, err = callGemini(geminiKey, systemPrompt, userPrompt)
	default:
		if openAIKey == "" {
			return nil, errors.New("finder: missing OPENAI_API_KEY")
		}
		llmOut, err = callOpenAI(openAIKey, systemPrompt, userPrompt)
	}
	if err != nil {
		return nil, fmt.Errorf("llm error: %w", err)
	}

	var parsed FinderNeighborhoodsOut
	if err := json.Unmarshal([]byte(llmOut), &parsed); err != nil {
		clean := strings.TrimSpace(llmOut)
		if !strings.HasPrefix(clean, "{") {
			if i := strings.Index(clean, "{"); i >= 0 {
				clean = clean[i:]
			}
		}
		if !strings.HasSuffix(clean, "}") {
			if j := strings.LastIndex(clean, "}"); j >= 0 {
				clean = clean[:j+1]
			}
		}
		if err := json.Unmarshal([]byte(clean), &parsed); err != nil {
			return nil, fmt.Errorf("failed to parse final JSON: %w; output=%s", err, llmOut)
		}
	}
	// New: anonymous OSM boundary mapping via LLM before other fill passes.
	parsed.Neighborhoods = fillBoundariesByLLMMappedOSM(cfg, cfg.City, parsed.Neighborhoods)

	// After initial parse, run a post-pass to fill missing boundaries using a boundary-only prompt.
	parsed.Neighborhoods = fillMissingBoundariesWithLLM(cfg, cfg.City, parsed.Neighborhoods)
	// // Fallback: fill any remaining missing boundaries from OpenStreetMap Nominatim.
	parsed.Neighborhoods = fillMissingBoundariesWithOSM(cfg, cfg.City, parsed.Neighborhoods)

	if cfg.OutputFile != "" {
		// Persist enriched neighborhoods (including optional boundaries).
		if err := os.MkdirAll(filepath.Dir(cfg.OutputFile), 0755); err == nil || filepath.Dir(cfg.OutputFile) == "." {
			b, _ := json.MarshalIndent(parsed, "", "  ")
			_ = os.WriteFile(cfg.OutputFile, b, 0644)
		}
	}

	return &parsed, nil
}
