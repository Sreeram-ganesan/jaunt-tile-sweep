# Advanced Finder (`finder_adv.go`) - Enhanced Neighborhood Discovery

## Overview

`finder_adv.go` is an improved version of the original `finder.go` that addresses the limitations identified in the neighborhood discovery process. This enhanced version provides better integration of OSM data, improved search strategies, enhanced prompting techniques, and more intelligent data fusion.

## Key Improvements Over Original Implementation

### 1. **Enhanced OSM Integration Strategy**

**Problem with Original**: OSM data was used as secondary context and fallback, leading to missed boundary data and inconsistent results.

**Advanced Solution**:
- **OSM-First Approach**: Prioritize OSM for boundary geometry data since it's more reliable and comprehensive
- **Enhanced Overpass Queries**: Query multiple admin levels (8, 9, 10, 11) and place types simultaneously
- **Geometry Extraction**: Direct conversion of OSM geometry to GeoJSON MultiPolygon format
- **Metadata Enrichment**: Capture admin levels, place types, area calculations, and bounding boxes

```go
// Enhanced OSM query includes geometry data directly
query := `
[out:json][timeout:30];
(area["name"~"^%s$",i]["admin_level"~"^[2-8]$"]->.a;);
(
  relation["boundary"="administrative"]["admin_level"~"^(%s)$"]["name"](area.a);
  relation["place"~"^(%s)$"]["name"](area.a);
  way["boundary"="administrative"]["admin_level"~"^(%s)$"]["name"](area.a);
  way["place"~"^(%s)$"]["name"](area.a);
);
out geom tags center bbox;
`
```

### 2. **Multi-Phase Search Strategy**

**Problem with Original**: Single search approach often missed official sources or got diluted with low-quality results.

**Advanced Solution**:
- **Phase 1**: Official Sources (government, municipal websites)
- **Phase 2**: Geographic Databases (OSM, Wikipedia, GeoNames)
- **Phase 3**: General Search (fallback with user query)

Each phase has targeted queries, domain filters, and confidence thresholds:

```go
phases := []SearchPhase{
    {
        Name:        "Official Sources",
        Query:       fmt.Sprintf("official administrative divisions %s neighborhoods wards districts site:(gov OR municipal OR city OR council)", cfg.City),
        Domains:     cfg.OfficialDomains,
        MaxResults:  10,
        MinScore:    0.7,
    },
    // ... additional phases
}
```

### 3. **Enhanced LLM Prompting**

**Problem with Original**: Generic prompts led to inconsistent JSON output and missed contextual nuances.

**Advanced Solution**:
- **Structured System Prompts**: Clear instructions with confidence scoring guidelines
- **Enhanced Context Building**: Organized OSM and web data presentation
- **Confidence-Based Scoring**: Source-quality-based confidence assignments
- **Better Response Parsing**: Robust JSON extraction and validation

```go
systemPrompt := `You are an expert geographic data analyst specializing in administrative boundaries.

CONFIDENCE SCORING:
- 0.9-1.0: Official government sources, OSM with boundaries
- 0.7-0.9: Wikipedia, established geographic databases  
- 0.5-0.7: News articles, local websites with geographic references
- 0.3-0.5: General mentions, unclear sources
- Below 0.3: Exclude from results`
```

### 4. **Intelligent Data Fusion**

**Problem with Original**: Simple name-based merging often missed aliases and didn't leverage strengths of each data source.

**Advanced Solution**:
- **OSM-First Boundaries**: Use OSM boundary data as primary source
- **LLM Name Validation**: Use LLM to validate and enhance OSM name data
- **Hybrid Confidence**: Boost confidence when multiple sources agree
- **Intelligent Matching**: Fuzzy name matching with normalized comparison

```go
func fuseDataIntelligently(cfg *FinderAdvConfig, osm []OSMNeighborhoodAdvanced, llm []NeighborhoodAdvanced) []NeighborhoodAdvanced {
    // Phase 1: Start with OSM data (high confidence boundaries)
    // Phase 2: Add LLM data, merging with OSM where names match  
    // Phase 3: Quality filtering and validation
}
```

### 5. **Quality Assessment and Validation**

**Problem with Original**: No systematic quality assessment or validation of results.

**Advanced Solution**:
- **Geometry Validation**: Verify GeoJSON boundary correctness
- **Coordinate Validation**: Sanity check lat/lng ranges
- **Confidence Scoring**: Multi-factor confidence calculation
- **Quality Metrics**: Overall quality score combining coverage and confidence

### 6. **Performance and Reliability Improvements**

**Problem with Original**: Sequential processing and limited error handling.

**Advanced Solution**:
- **Parallel Processing**: Concurrent content extraction with semaphores
- **Enhanced Timeouts**: Configurable timeouts for long-running operations
- **Better Error Handling**: Graceful degradation and detailed logging
- **Caching Support**: Framework for caching expensive operations

## Usage Example

```go
cfg := &FinderAdvConfig{
    City:                "London",
    LLM:                 "openai", // or "gemini"
    MaxResults:          20,
    EnableMultiPhase:    true,
    PreferOSMBoundaries: true,
    MinConfidenceScore:  0.4,
    EnableValidation:    true,
    OutputFile:          "london_neighborhoods.json",
}

result, err := RunAdvancedFinder(cfg)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Found %d neighborhoods with quality score: %.2f\n", 
    result.Metadata.TotalFound, result.Metadata.QualityScore)
```

## Output Structure

The advanced finder provides richer output metadata:

```json
{
  "neighborhoods": [
    {
      "name": "Westminster",
      "lat": 51.4975,
      "lng": -0.1357,
      "boundary": {"type": "MultiPolygon", "coordinates": [...]},
      "source": "hybrid",
      "confidence": 0.95,
      "aliases": ["City of Westminster"],
      "admin_level": "8",
      "area_km2": 21.31,
      "osm_id": "R65606",
      "has_valid_boundary": true,
      "geometry_valid": true
    }
  ],
  "metadata": {
    "city": "London",
    "total_found": 32,
    "osm_count": 18,
    "llm_count": 6,
    "hybrid_count": 8,
    "processing_time_ms": 15420,
    "quality_score": 0.87
  }
}
```

## Configuration Options

### Enhanced OSM Configuration
- `PreferOSMBoundaries`: Prioritize OSM boundary data
- `OSMAdminLevels`: Target specific administrative levels
- `OSMPlaceTypes`: Target specific place classifications
- `MinBoundaryArea`: Filter out tiny polygons

### Multi-Phase Search
- `EnableMultiPhase`: Enable/disable phase-based searching
- `OfficialDomains`: Domains considered authoritative
- `MinConfidenceScore`: Quality threshold for inclusion

### Performance & Reliability  
- `MaxConcurrency`: Control parallel processing
- `Timeout`: Configurable operation timeouts
- `EnableCaching`: Cache expensive operations
- `EnableValidation`: Enable geometry validation

## When to Use Advanced Finder

Use `RunAdvancedFinder` when:

1. **Boundary Quality Matters**: You need high-quality polygon boundaries
2. **Official Data Priority**: You want to prioritize government/official sources
3. **Data Validation**: You need confidence scores and quality metrics
4. **Performance Requirements**: You need parallel processing and caching
5. **Multiple Cities**: You're processing multiple cities and want consistent results

## Approach Explanation

The advanced approach takes a **data-source-strength-aware** strategy:

1. **OSM for Geometry**: OpenStreetMap excels at boundary geometry and administrative structure
2. **Tavily+LLM for Names**: Web search + AI excels at finding official names and aliases
3. **Hybrid Validation**: Cross-validation between sources increases confidence
4. **Quality-First**: Focus on data quality over quantity

This approach addresses the core issue that "results from original approach are not great" by:

- **Higher Precision**: Multi-phase search reduces noise
- **Better Boundaries**: OSM-first approach provides more accurate polygons  
- **Quality Metrics**: Transparent confidence and quality scoring
- **Source Accountability**: Clear attribution of data sources
- **Validation**: Systematic verification of results

The result is a more robust, reliable, and higher-quality neighborhood discovery system.