# RunFinder (`finder.go`) – Detailed Explanation

The `finder.go` file in the `@Sreeram-ganesan/jaunt-tile-sweep` repository implements a comprehensive pipeline to identify, enrich, and output neighborhood (or ward) boundaries for a given city. It leverages multiple external APIs and data sources, including Tavily, OpenAI/Gemini LLMs, and OpenStreetMap (OSM), to maximize the quality and completeness of the data.

---

## High-Level Overview

The script's main function is `RunFinder`, which, given a `FinderConfig`, orchestrates the following steps:

1. **Configuration and Defaults**: Sets up API keys and sensible defaults for search parameters.
2. **Tavily Search and Extraction**: Optionally queries Tavily for authoritative datasets, extracts content, and deduplicates/filter results.
3. **Context Building**: Assembles relevant context data for LLM prompting (including Tavily, OSM, or both).
4. **LLM Call**: Prompts OpenAI or Gemini to infer a list of neighborhoods (with centroid and polygon boundary if possible).
5. **Boundary Enrichment**: 
    - Fills missing boundaries via LLM with a boundary-only prompt.
    - Optionally matches to anonymous OSM boundaries using LLM mapping.
    - Finally, attempts to fill any remaining boundaries using OSM/Nominatim.
6. **Result Output**: Returns or saves a JSON object of enriched neighborhoods.

---

## Detailed Steps and Components

### 1. **Configuration Structures**

- `FinderConfig`: Main config struct; controls city, query, LLM, result limits, extraction flags, domain filters, and output file location.
- `FinderNeighborhood` and `FinderNeighborhoodsOut`: Output data model (name, lat/lng, and boundary as raw GeoJSON).

### 2. **Tavily Integration**

- **Search**: Uses Tavily's search API (with optional depth, domain filters, and max results) to find authoritative sources for neighborhood boundaries.
- **Content Extraction**: Batches URL extraction to fetch page content, attempting to get clean, relevant text for LLM context.

### 3. **LLM Calls**

- **System and User Prompts**: Carefully crafted to instruct the LLM to only return valid JSON in the required format, prioritizing official sources and GeoJSON MultiPolygon boundaries.
- **Models Supported**: OpenAI (GPT-4o-mini) and Gemini (1.5 Flash), selectable via config.

### 4. **OSM Data Integration**

- **fetchOSMNeighborhoods**: Uses Overpass API to obtain neighborhood/ward relations for the city, providing names and centroids.
- **mergePreferOSM**: Merges LLM and OSM lists, preferring OSM data for centroid when available.

### 5. **Boundary Filling**

- **fillMissingBoundariesWithLLM**: Second LLM pass to try and fill missing boundaries for neighborhoods.
- **fillMissingBoundariesWithOSM**: Uses Nominatim to get boundaries for neighborhoods still missing them.
- **fillBoundariesByLLMMappedOSM**: Uses LLM to map neighborhood names to anonymous OSM relations, then fetches and assigns corresponding boundaries.

### 6. **Helpers & Utilities**

- **HTTP and JSON helpers**: Common utilities for HTTP POST/GET with retry/backoff and JSON (un)marshalling.
- **Text Cleanup**: Functions to clean and truncate content, extract domains, and normalize URLs.
- **GeoJSON Normalization**: (Not shown, but referenced) Ensures boundaries are always MultiPolygon.

---

## Example Output

The script produces a JSON file (optionally written to disk) containing:

```json
{
  "neighborhoods": [
    {
      "name": "Downtown",
      "lat": 40.123,
      "lng": -74.567,
      "boundary": { "type": "MultiPolygon", "coordinates": [[[ ... ]]] }
    },
    ...
  ]
}
```

---

## Key Design Features

- **Multi-source Data Fusion**: Combines Tavily, OSM, and LLMs to maximize coverage and quality.
- **Resilient Fallbacks**: Attempts multiple approaches to fill missing boundaries.
- **Polite API Usage**: Sets custom user agents and throttles requests to comply with OSM/Nominatim policies.
- **Idempotent and Robust**: Handles API failures gracefully, skips or fills where possible, and continues processing.

---

## When to Use

Use `RunFinder` to:

- Generate a comprehensive, boundary-enriched list of neighborhoods/wards for a city.
- Obtain centroid coordinates and boundaries, normalized in a standard JSON/GeoJSON format.
- Leverage AI to fill in missing geospatial data by synthesizing from multiple sources.

---
