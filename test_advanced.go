package main

import (
	"fmt"
	"log"
	"os"
)

// TestAdvancedFinder demonstrates the differences between original and advanced finder approaches
func TestAdvancedFinder() {
	// Check for required API keys
	if os.Getenv("OPENAI_API_KEY") == "" && os.Getenv("GOOGLE_API_KEY") == "" {
		log.Printf("⚠️ No API keys found. Set OPENAI_API_KEY or GOOGLE_API_KEY to test with real LLM calls")
		log.Printf("📝 Demonstrating configuration and architecture differences instead...")
		demonstrateConfigDifferences()
		return
	}

	if os.Getenv("TAVILY_API_KEY") == "" {
		log.Printf("⚠️ TAVILY_API_KEY not found. Running in OSM-only mode for demonstration")
	}

	// Run with a smaller city for testing
	testCity := "Cambridge"
	
	log.Printf("🔬 Testing Advanced Finder with city: %s", testCity)
	
	// Advanced finder configuration
	advCfg := &FinderAdvConfig{
		City:                testCity,
		LLM:                 "openai",
		MaxResults:          15,
		EnableMultiPhase:    true,
		PreferOSMBoundaries: true,
		MinConfidenceScore:  0.3,
		EnableValidation:    true,
		SkipTavily:          os.Getenv("TAVILY_API_KEY") == "", // Skip if no key
		
		// Enhanced settings
		OSMAdminLevels:      []string{"8", "9", "10"},
		MinBoundaryArea:     0.1, // Small minimum for testing
		MaxConcurrency:      2,   // Conservative for testing
	}

	// Show configuration differences
	fmt.Printf("\n📋 ADVANCED FINDER CONFIGURATION:\n")
	fmt.Printf("  City: %s\n", advCfg.City)
	fmt.Printf("  Multi-phase search: %t\n", advCfg.EnableMultiPhase)
	fmt.Printf("  Prefer OSM boundaries: %t\n", advCfg.PreferOSMBoundaries)
	fmt.Printf("  Min confidence: %.1f\n", advCfg.MinConfidenceScore)
	fmt.Printf("  Validation enabled: %t\n", advCfg.EnableValidation)
	fmt.Printf("  OSM admin levels: %v\n", advCfg.OSMAdminLevels)
	fmt.Printf("  Min boundary area: %.1f km²\n", advCfg.MinBoundaryArea)

	// Test just the OSM enhanced data collection to show improvements
	log.Printf("\n🌍 Testing enhanced OSM data collection...")
	osmData, err := fetchEnhancedOSMData(advCfg)
	if err != nil {
		log.Printf("❌ OSM data collection failed: %v", err)
		return
	}
	
	log.Printf("✅ Enhanced OSM collection successful!")
	log.Printf("📊 Found %d administrative areas with enhanced metadata", len(osmData))
	
	// Show sample of enhanced OSM data
	for i, osm := range osmData {
		if i >= 3 { // Show first 3 for demo
			break
		}
		fmt.Printf("  %d. %s (ID: %s, Type: %s", 
			i+1, osm.Name, osm.ID, osm.PlaceType)
		if osm.AdminLevel != "" {
			fmt.Printf(", Level: %s", osm.AdminLevel)
		}
		if osm.AreaKm2 > 0 {
			fmt.Printf(", Area: %.1f km²", osm.AreaKm2)
		}
		if len(osm.Boundary) > 0 {
			fmt.Printf(", ✅ Has Boundary")
		} else {
			fmt.Printf(", ❌ No Boundary")
		}
		fmt.Println(")")
	}
	
	if len(osmData) > 3 {
		fmt.Printf("  ... and %d more\n", len(osmData)-3)
	}

	log.Printf("\n🎯 Key Improvements Demonstrated:")
	log.Printf("  ✅ Enhanced OSM queries with geometry extraction")
	log.Printf("  ✅ Administrative level and place type classification")
	log.Printf("  ✅ Area calculations and bounding box data")
	log.Printf("  ✅ Direct boundary geometry extraction from OSM")
	log.Printf("  ✅ Structured metadata for quality assessment")
}

func demonstrateConfigDifferences() {
	fmt.Printf("\n📊 CONFIGURATION COMPARISON:\n")
	
	fmt.Printf("\n🔴 ORIGINAL FINDER LIMITATIONS:\n")
	fmt.Printf("  - Single-phase search strategy\n")
	fmt.Printf("  - OSM used as secondary/fallback data\n")
	fmt.Printf("  - Generic prompting without confidence scoring\n")
	fmt.Printf("  - Simple name-based data merging\n")
	fmt.Printf("  - No systematic validation or quality metrics\n")
	fmt.Printf("  - Sequential processing, limited error handling\n")
	
	fmt.Printf("\n🟢 ADVANCED FINDER IMPROVEMENTS:\n")
	fmt.Printf("  - Multi-phase search (Official → Geographic → General)\n")
	fmt.Printf("  - OSM-first approach for boundary geometry\n")
	fmt.Printf("  - Structured prompts with confidence scoring\n")
	fmt.Printf("  - Intelligent data fusion with fuzzy matching\n")
	fmt.Printf("  - Geometric validation and quality assessment\n")
	fmt.Printf("  - Parallel processing with graceful error handling\n")
	
	fmt.Printf("\n🎯 KEY ARCHITECTURAL CHANGES:\n")
	fmt.Printf("  1. OSM Integration: Geometry-first vs context-only\n")
	fmt.Printf("  2. Search Strategy: Multi-phase vs single query\n")
	fmt.Printf("  3. LLM Prompting: Structured confidence vs generic extraction\n")
	fmt.Printf("  4. Data Fusion: Intelligence vs simple merge\n")
	fmt.Printf("  5. Validation: Systematic vs none\n")
	fmt.Printf("  6. Performance: Parallel + caching vs sequential\n")
	
	fmt.Printf("\n📈 EXPECTED IMPROVEMENTS:\n")
	fmt.Printf("  - Higher boundary coverage (OSM geometry direct extraction)\n")
	fmt.Printf("  - Better name accuracy (official source prioritization)\n")
	fmt.Printf("  - Quality transparency (confidence scores + validation)\n")
	fmt.Printf("  - Performance gains (parallel processing)\n")
	fmt.Printf("  - Reliability improvements (better error handling)\n")
}

func init() {
	// Set log format for better demo output
	log.SetFlags(log.Ltime)
}