"""
Football Data ETL Pipeline Orchestrator

Single command to run entire data cleaning pipeline:
    python scripts/run_pipeline.py

This script:
1. Cleans league match data (5 European leagues)
2. Cleans World Cup match data
3. Cleans World Cup standings
4. Merges and cleans World Cup player data
5. Exports all to data/clean/

Output files:
- clean_league_matches.csv
- clean_wc_matches.csv
- clean_wc_standings.csv
- clean_wc_players.csv
"""

import sys
import os
from datetime import datetime

# Get project root directory and change to it
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)

# Add project root to path
sys.path.insert(0, PROJECT_ROOT)

from scripts.etl.clean_leagues import clean_leagues
from scripts.etl.clean_wc_matches import clean_wc_matches
from scripts.etl.clean_wc_standings import clean_wc_standings
from scripts.etl.clean_wc_players import clean_wc_players
from scripts.etl.create_reference_tables import create_reference_tables
from scripts.etl.normalize_data import normalize_data
from scripts.etl.split_matches import split_matches
from scripts.etl.derive_metrics import derive_all_metrics
from scripts.etl.validate_data import validate_all


def print_banner(text):
    """Print formatted banner"""
    print("\n" + "="*80)
    print(text.center(80))
    print("="*80 + "\n")


def run_pipeline():
    """
    Execute complete ETL pipeline
    """
    start_time = datetime.now()
    
    print_banner("FOOTBALL DATA ETL PIPELINE")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = {
        'league_matches': None,
        'wc_matches': None,
        'wc_standings': None,
        'wc_players': None,
        'ref_tables': None,
        'normalization': None,
        'split_matches': None,
        'derived_metrics': None,
        'validation': None
    }
    
    try:
        # Step 1: Clean league data
        print_banner("STEP 1/9: CLEANING LEAGUE DATA")
        results['league_matches'] = clean_leagues()
        
        # Step 2: Clean World Cup matches
        print_banner("STEP 2/9: CLEANING WORLD CUP MATCHES")
        results['wc_matches'] = clean_wc_matches()
        
        # Step 3: Clean World Cup standings
        print_banner("STEP 3/9: CLEANING WORLD CUP STANDINGS")
        results['wc_standings'] = clean_wc_standings()
        
        # Step 4: Merge World Cup player data
        print_banner("STEP 4/9: MERGING WORLD CUP PLAYER DATA")
        results['wc_players'] = clean_wc_players()
        
        # Step 5: Create reference tables (teams & players)
        print_banner("STEP 5/9: CREATING REFERENCE TABLES")
        results['ref_tables'] = create_reference_tables()
        
        # Step 6: Normalize data with ID-based references
        print_banner("STEP 6/9: NORMALIZING DATA WITH IDs")
        results['normalization'] = normalize_data()
        
        # Step 7: Split matches into base and advanced
        print_banner("STEP 7/9: SPLITTING MATCHES INTO BASE & ADVANCED")
        results['split_matches'] = split_matches()
        
        # Step 8: Derive performance metrics
        print_banner("STEP 8/9: DERIVING PERFORMANCE METRICS")
        results['derived_metrics'] = derive_all_metrics()
        
        # Step 9: Validate all output data
        print_banner("STEP 9/9: VALIDATING OUTPUT DATA")
        results['validation'] = validate_all()
        
        # Summary
        print_banner("PIPELINE SUMMARY")
        
        success_count = sum(1 for v in results.values() if v is not None)
        total_count = len(results)
        
        print(f"✅ League Matches:     {'SUCCESS' if results['league_matches'] is not None else 'FAILED'}")
        if results['league_matches'] is not None:
            print(f"   Rows: {len(results['league_matches']):,}")
            
        print(f"✅ WC Matches:         {'SUCCESS' if results['wc_matches'] is not None else 'FAILED'}")
        if results['wc_matches'] is not None:
            print(f"   Rows: {len(results['wc_matches']):,}")
            
        print(f"✅ WC Standings:       {'SUCCESS' if results['wc_standings'] is not None else 'FAILED'}")
        if results['wc_standings'] is not None:
            print(f"   Rows: {len(results['wc_standings']):,}")
            
        print(f"✅ WC Players:         {'SUCCESS' if results['wc_players'] is not None else 'FAILED'}")
        if results['wc_players'] is not None:
            print(f"   Rows: {len(results['wc_players']):,}")
        
        print(f"✅ Reference Tables:   {'SUCCESS' if results['ref_tables'] is not None else 'FAILED'}")
        print(f"✅ Normalization:      {'SUCCESS' if results['normalization'] is not None else 'FAILED'}")
        print(f"✅ Split Matches:      {'SUCCESS' if results['split_matches'] is not None else 'FAILED'}")
        print(f"✅ Derived Metrics:    {'SUCCESS' if results['derived_metrics'] is not None else 'FAILED'}")
        print(f"✅ Data Validation:    {'SUCCESS (all checks passed)' if results['validation'] is not None else 'FAILED'}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n{'='*80}")
        print(f"Pipeline completed: {success_count}/{total_count} steps successful")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        if success_count == total_count:
            print("\n🎉 PIPELINE COMPLETE - DATABASE-READY OUTPUT GENERATED!")
            print("\n📦 Final Database Tables:")
            print("   • ref_teams.csv                      - Teams reference (dimension table)")
            print("   • ref_players.csv                    - Players reference (dimension table)")
            print("   • db_matches_base_enhanced.csv       - Core match data (all competitions)")
            print("   • db_match_stats_advanced_enhanced.csv - Advanced stats (World Cup)")
            print("   • db_players_stats_enhanced.csv      - Player statistics with metrics")
            print("   • db_standings_enhanced.csv          - Group standings with metrics")
            print("\n📁 All files saved to: data/clean/")
            print("\n✨ Features:")
            print("   ✓ ID-based normalization (no string joins)")
            print("   ✓ Separated base and advanced stats (no NULL hell)")
            print("   ✓ Derived performance metrics (SofaScore-style)")
            print("   ✓ Production-ready schema")
            print("   ✓ Data quality validated (all checks passed)")
            return 0
        else:
            print("\n⚠️  Some steps failed. Check logs above.")
            return 1
            
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit_code = run_pipeline()
    sys.exit(exit_code)
