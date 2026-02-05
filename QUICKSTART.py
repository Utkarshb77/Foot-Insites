"""
Quick Start Guide for Football Data ETL Pipeline
"""

# ============================================================================
# STEP 1: Install Dependencies
# ============================================================================

# Run this in your terminal:
# pip install -r requirements.txt


# ============================================================================
# STEP 2: Run the Complete Pipeline
# ============================================================================

# OPTION 1 - From project root directory:
# python scripts/run_pipeline.py

# OPTION 2 - From scripts directory:
# cd scripts
# python run_pipeline.py 

# This single command will:
# - Clean all 5 European league datasets
# - Clean World Cup match data
# - Clean World Cup standings
# - Merge and clean World Cup player statistics
# - Export everything to data/clean/


# ============================================================================
# STEP 3: Verify Output Files
# ============================================================================

# Check the data/clean/ directory for these files:
# - clean_league_matches.csv    (all league matches unified)
# - clean_wc_matches.csv         (World Cup matches with advanced stats)
# - clean_wc_standings.csv       (World Cup group standings)
# - clean_wc_players.csv         (merged player statistics)


# ============================================================================
# OPTIONAL: Run Individual Cleaning Scripts
# ============================================================================

# If you only need specific datasets, run individually:

# Clean leagues only:
# python -m scripts.etl.clean_leagues

# Clean World Cup matches only:
# python -m scripts.etl.clean_wc_matches

# Clean World Cup standings only:
# python -m scripts.etl.clean_wc_standings

# Clean World Cup players only:
# python -m scripts.etl.clean_wc_players


# ============================================================================
# SCHEMA REFERENCE
# ============================================================================

"""
clean_league_matches.csv columns:
    date, time, competition_name, season, home_team, away_team,
    home_goals, away_goals, home_shots, away_shots, home_sot, away_sot,
    home_fouls, away_fouls, home_corners, away_corners,
    home_yellow, away_yellow, home_red, away_red, referee

clean_wc_matches.csv columns:
    date, time, competition_name, season, home_team, away_team,
    home_goals, away_goals, home_xg, away_xg,
    home_possession, away_possession, home_shots, away_shots,
    home_sot, away_sot, home_passes_completed, home_passes_attempted,
    away_passes_completed, away_passes_attempted,
    home_tackles, away_tackles, home_interceptions, away_interceptions,
    home_clearances, away_clearances, home_saves, away_saves, venue

clean_wc_standings.csv columns:
    group, rank, team, played, wins, draws, losses,
    goals_for, goals_against, goal_difference, points

clean_wc_players.csv columns:
    player, team, position, age, minutes, games, goals, assists,
    shots, shots_on_target, passes_completed, passes,
    passes_pct, tackles, interceptions, clearances,
    touches, dispossessed, xg, xg_assist
"""
