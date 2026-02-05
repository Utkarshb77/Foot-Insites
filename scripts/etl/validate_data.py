"""
Data Validation Module
Performs sanity checks and data quality validation across all pipeline outputs.
Must pass ALL checks before pipeline is considered successful.
"""

import pandas as pd
import os
from datetime import datetime


class ValidationError(Exception):
    """Custom exception for validation failures"""
    pass


def print_validation_header():
    """Print validation section header"""
    print("\n" + "="*80)
    print("DATA VALIDATION CHECKS".center(80))
    print("="*80)


def validate_reference_tables(output_dir='data/clean'):
    """
    Validate dimension tables (ref_teams, ref_players)
    """
    print("\n🔍 Validating Reference Tables...")
    
    teams_file = os.path.join(output_dir, 'ref_teams.csv')
    players_file = os.path.join(output_dir, 'ref_players.csv')
    
    # Check files exist
    if not os.path.exists(teams_file):
        raise ValidationError(f"❌ Missing file: {teams_file}")
    if not os.path.exists(players_file):
        raise ValidationError(f"❌ Missing file: {players_file}")
    
    # Load data
    teams_df = pd.read_csv(teams_file)
    players_df = pd.read_csv(players_file)
    
    # TEAMS VALIDATION
    # Check required columns
    required_teams_cols = ['team_id', 'team_name', 'competition_type', 'primary_competition']
    missing_cols = set(required_teams_cols) - set(teams_df.columns)
    if missing_cols:
        raise ValidationError(f"❌ ref_teams missing columns: {missing_cols}")
    
    # Check for NULL team_id
    if teams_df['team_id'].isnull().any():
        raise ValidationError("❌ ref_teams contains NULL team_id")
    
    # Check for duplicate team_id
    if teams_df['team_id'].duplicated().any():
        duplicates = teams_df[teams_df['team_id'].duplicated()]['team_id'].tolist()
        raise ValidationError(f"❌ Duplicate team_ids found: {duplicates}")
    
    # Check expected row count (should be ~130)
    if len(teams_df) < 100 or len(teams_df) > 150:
        raise ValidationError(f"❌ Unexpected team count: {len(teams_df)} (expected 100-150)")
    
    # Check competition_type values
    valid_types = ['league', 'international']
    invalid_types = teams_df[~teams_df['competition_type'].isin(valid_types)]
    if len(invalid_types) > 0:
        raise ValidationError(f"❌ Invalid competition_type values found: {invalid_types['competition_type'].unique()}")
    
    print(f"   ✅ ref_teams: {len(teams_df)} teams validated")
    
    # PLAYERS VALIDATION
    # Check required columns
    required_players_cols = ['player_id', 'player_name', 'team_id']
    missing_cols = set(required_players_cols) - set(players_df.columns)
    if missing_cols:
        raise ValidationError(f"❌ ref_players missing columns: {missing_cols}")
    
    # Check for NULL player_id
    if players_df['player_id'].isnull().any():
        raise ValidationError("❌ ref_players contains NULL player_id")
    
    # Check for NULL team_id (FK violation)
    if players_df['team_id'].isnull().any():
        raise ValidationError("❌ ref_players contains NULL team_id (FK violation)")
    
    # Check for duplicate player_id
    if players_df['player_id'].duplicated().any():
        duplicates = players_df[players_df['player_id'].duplicated()]['player_id'].tolist()
        raise ValidationError(f"❌ Duplicate player_ids found: {duplicates}")
    
    # Validate FK: all team_ids exist in ref_teams
    invalid_team_ids = set(players_df['team_id']) - set(teams_df['team_id'])
    if invalid_team_ids:
        raise ValidationError(f"❌ Players reference non-existent team_ids: {invalid_team_ids}")
    
    # Check expected row count (should be ~680 for World Cup)
    if len(players_df) < 600 or len(players_df) > 800:
        raise ValidationError(f"❌ Unexpected player count: {len(players_df)} (expected 600-800)")
    
    # Validate age range if present
    if 'age' in players_df.columns:
        age_col = players_df['age'].dropna()
        if len(age_col) > 0:
            if age_col.min() < 15 or age_col.max() > 45:
                raise ValidationError(f"❌ Invalid age range: {age_col.min()}-{age_col.max()} (expected 15-45)")
    
    print(f"   ✅ ref_players: {len(players_df)} players validated")


def validate_matches_base(output_dir='data/clean'):
    """
    Validate core match data table
    """
    print("\n🔍 Validating Match Base Data...")
    
    matches_file = os.path.join(output_dir, 'db_matches_base_enhanced.csv')
    teams_file = os.path.join(output_dir, 'ref_teams.csv')
    
    if not os.path.exists(matches_file):
        raise ValidationError(f"❌ Missing file: {matches_file}")
    
    df = pd.read_csv(matches_file)
    teams_df = pd.read_csv(teams_file)
    valid_team_ids = set(teams_df['team_id'])
    
    # Check required columns
    required_cols = ['match_id', 'home_team_id', 'away_team_id', 'home_goals', 'away_goals', 'date']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValidationError(f"❌ db_matches_base missing columns: {missing_cols}")
    
    # Check for NULL match_id
    if df['match_id'].isnull().any():
        raise ValidationError("❌ Matches contain NULL match_id")
    
    # Check for duplicate match_id
    if df['match_id'].duplicated().any():
        duplicates = df[df['match_id'].duplicated()]['match_id'].tolist()
        raise ValidationError(f"❌ Duplicate match_ids found: {duplicates[:5]}...")
    
    # Check expected row count (~1890)
    if len(df) < 1800 or len(df) > 2000:
        raise ValidationError(f"❌ Unexpected match count: {len(df)} (expected 1800-2000)")
    
    # FOREIGN KEY VALIDATION
    # Check for NULL team_ids
    if df['home_team_id'].isnull().any():
        raise ValidationError("❌ Matches contain NULL home_team_id (FK violation)")
    if df['away_team_id'].isnull().any():
        raise ValidationError("❌ Matches contain NULL away_team_id (FK violation)")
    
    # Validate FK: all team_ids exist in ref_teams
    invalid_home = set(df['home_team_id']) - valid_team_ids
    if invalid_home:
        raise ValidationError(f"❌ Matches reference non-existent home_team_ids: {invalid_home}")
    
    invalid_away = set(df['away_team_id']) - valid_team_ids
    if invalid_away:
        raise ValidationError(f"❌ Matches reference non-existent away_team_ids: {invalid_away}")
    
    # BUSINESS LOGIC VALIDATION
    # No team plays itself
    self_matches = df[df['home_team_id'] == df['away_team_id']]
    if len(self_matches) > 0:
        raise ValidationError(f"❌ Found {len(self_matches)} self-matches (home_team_id == away_team_id)")
    
    # No negative goals
    if (df['home_goals'] < 0).any():
        raise ValidationError("❌ Negative home_goals found")
    if (df['away_goals'] < 0).any():
        raise ValidationError("❌ Negative away_goals found")
    
    # Shots validation
    if 'home_shots' in df.columns:
        if (df['home_shots'].dropna() < 0).any():
            raise ValidationError("❌ Negative home_shots found")
    
    if 'home_sot' in df.columns and 'home_shots' in df.columns:
        invalid = df[(df['home_sot'] > df['home_shots']) & df['home_sot'].notna() & df['home_shots'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ home_sot > home_shots in {len(invalid)} matches")
    
    if 'away_sot' in df.columns and 'away_shots' in df.columns:
        invalid = df[(df['away_sot'] > df['away_shots']) & df['away_sot'].notna() & df['away_shots'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ away_sot > away_shots in {len(invalid)} matches")
    
    # Shot accuracy validation
    if 'home_shot_accuracy' in df.columns:
        invalid = df[(df['home_shot_accuracy'] > 100) & df['home_shot_accuracy'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ home_shot_accuracy > 100% in {len(invalid)} matches")
    
    # Date validation
    try:
        pd.to_datetime(df['date'], errors='raise')
    except Exception as e:
        raise ValidationError(f"❌ Invalid date format: {e}")
    
    # Result validation (if present)
    if 'result' in df.columns:
        valid_results = ['H', 'A', 'D']
        invalid_results = df[~df['result'].isin(valid_results) & df['result'].notna()]
        if len(invalid_results) > 0:
            raise ValidationError(f"❌ Invalid result values: {invalid_results['result'].unique()}")
    
    print(f"   ✅ db_matches_base: {len(df)} matches validated")


def validate_match_stats_advanced(output_dir='data/clean'):
    """
    Validate advanced match statistics table (World Cup only)
    """
    print("\n🔍 Validating Advanced Match Stats...")
    
    adv_file = os.path.join(output_dir, 'db_match_stats_advanced_enhanced.csv')
    base_file = os.path.join(output_dir, 'db_matches_base_enhanced.csv')
    
    if not os.path.exists(adv_file):
        raise ValidationError(f"❌ Missing file: {adv_file}")
    
    adv_df = pd.read_csv(adv_file)
    base_df = pd.read_csv(base_file)
    valid_match_ids = set(base_df['match_id'])
    
    # Check required columns
    if 'match_id' not in adv_df.columns:
        raise ValidationError("❌ db_match_stats_advanced missing match_id column")
    
    # Check expected row count (exactly 64 for World Cup)
    if len(adv_df) != 64:
        raise ValidationError(f"❌ Unexpected advanced stats count: {len(adv_df)} (expected exactly 64)")
    
    # Validate FK: all match_ids exist in base table
    invalid_ids = set(adv_df['match_id']) - valid_match_ids
    if invalid_ids:
        raise ValidationError(f"❌ Advanced stats reference non-existent match_ids: {invalid_ids}")
    
    # Possession validation
    if 'home_possession' in adv_df.columns and 'away_possession' in adv_df.columns:
        # Check possession <= 100
        invalid = adv_df[(adv_df['home_possession'] > 100) & adv_df['home_possession'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ home_possession > 100% in {len(invalid)} matches")
        
        invalid = adv_df[(adv_df['away_possession'] > 100) & adv_df['away_possession'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ away_possession > 100% in {len(invalid)} matches")
        
        # Check possession sum ≈ 100 (±2% tolerance)
        possession_sum = adv_df[['home_possession', 'away_possession']].sum(axis=1)
        invalid = possession_sum[(possession_sum < 98) | (possession_sum > 102)]
        if len(invalid) > 0:
            raise ValidationError(f"❌ Possession sum ≠ 100% in {len(invalid)} matches (tolerance ±2%)")
    
    # xG validation (non-negative)
    if 'home_xg' in adv_df.columns:
        if (adv_df['home_xg'].dropna() < 0).any():
            raise ValidationError("❌ Negative home_xg found")
    if 'away_xg' in adv_df.columns:
        if (adv_df['away_xg'].dropna() < 0).any():
            raise ValidationError("❌ Negative away_xg found")
    
    # Passes validation
    if 'home_passes_completed' in adv_df.columns and 'home_passes_attempted' in adv_df.columns:
        invalid = adv_df[(adv_df['home_passes_completed'] > adv_df['home_passes_attempted']) & 
                        adv_df['home_passes_completed'].notna() & 
                        adv_df['home_passes_attempted'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ home_passes_completed > attempted in {len(invalid)} matches")
    
    # Pass accuracy validation
    if 'home_pass_accuracy' in adv_df.columns:
        invalid = adv_df[(adv_df['home_pass_accuracy'] > 100) & adv_df['home_pass_accuracy'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ home_pass_accuracy > 100% in {len(invalid)} matches")
    
    print(f"   ✅ db_match_stats_advanced: {len(adv_df)} records validated")


def validate_players_stats(output_dir='data/clean'):
    """
    Validate player statistics table
    """
    print("\n🔍 Validating Player Stats...")
    
    players_file = os.path.join(output_dir, 'db_players_stats_enhanced.csv')
    ref_players_file = os.path.join(output_dir, 'ref_players.csv')
    
    if not os.path.exists(players_file):
        raise ValidationError(f"❌ Missing file: {players_file}")
    
    df = pd.read_csv(players_file)
    ref_df = pd.read_csv(ref_players_file)
    valid_player_ids = set(ref_df['player_id'])
    
    # Check required columns
    required_cols = ['player_id', 'team_id']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValidationError(f"❌ db_players_stats missing columns: {missing_cols}")
    
    # Validate FK: all player_ids exist in ref_players
    invalid_ids = set(df['player_id']) - valid_player_ids
    if invalid_ids:
        raise ValidationError(f"❌ Player stats reference non-existent player_ids: {list(invalid_ids)[:5]}...")
    
    # Check expected row count (~680)
    if len(df) < 600 or len(df) > 800:
        raise ValidationError(f"❌ Unexpected player stats count: {len(df)} (expected 600-800)")
    
    # No negative values
    numeric_cols = ['goals', 'assists', 'shots', 'shots_on_target', 'minutes', 'games']
    for col in numeric_cols:
        if col in df.columns:
            if (df[col].dropna() < 0).any():
                raise ValidationError(f"❌ Negative {col} values found")
    
    # Shots validation
    if 'shots_on_target' in df.columns and 'shots' in df.columns:
        invalid = df[(df['shots_on_target'] > df['shots']) & 
                     df['shots_on_target'].notna() & 
                     df['shots'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ shots_on_target > shots for {len(invalid)} players")
    
    # Passes validation
    if 'passes_completed' in df.columns and 'passes' in df.columns:
        invalid = df[(df['passes_completed'] > df['passes']) & 
                     df['passes_completed'].notna() & 
                     df['passes'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ passes_completed > passes for {len(invalid)} players")
    
    # Goals validation
    if 'goals' in df.columns and 'shots' in df.columns:
        invalid = df[(df['goals'] > df['shots']) & 
                     df['goals'].notna() & 
                     df['shots'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ goals > shots for {len(invalid)} players (impossible)")
    
    # Percentage validations
    if 'passes_pct' in df.columns:
        invalid = df[(df['passes_pct'] > 100) & df['passes_pct'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ passes_pct > 100% for {len(invalid)} players")
    
    if 'shot_efficiency' in df.columns:
        invalid = df[(df['shot_efficiency'] > 100) & df['shot_efficiency'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ shot_efficiency > 100% for {len(invalid)} players")
    
    print(f"   ✅ db_players_stats: {len(df)} players validated")


def validate_standings(output_dir='data/clean'):
    """
    Validate standings table
    """
    print("\n🔍 Validating Standings...")
    
    standings_file = os.path.join(output_dir, 'db_standings_enhanced.csv')
    
    if not os.path.exists(standings_file):
        raise ValidationError(f"❌ Missing file: {standings_file}")
    
    df = pd.read_csv(standings_file)
    
    # Check expected row count (exactly 32 for World Cup)
    if len(df) != 32:
        raise ValidationError(f"❌ Unexpected standings count: {len(df)} (expected exactly 32)")
    
    # Check required columns
    required_cols = ['group', 'rank', 'team', 'played', 'wins', 'draws', 'losses', 'points']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValidationError(f"❌ db_standings missing columns: {missing_cols}")
    
    # Business logic: wins + draws + losses = played
    invalid = df[df['wins'] + df['draws'] + df['losses'] != df['played']]
    if len(invalid) > 0:
        raise ValidationError(f"❌ wins + draws + losses ≠ played for {len(invalid)} teams")
    
    # Business logic: points = wins*3 + draws
    expected_points = df['wins'] * 3 + df['draws']
    invalid = df[df['points'] != expected_points]
    if len(invalid) > 0:
        raise ValidationError(f"❌ Incorrect points calculation for {len(invalid)} teams")
    
    # No negative values
    numeric_cols = ['played', 'wins', 'draws', 'losses', 'goals_for', 'goals_against', 'points']
    for col in numeric_cols:
        if col in df.columns:
            if (df[col] < 0).any():
                raise ValidationError(f"❌ Negative {col} values found")
    
    # Wins/draws/losses <= played
    if (df['wins'] > df['played']).any():
        raise ValidationError("❌ wins > played for some teams")
    if (df['draws'] > df['played']).any():
        raise ValidationError("❌ draws > played for some teams")
    if (df['losses'] > df['played']).any():
        raise ValidationError("❌ losses > played for some teams")
    
    # Validate rank is unique within each group
    for group in df['group'].unique():
        group_df = df[df['group'] == group]
        if len(group_df['rank'].unique()) != len(group_df):
            raise ValidationError(f"❌ Duplicate ranks found in {group}")
    
    # Win percentage validation
    if 'win_percentage' in df.columns:
        invalid = df[(df['win_percentage'] > 100) & df['win_percentage'].notna()]
        if len(invalid) > 0:
            raise ValidationError(f"❌ win_percentage > 100% for {len(invalid)} teams")
    
    print(f"   ✅ db_standings: {len(df)} teams validated")


def validate_all(output_dir='data/clean'):
    """
    Run all validation checks
    Returns True if all pass, raises ValidationError if any fail
    """
    print_validation_header()
    
    try:
        validate_reference_tables(output_dir)
        validate_matches_base(output_dir)
        validate_match_stats_advanced(output_dir)
        validate_players_stats(output_dir)
        validate_standings(output_dir)
        
        print("\n" + "="*80)
        print("✅ ALL VALIDATION CHECKS PASSED".center(80))
        print("="*80)
        return True
        
    except ValidationError as e:
        print("\n" + "="*80)
        print("❌ VALIDATION FAILED".center(80))
        print("="*80)
        print(f"\n{str(e)}\n")
        raise


if __name__ == "__main__":
    # Run validation when script is executed directly
    try:
        validate_all()
        print("\n✨ Data quality verified - ready for production use!")
    except ValidationError as e:
        print("\n⚠️  Pipeline output failed validation checks.")
        print("   Fix data issues and re-run pipeline.")
        exit(1)
