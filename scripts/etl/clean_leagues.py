"""
Clean European League Match Data
Processes league CSV files into unified format
"""

import pandas as pd
import os
from .utils import (
    standardize_date,
    standardize_time,
    clean_team_name,
    safe_int,
    remove_nulls_and_duplicates
)


# League configurations
LEAGUE_CONFIG = {
    'D1.csv': {'name': 'Bundesliga', 'country': 'Germany'},
    'E0.csv': {'name': 'Premier League', 'country': 'England'},
    'F1.csv': {'name': 'Ligue 1', 'country': 'France'},
    'I1.csv': {'name': 'Serie A', 'country': 'Italy'},
    'SP1.csv': {'name': 'La Liga', 'country': 'Spain'}
}

# Column mapping from raw to clean
COLUMN_MAPPING = {
    'Date': 'date',
    'Time': 'time',
    'HomeTeam': 'home_team',
    'AwayTeam': 'away_team',
    'FTHG': 'home_goals',
    'FTAG': 'away_goals',
    'HS': 'home_shots',
    'AS': 'away_shots',
    'HST': 'home_sot',
    'AST': 'away_sot',
    'HF': 'home_fouls',
    'AF': 'away_fouls',
    'HC': 'home_corners',
    'AC': 'away_corners',
    'HY': 'home_yellow',
    'AY': 'away_yellow',
    'HR': 'home_red',
    'AR': 'away_red',
    'Referee': 'referee'
}

# Final column order for output
FINAL_COLUMNS = [
    'date',
    'time',
    'competition_name',
    'season',
    'home_team',
    'away_team',
    'home_goals',
    'away_goals',
    'home_shots',
    'away_shots',
    'home_sot',
    'away_sot',
    'home_fouls',
    'away_fouls',
    'home_corners',
    'away_corners',
    'home_yellow',
    'away_yellow',
    'home_red',
    'away_red',
    'referee'
]


def clean_single_league(file_path, league_name, country_name, season='2022-23'):
    """
    Clean a single league CSV file
    
    Args:
        file_path: Path to raw CSV file
        league_name: Name of the league
        country_name: Country of the league
        season: Season identifier
        
    Returns:
        Cleaned DataFrame
    """
    try:
        # Read raw data
        df = pd.read_csv(file_path)
        
        # Select and rename columns
        available_cols = {k: v for k, v in COLUMN_MAPPING.items() if k in df.columns}
        df_clean = df[list(available_cols.keys())].copy()
        df_clean.columns = [available_cols[col] for col in df_clean.columns]
        
        # Add metadata
        df_clean['competition_name'] = league_name
        df_clean['season'] = season
        
        # Standardize data types
        df_clean['date'] = df_clean['date'].apply(standardize_date)
        
        if 'time' in df_clean.columns:
            df_clean['time'] = df_clean['time'].apply(standardize_time)
        else:
            df_clean['time'] = None
            
        df_clean['home_team'] = df_clean['home_team'].apply(clean_team_name)
        df_clean['away_team'] = df_clean['away_team'].apply(clean_team_name)
        
        # Convert numeric columns
        numeric_cols = [
            'home_goals', 'away_goals', 'home_shots', 'away_shots',
            'home_sot', 'away_sot', 'home_fouls', 'away_fouls',
            'home_corners', 'away_corners', 'home_yellow', 'away_yellow',
            'home_red', 'away_red'
        ]
        
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(safe_int)
        
        # Remove nulls and duplicates
        df_clean = remove_nulls_and_duplicates(
            df_clean,
            required_cols=['date', 'home_team', 'away_team']
        )
        
        # Reorder columns
        for col in FINAL_COLUMNS:
            if col not in df_clean.columns:
                df_clean[col] = None
                
        df_clean = df_clean[FINAL_COLUMNS]
        
        print(f"✅ Cleaned {league_name}: {len(df_clean)} matches")
        return df_clean
        
    except Exception as e:
        print(f"❌ Error cleaning {league_name}: {e}")
        return None


def clean_leagues(raw_dir='leagues', output_dir='data/clean'):
    """
    Process all league files and create unified clean dataset
    
    Args:
        raw_dir: Directory containing raw league CSV files
        output_dir: Directory for output file
        
    Returns:
        Combined DataFrame of all leagues
    """
    print("="*80)
    print("CLEANING LEAGUE DATASETS")
    print("="*80)
    
    all_league_data = []
    
    for filename, config in LEAGUE_CONFIG.items():
        file_path = os.path.join(raw_dir, filename)
        
        if not os.path.exists(file_path):
            print(f"⚠️  File not found: {file_path}")
            continue
            
        cleaned = clean_single_league(
            file_path,
            config['name'],
            config['country']
        )
        
        if cleaned is not None:
            all_league_data.append(cleaned)
    
    if not all_league_data:
        print("❌ No league data processed")
        return None
    
    # Combine all leagues
    combined_df = pd.concat(all_league_data, ignore_index=True)
    
    # Save to file
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'clean_league_matches.csv')
    combined_df.to_csv(output_file, index=False)
    
    print(f"\n✅ Combined all leagues: {len(combined_df)} total matches")
    print(f"   Leagues included: {combined_df['competition_name'].nunique()}")
    print(f"   Saved to: {output_file}")
    print("="*80)
    
    return combined_df


if __name__ == '__main__':
    clean_leagues()
