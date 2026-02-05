"""
Clean World Cup Match Data
Processes 2022 World Cup match statistics
"""

import pandas as pd
import os
from .utils import (
    standardize_date,
    standardize_time,
    clean_team_name,
    safe_int,
    safe_float,
    remove_nulls_and_duplicates
)


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
    'home_xg',
    'away_xg',
    'home_possession',
    'away_possession',
    'home_shots',
    'away_shots',
    'home_sot',
    'away_sot',
    'home_passes_completed',
    'home_passes_attempted',
    'away_passes_completed',
    'away_passes_attempted',
    'home_tackles',
    'away_tackles',
    'home_interceptions',
    'away_interceptions',
    'home_clearances',
    'away_clearances',
    'home_saves',
    'away_saves',
    'venue'
]


def clean_wc_matches(raw_file='2022 world cup/data.csv', output_dir='data/clean'):
    """
    Clean World Cup match data
    
    Args:
        raw_file: Path to raw World Cup match CSV
        output_dir: Directory for output file
        
    Returns:
        Cleaned DataFrame
    """
    print("="*80)
    print("CLEANING WORLD CUP MATCH DATA")
    print("="*80)
    
    try:
        df = pd.read_csv(raw_file)
        print(f"Loaded {len(df)} raw records")
        
        df_clean = pd.DataFrame()
        
        # Core match info
        if 'date' in df.columns:
            df_clean['date'] = df['date'].apply(standardize_date)
        
        if 'time' in df.columns:
            df_clean['time'] = df['time'].apply(standardize_time)
        
        # Teams and scores
        if 'home_team' in df.columns:
            df_clean['home_team'] = df['home_team'].apply(clean_team_name)
        if 'away_team' in df.columns:
            df_clean['away_team'] = df['away_team'].apply(clean_team_name)
        
        if 'home_score' in df.columns:
            df_clean['home_goals'] = df['home_score'].apply(safe_int)
        if 'away_score' in df.columns:
            df_clean['away_goals'] = df['away_score'].apply(safe_int)
        
        # Venue and referee
        if 'venue' in df.columns:
            df_clean['venue'] = df['venue'].astype(str).str.strip()
        
        # Expected goals (xG)
        if 'home_xg' in df.columns:
            df_clean['home_xg'] = df['home_xg'].apply(safe_float)
        if 'away_xg' in df.columns:
            df_clean['away_xg'] = df['away_xg'].apply(safe_float)
        
        # Possession
        if 'home_possession' in df.columns:
            df_clean['home_possession'] = df['home_possession'].apply(safe_float)
        if 'away_possession' in df.columns:
            df_clean['away_possession'] = df['away_possession'].apply(safe_float)
        
        # Shots
        for old, new in [('home_shots_total', 'home_shots'), ('away_shots_total', 'away_shots'),
                         ('home_shots_on_target', 'home_sot'), ('away_shots_on_target', 'away_sot')]:
            if old in df.columns:
                df_clean[new] = df[old].apply(safe_int)
        
        # Passes
        for old, new in [('home_passes_completed', 'home_passes_completed'),
                         ('home_passes', 'home_passes_attempted'),
                         ('away_passes_completed', 'away_passes_completed'),
                         ('away_passes', 'away_passes_attempted')]:
            if old in df.columns:
                df_clean[new] = df[old].apply(safe_int)
        
        # Defensive actions
        for col in ['home_tackles', 'away_tackles', 'home_interceptions', 'away_interceptions',
                    'home_clearances', 'away_clearances', 'home_saves', 'away_saves']:
            if col in df.columns:
                df_clean[col] = df[col].apply(safe_int)
        
        # Add metadata
        df_clean['competition_name'] = 'World Cup'
        df_clean['season'] = '2022'
        
        # Remove nulls and duplicates
        df_clean = remove_nulls_and_duplicates(
            df_clean,
            required_cols=['home_team', 'away_team']
        )
        
        # Reorder columns
        for col in FINAL_COLUMNS:
            if col not in df_clean.columns:
                df_clean[col] = None
                
        df_clean = df_clean[FINAL_COLUMNS]
        
        # Save to file
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'clean_wc_matches.csv')
        df_clean.to_csv(output_file, index=False)
        
        print(f"✅ Cleaned World Cup matches: {len(df_clean)} matches")
        print(f"   Saved to: {output_file}")
        print("="*80)
        
        return df_clean
        
    except Exception as e:
        print(f"❌ Error cleaning World Cup match data: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == '__main__':
    clean_wc_matches()
