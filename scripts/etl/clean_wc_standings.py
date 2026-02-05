"""
Clean World Cup Standings Data
Processes 2022 World Cup group stage standings
"""

import pandas as pd
import os
from .utils import (
    clean_team_name,
    safe_int,
    remove_nulls_and_duplicates
)


# Final column order for output
FINAL_COLUMNS = [
    'group',
    'rank',
    'team',
    'played',
    'wins',
    'draws',
    'losses',
    'goals_for',
    'goals_against',
    'goal_difference',
    'points'
]


def clean_wc_standings(raw_file='2022 world cup/group_stats.csv', output_dir='data/clean'):
    """
    Clean World Cup standings data
    
    Args:
        raw_file: Path to raw World Cup standings CSV
        output_dir: Directory for output file
        
    Returns:
        Cleaned DataFrame
    """
    print("="*80)
    print("CLEANING WORLD CUP STANDINGS DATA")
    print("="*80)
    
    try:
        df = pd.read_csv(raw_file)
        print(f"Loaded {len(df)} raw records")
        
        df_clean = pd.DataFrame()
        
        # Group and rank
        if 'group' in df.columns:
            df_clean['group'] = df['group'].astype(str).str.strip()
        if 'rank' in df.columns:
            df_clean['rank'] = df['rank'].apply(safe_int)
        
        # Team name
        if 'team' in df.columns:
            df_clean['team'] = df['team'].apply(clean_team_name)
        
        # Match statistics
        column_mapping = {
            'matches_played': 'played',
            'wins': 'wins',
            'draws': 'draws',
            'losses': 'losses',
            'goals_scored': 'goals_for',
            'goals_against': 'goals_against',
            'goal_diff': 'goal_difference',
            'goal_difference': 'goal_difference',
            'points': 'points'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df_clean[new_col] = df[old_col].apply(safe_int)
        
        # Remove duplicates
        df_clean = df_clean.drop_duplicates()
        
        # Reorder columns
        for col in FINAL_COLUMNS:
            if col not in df_clean.columns:
                df_clean[col] = None
                
        df_clean = df_clean[FINAL_COLUMNS]
        
        # Save to file
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'clean_wc_standings.csv')
        df_clean.to_csv(output_file, index=False)
        
        print(f"✅ Cleaned World Cup standings: {len(df_clean)} teams")
        print(f"   Groups: {df_clean['group'].nunique()}")
        print(f"   Saved to: {output_file}")
        print("="*80)
        
        return df_clean
        
    except Exception as e:
        print(f"❌ Error cleaning World Cup standings: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == '__main__':
    clean_wc_standings()
