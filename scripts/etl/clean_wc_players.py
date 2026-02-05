"""
Clean & Merge World Cup Player Data
Merges multiple player statistics files into unified dataset
"""

import pandas as pd
import os
from .utils import (
    clean_team_name,
    safe_int,
    safe_float,
    remove_nulls_and_duplicates
)


# Player data files and columns to extract
PLAYER_FILES = {
    'player_stats.csv': ['player', 'position', 'team', 'age', 'minutes_90s', 'games', 'goals', 'assists'],
    'player_shooting.csv': ['player', 'team', 'shots_total', 'shots_on_target', 'xg'],
    'player_passing.csv': ['player', 'team', 'passes_completed', 'passes', 'passes_pct'],
    'player_defense.csv': ['player', 'team', 'tackles', 'interceptions', 'clearances'],
    'player_possession.csv': ['player', 'team', 'touches', 'dispossessed'],
    'player_gca.csv': ['player', 'team', 'xg_assist']
}

# Final column order for output
FINAL_COLUMNS = [
    'player',
    'team',
    'position',
    'age',
    'minutes',
    'games',
    'goals',
    'assists',
    'shots',
    'shots_on_target',
    'passes_completed',
    'passes',
    'passes_pct',
    'tackles',
    'interceptions',
    'clearances',
    'touches',
    'dispossessed',
    'xg',
    'xg_assist'
]


def clean_wc_players(raw_dir='2022 world cup/player data', output_dir='data/clean'):
    """
    Merge and clean World Cup player data from multiple files
    
    Args:
        raw_dir: Directory containing raw player CSV files
        output_dir: Directory for output file
        
    Returns:
        Merged and cleaned DataFrame
    """
    print("="*80)
    print("CLEANING WORLD CUP PLAYER DATA")
    print("="*80)
    
    try:
        merged_df = None
        
        for filename, key_cols in PLAYER_FILES.items():
            file_path = os.path.join(raw_dir, filename)
            
            if not os.path.exists(file_path):
                print(f"⚠️  File not found: {filename}")
                continue
            
            df = pd.read_csv(file_path)
            print(f"   Processing {filename}: {len(df)} rows")
            
            # Filter to only existing columns
            existing_cols = [col for col in key_cols if col in df.columns]
            
            if not existing_cols:
                print(f"   ⚠️  No matching columns in {filename}")
                continue
            
            df_subset = df[existing_cols].copy()
            
            # Clean player and team names
            if 'player' in df_subset.columns:
                df_subset['player'] = df_subset['player'].apply(clean_team_name)
            if 'team' in df_subset.columns:
                df_subset['team'] = df_subset['team'].apply(clean_team_name)
            
            # Merge with main dataframe
            if merged_df is None:
                merged_df = df_subset
            else:
                merge_keys = ['player', 'team'] if 'player' in existing_cols and 'team' in existing_cols else ['player']
                merged_df = merged_df.merge(df_subset, on=merge_keys, how='outer', suffixes=('', '_dup'))
            
            print(f"   ✅ Merged {filename}")
        
        if merged_df is None:
            print("❌ No player data files found")
            return None
        
        # Rename columns to match schema
        column_rename = {
            'minutes_90s': 'minutes',
            'shots_total': 'shots'
        }
        merged_df = merged_df.rename(columns=column_rename)
        
        # Convert numeric columns
        int_cols = [
            'age', 'minutes', 'games', 'goals', 'assists', 'shots',
            'shots_on_target', 'passes_completed', 'passes',
            'tackles', 'interceptions', 'clearances', 'touches', 'dispossessed'
        ]
        
        for col in int_cols:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].apply(safe_int)
        
        float_cols = ['xg', 'xg_assist', 'passes_pct']
        for col in float_cols:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].apply(safe_float)
        
        # Remove duplicates
        merged_df = merged_df.drop_duplicates(subset=['player', 'team'], keep='first')
        
        # Remove rows without player name
        merged_df = merged_df.dropna(subset=['player'])
        
        # Reorder columns
        for col in FINAL_COLUMNS:
            if col not in merged_df.columns:
                merged_df[col] = None
                
        merged_df = merged_df[FINAL_COLUMNS]
        
        # Save to file
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'clean_wc_players.csv')
        merged_df.to_csv(output_file, index=False)
        
        print(f"\n✅ Merged player data: {len(merged_df)} players")
        print(f"   Teams: {merged_df['team'].nunique()}")
        print(f"   Saved to: {output_file}")
        print("="*80)
        
        return merged_df
        
    except Exception as e:
        print(f"❌ Error merging player data: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == '__main__':
    clean_wc_players()
