"""
Split Matches into Base and Advanced Stats Tables
Avoids NULL hell by separating basic match data from advanced statistics
"""

import pandas as pd
import os


def split_matches(output_dir='data/clean'):
    """
    Split match data into:
    1. matches_base - core match info (all competitions)
    2. match_stats_advanced - advanced statistics (World Cup only)
    """
    print("="*80)
    print("SPLITTING MATCHES INTO BASE AND ADVANCED TABLES")
    print("="*80)
    
    # Base columns (common to all competitions)
    BASE_COLUMNS = [
        'match_id',
        'competition_name',
        'season',
        'date',
        'time',
        'home_team_id',
        'away_team_id',
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
        'venue',
        'referee'
    ]
    
    # Advanced stats columns (World Cup only)
    ADVANCED_COLUMNS = [
        'match_id',
        'home_xg',
        'away_xg',
        'home_possession',
        'away_possession',
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
        'away_saves'
    ]
    
    all_base_matches = []
    all_advanced_stats = []
    match_id_counter = 1
    
    # Process league matches
    league_file = os.path.join(output_dir, 'normalized_league_matches.csv')
    if os.path.exists(league_file):
        df = pd.read_csv(league_file)
        
        for _, row in df.iterrows():
            base_match = {
                'match_id': match_id_counter,
                'competition_name': row['competition_name'],
                'season': row.get('season'),
                'date': row['date'],
                'time': row.get('time'),
                'home_team_id': row['home_team_id'],
                'away_team_id': row['away_team_id'],
                'home_goals': row.get('home_goals'),
                'away_goals': row.get('away_goals'),
                'home_shots': row.get('home_shots'),
                'away_shots': row.get('away_shots'),
                'home_sot': row.get('home_sot'),
                'away_sot': row.get('away_sot'),
                'home_fouls': row.get('home_fouls'),
                'away_fouls': row.get('away_fouls'),
                'home_corners': row.get('home_corners'),
                'away_corners': row.get('away_corners'),
                'home_yellow': row.get('home_yellow'),
                'away_yellow': row.get('away_yellow'),
                'home_red': row.get('home_red'),
                'away_red': row.get('away_red'),
                'venue': row.get('venue'),
                'referee': row.get('referee')
            }
            all_base_matches.append(base_match)
            match_id_counter += 1
        
        print(f"✅ Processed {len(df)} league matches")
    
    # Process World Cup matches
    wc_file = os.path.join(output_dir, 'normalized_wc_matches.csv')
    if os.path.exists(wc_file):
        df = pd.read_csv(wc_file)
        
        for _, row in df.iterrows():
            # Base match data
            base_match = {
                'match_id': match_id_counter,
                'competition_name': row['competition_name'],
                'season': row.get('season'),
                'date': row['date'],
                'time': row.get('time'),
                'home_team_id': row['home_team_id'],
                'away_team_id': row['away_team_id'],
                'home_goals': row.get('home_goals'),
                'away_goals': row.get('away_goals'),
                'home_shots': row.get('home_shots'),
                'away_shots': row.get('away_shots'),
                'home_sot': row.get('home_sot'),
                'away_sot': row.get('away_sot'),
                'home_fouls': row.get('home_fouls'),
                'away_fouls': row.get('away_fouls'),
                'home_corners': row.get('home_corners'),
                'away_corners': row.get('away_corners'),
                'home_yellow': row.get('home_yellow'),
                'away_yellow': row.get('away_yellow'),
                'home_red': row.get('home_red'),
                'away_red': row.get('away_red'),
                'venue': row.get('venue'),
                'referee': None  # WC data doesn't have referee
            }
            all_base_matches.append(base_match)
            
            # Advanced stats (WC only)
            advanced_stats = {
                'match_id': match_id_counter,
                'home_xg': row.get('home_xg'),
                'away_xg': row.get('away_xg'),
                'home_possession': row.get('home_possession'),
                'away_possession': row.get('away_possession'),
                'home_passes_completed': row.get('home_passes_completed'),
                'home_passes_attempted': row.get('home_passes_attempted'),
                'away_passes_completed': row.get('away_passes_completed'),
                'away_passes_attempted': row.get('away_passes_attempted'),
                'home_tackles': row.get('home_tackles'),
                'away_tackles': row.get('away_tackles'),
                'home_interceptions': row.get('home_interceptions'),
                'away_interceptions': row.get('away_interceptions'),
                'home_clearances': row.get('home_clearances'),
                'away_clearances': row.get('away_clearances'),
                'home_saves': row.get('home_saves'),
                'away_saves': row.get('away_saves')
            }
            all_advanced_stats.append(advanced_stats)
            
            match_id_counter += 1
        
        print(f"✅ Processed {len(df)} World Cup matches (with advanced stats)")
    
    # Create DataFrames
    matches_base_df = pd.DataFrame(all_base_matches)
    match_stats_advanced_df = pd.DataFrame(all_advanced_stats)
    
    # Save base matches
    base_file = os.path.join(output_dir, 'db_matches_base.csv')
    matches_base_df.to_csv(base_file, index=False)
    print(f"\n✅ Created matches_base table: {len(matches_base_df)} matches")
    print(f"   Saved to: {base_file}")
    
    # Save advanced stats
    if len(all_advanced_stats) > 0:
        advanced_file = os.path.join(output_dir, 'db_match_stats_advanced.csv')
        match_stats_advanced_df.to_csv(advanced_file, index=False)
        print(f"✅ Created match_stats_advanced table: {len(match_stats_advanced_df)} records")
        print(f"   Saved to: {advanced_file}")
    
    print("="*80)
    
    return matches_base_df, match_stats_advanced_df


if __name__ == '__main__':
    split_matches()
