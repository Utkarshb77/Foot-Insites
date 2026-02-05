"""
Normalize Data with ID-Based References
Replaces team/player names with foreign key IDs
"""

import pandas as pd
import os


def normalize_matches(output_dir='data/clean'):
    """
    Replace team names with team_ids in match data
    """
    print("="*80)
    print("NORMALIZING MATCH DATA WITH TEAM IDs")
    print("="*80)
    
    # Load teams reference
    teams_file = os.path.join(output_dir, 'ref_teams.csv')
    if not os.path.exists(teams_file):
        print("❌ Teams reference table not found.")
        return None, None
    
    teams_df = pd.read_csv(teams_file)
    team_name_to_id = dict(zip(teams_df['team_name'], teams_df['team_id']))
    
    # Normalize league matches
    league_file = os.path.join(output_dir, 'clean_league_matches.csv')
    league_normalized = None
    
    if os.path.exists(league_file):
        df = pd.read_csv(league_file)
        
        # Add team IDs
        df['home_team_id'] = df['home_team'].map(team_name_to_id)
        df['away_team_id'] = df['away_team'].map(team_name_to_id)
        
        # Keep team names for reference but they won't be in final DB
        # Reorder columns to put IDs first
        id_cols = ['home_team_id', 'away_team_id']
        name_cols = ['home_team', 'away_team']
        other_cols = [c for c in df.columns if c not in id_cols + name_cols]
        
        df = df[id_cols + name_cols + other_cols]
        
        output_file = os.path.join(output_dir, 'normalized_league_matches.csv')
        df.to_csv(output_file, index=False)
        league_normalized = df
        
        print(f"✅ Normalized league matches: {len(df)} records")
        print(f"   Mapped teams: {df['home_team_id'].notna().sum()}/{len(df)}")
        print(f"   Saved to: {output_file}")
    
    # Normalize World Cup matches
    wc_file = os.path.join(output_dir, 'clean_wc_matches.csv')
    wc_normalized = None
    
    if os.path.exists(wc_file):
        df = pd.read_csv(wc_file)
        
        # Add team IDs
        df['home_team_id'] = df['home_team'].map(team_name_to_id)
        df['away_team_id'] = df['away_team'].map(team_name_to_id)
        
        # Reorder columns
        id_cols = ['home_team_id', 'away_team_id']
        name_cols = ['home_team', 'away_team']
        other_cols = [c for c in df.columns if c not in id_cols + name_cols]
        
        df = df[id_cols + name_cols + other_cols]
        
        output_file = os.path.join(output_dir, 'normalized_wc_matches.csv')
        df.to_csv(output_file, index=False)
        wc_normalized = df
        
        print(f"✅ Normalized WC matches: {len(df)} records")
        print(f"   Mapped teams: {df['home_team_id'].notna().sum()}/{len(df)}")
        print(f"   Saved to: {output_file}")
    
    print("="*80)
    
    return league_normalized, wc_normalized


def normalize_players(output_dir='data/clean'):
    """
    Replace team names with team_ids in player data
    """
    print("="*80)
    print("NORMALIZING PLAYER DATA WITH IDs")
    print("="*80)
    
    # Load references
    teams_file = os.path.join(output_dir, 'ref_teams.csv')
    players_ref_file = os.path.join(output_dir, 'ref_players.csv')
    
    if not os.path.exists(teams_file) or not os.path.exists(players_ref_file):
        print("❌ Reference tables not found.")
        return None
    
    players_df = pd.read_csv(players_ref_file)
    
    # Load player stats
    stats_file = os.path.join(output_dir, 'clean_wc_players.csv')
    if not os.path.exists(stats_file):
        print("❌ Player stats file not found.")
        return None
    
    stats_df = pd.read_csv(stats_file)
    
    # Create player name to ID mapping
    player_name_team_to_id = {
        (row['player_name'], row['team_name']): row['player_id']
        for _, row in players_df.iterrows()
    }
    
    # Add player_id to stats
    stats_df['player_id'] = stats_df.apply(
        lambda row: player_name_team_to_id.get((row['player'], row['team'])),
        axis=1
    )
    stats_df['team_id'] = stats_df['player_id'].map(
        dict(zip(players_df['player_id'], players_df['team_id']))
    )
    
    # Reorder: IDs first
    id_cols = ['player_id', 'team_id']
    name_cols = ['player', 'team']
    other_cols = [c for c in stats_df.columns if c not in id_cols + name_cols]
    
    stats_df = stats_df[id_cols + name_cols + other_cols]
    
    output_file = os.path.join(output_dir, 'normalized_wc_players.csv')
    stats_df.to_csv(output_file, index=False)
    
    print(f"✅ Normalized player data: {len(stats_df)} records")
    print(f"   Mapped players: {stats_df['player_id'].notna().sum()}/{len(stats_df)}")
    print(f"   Saved to: {output_file}")
    print("="*80)
    
    return stats_df


def normalize_data(output_dir='data/clean'):
    """
    Normalize all data with ID-based references
    """
    league_df, wc_df = normalize_matches(output_dir)
    players_df = normalize_players(output_dir)
    
    return league_df, wc_df, players_df


if __name__ == '__main__':
    normalize_data()
