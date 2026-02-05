"""
Create Reference Tables (Dimension Tables)
Generates normalized teams and players tables with unique IDs
"""

import pandas as pd
import os


def create_teams_table(output_dir='data/clean'):
    """
    Generate teams reference table from all data sources
    
    Returns:
        DataFrame with team_id, team_name, competition_type, country
    """
    print("="*80)
    print("CREATING TEAMS REFERENCE TABLE")
    print("="*80)
    
    teams_data = []
    team_id_counter = 1
    seen_teams = set()
    
    # Extract teams from league matches
    league_file = os.path.join(output_dir, 'clean_league_matches.csv')
    if os.path.exists(league_file):
        df = pd.read_csv(league_file)
        
        for _, row in df.iterrows():
            competition = row['competition_name']
            
            # Home team
            if row['home_team'] and row['home_team'] not in seen_teams:
                teams_data.append({
                    'team_id': team_id_counter,
                    'team_name': row['home_team'],
                    'competition_type': 'league',
                    'country': None,  # Could be derived from competition_name
                    'primary_competition': competition
                })
                seen_teams.add(row['home_team'])
                team_id_counter += 1
            
            # Away team
            if row['away_team'] and row['away_team'] not in seen_teams:
                teams_data.append({
                    'team_id': team_id_counter,
                    'team_name': row['away_team'],
                    'competition_type': 'league',
                    'country': None,
                    'primary_competition': competition
                })
                seen_teams.add(row['away_team'])
                team_id_counter += 1
        
        print(f"✅ Extracted {len([t for t in teams_data if t['competition_type'] == 'league'])} league teams")
    
    # Extract teams from World Cup matches
    wc_file = os.path.join(output_dir, 'clean_wc_matches.csv')
    if os.path.exists(wc_file):
        df = pd.read_csv(wc_file)
        
        for _, row in df.iterrows():
            # Home team
            if row['home_team'] and row['home_team'] not in seen_teams:
                teams_data.append({
                    'team_id': team_id_counter,
                    'team_name': row['home_team'],
                    'competition_type': 'international',
                    'country': row['home_team'],  # National team name
                    'primary_competition': 'World Cup'
                })
                seen_teams.add(row['home_team'])
                team_id_counter += 1
            
            # Away team
            if row['away_team'] and row['away_team'] not in seen_teams:
                teams_data.append({
                    'team_id': team_id_counter,
                    'team_name': row['away_team'],
                    'competition_type': 'international',
                    'country': row['away_team'],
                    'primary_competition': 'World Cup'
                })
                seen_teams.add(row['away_team'])
                team_id_counter += 1
        
        print(f"✅ Extracted {len([t for t in teams_data if t['competition_type'] == 'international'])} international teams")
    
    # Create DataFrame
    teams_df = pd.DataFrame(teams_data)
    
    # Save to file
    output_file = os.path.join(output_dir, 'ref_teams.csv')
    teams_df.to_csv(output_file, index=False)
    
    print(f"\n✅ Created teams reference table: {len(teams_df)} unique teams")
    print(f"   Saved to: {output_file}")
    print("="*80)
    
    return teams_df


def create_players_table(output_dir='data/clean'):
    """
    Generate players reference table from World Cup player data
    
    Returns:
        DataFrame with player_id, player_name, team_id, position, age
    """
    print("="*80)
    print("CREATING PLAYERS REFERENCE TABLE")
    print("="*80)
    
    # Load teams reference first
    teams_file = os.path.join(output_dir, 'ref_teams.csv')
    if not os.path.exists(teams_file):
        print("❌ Teams reference table not found. Run create_teams_table() first.")
        return None
    
    teams_df = pd.read_csv(teams_file)
    team_name_to_id = dict(zip(teams_df['team_name'], teams_df['team_id']))
    
    # Load player data
    players_file = os.path.join(output_dir, 'clean_wc_players.csv')
    if not os.path.exists(players_file):
        print("❌ Player data not found.")
        return None
    
    df = pd.read_csv(players_file)
    
    players_data = []
    
    for idx, row in df.iterrows():
        # Map team name to team_id
        team_id = team_name_to_id.get(row['team'])
        
        players_data.append({
            'player_id': idx + 1,
            'player_name': row['player'],
            'team_id': team_id,
            'team_name': row['team'],  # Keep for reference
            'position': row['position'] if pd.notna(row['position']) else None,
            'age': int(row['age']) if pd.notna(row['age']) else None
        })
    
    players_df = pd.DataFrame(players_data)
    
    # Save to file
    output_file = os.path.join(output_dir, 'ref_players.csv')
    players_df.to_csv(output_file, index=False)
    
    print(f"✅ Created players reference table: {len(players_df)} players")
    print(f"   Teams mapped: {players_df['team_id'].notna().sum()}")
    print(f"   Saved to: {output_file}")
    print("="*80)
    
    return players_df


def create_reference_tables(output_dir='data/clean'):
    """
    Create all reference tables
    """
    teams_df = create_teams_table(output_dir)
    players_df = create_players_table(output_dir)
    
    return teams_df, players_df


if __name__ == '__main__':
    create_reference_tables()
