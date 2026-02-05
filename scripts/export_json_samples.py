"""
JSON Sample Exporter
Generates sample JSON files for frontend development.
Allows frontend work to start without backend dependency.

Output:
- data/json_samples/sample_matches.json
- data/json_samples/sample_players.json
- data/json_samples/sample_standings.json
- data/json_samples/sample_teams.json
"""

import pandas as pd
import json
import os


def create_json_directory():
    """Create JSON samples directory if it doesn't exist"""
    output_dir = 'data/json_samples'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir


def export_sample_matches(output_dir, limit=20):
    """
    Export sample matches JSON
    """
    print(f"\n📄 Exporting sample matches...")
    
    # Load matches data
    matches_file = 'data/clean/db_matches_base_enhanced.csv'
    if not os.path.exists(matches_file):
        print(f"   ❌ File not found: {matches_file}")
        print("   ⚠️  Run pipeline first: python scripts/run_pipeline.py")
        return
    
    df = pd.read_csv(matches_file)
    
    # Get sample - mix of competitions
    sample_df = df.groupby('competition_name', group_keys=False).apply(lambda x: x.head(4))
    sample_df = sample_df.head(limit)
    
    # Convert to JSON-friendly format
    matches_data = []
    for _, row in sample_df.iterrows():
        match = {
            "matchId": int(row['match_id']),
            "competition": row['competition_name'],
            "season": row['season'],
            "date": row['date'],
            "time": row['time'] if pd.notna(row['time']) else None,
            "homeTeam": {
                "teamId": int(row['home_team_id']),
                "goals": int(row['home_goals']),
                "shots": int(row['home_shots']) if pd.notna(row['home_shots']) else None,
                "shotsOnTarget": int(row['home_sot']) if pd.notna(row['home_sot']) else None,
                "shotAccuracy": float(row['home_shot_accuracy']) if pd.notna(row['home_shot_accuracy']) else None,
                "fouls": int(row['home_fouls']) if pd.notna(row['home_fouls']) else None,
                "corners": int(row['home_corners']) if pd.notna(row['home_corners']) else None,
                "yellowCards": int(row['home_yellow']) if pd.notna(row['home_yellow']) else None,
                "redCards": int(row['home_red']) if pd.notna(row['home_red']) else None
            },
            "awayTeam": {
                "teamId": int(row['away_team_id']),
                "goals": int(row['away_goals']),
                "shots": int(row['away_shots']) if pd.notna(row['away_shots']) else None,
                "shotsOnTarget": int(row['away_sot']) if pd.notna(row['away_sot']) else None,
                "shotAccuracy": float(row['away_shot_accuracy']) if pd.notna(row['away_shot_accuracy']) else None,
                "fouls": int(row['away_fouls']) if pd.notna(row['away_fouls']) else None,
                "corners": int(row['away_corners']) if pd.notna(row['away_corners']) else None,
                "yellowCards": int(row['away_yellow']) if pd.notna(row['away_yellow']) else None,
                "redCards": int(row['away_red']) if pd.notna(row['away_red']) else None
            },
            "stats": {
                "goalDifference": int(row['goal_difference']),
                "totalGoals": int(row['total_goals']),
                "totalCards": int(row['total_cards']),
                "result": row['result']
            },
            "venue": row['venue'] if pd.notna(row['venue']) else None,
            "referee": row['referee'] if pd.notna(row['referee']) else None
        }
        matches_data.append(match)
    
    # Save to JSON
    output_file = os.path.join(output_dir, 'sample_matches.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(matches_data, f, indent=2, ensure_ascii=False)
    
    print(f"   ✅ Exported {len(matches_data)} matches to {output_file}")


def export_sample_players(output_dir, limit=30):
    """
    Export sample players JSON
    """
    print(f"\n📄 Exporting sample players...")
    
    # Load players data
    players_file = 'data/clean/db_players_stats_enhanced.csv'
    if not os.path.exists(players_file):
        print(f"   ❌ File not found: {players_file}")
        return
    
    df = pd.read_csv(players_file)
    
    # Get top players by goals
    sample_df = df.sort_values('goals', ascending=False, na_position='last').head(limit)
    
    # Convert to JSON-friendly format
    players_data = []
    for _, row in sample_df.iterrows():
        player = {
            "playerId": int(row['player_id']),
            "name": row['player'],
            "teamId": int(row['team_id']),
            "team": row['team'],
            "position": row['position'] if pd.notna(row['position']) else None,
            "age": float(row['age']) if pd.notna(row['age']) else None,
            "stats": {
                "minutes": int(row['minutes']) if pd.notna(row['minutes']) else None,
                "games": int(row['games']) if pd.notna(row['games']) else None,
                "goals": int(row['goals']) if pd.notna(row['goals']) else 0,
                "assists": int(row['assists']) if pd.notna(row['assists']) else 0,
                "shots": int(row['shots']) if pd.notna(row['shots']) else None,
                "shotsOnTarget": int(row['shots_on_target']) if pd.notna(row['shots_on_target']) else None,
                "passesCompleted": int(row['passes_completed']) if pd.notna(row['passes_completed']) else None,
                "passesAttempted": int(row['passes']) if pd.notna(row['passes']) else None,
                "passAccuracy": float(row['passes_pct']) if pd.notna(row['passes_pct']) else None,
                "tackles": int(row['tackles']) if pd.notna(row['tackles']) else None,
                "interceptions": int(row['interceptions']) if pd.notna(row['interceptions']) else None,
                "touches": int(row['touches']) if pd.notna(row['touches']) else None,
                "xG": float(row['xg']) if pd.notna(row['xg']) else None,
                "xA": float(row['xg_assist']) if pd.notna(row['xg_assist']) else None
            },
            "metrics": {
                "goalsPerGame": float(row['goals_per_game']) if pd.notna(row['goals_per_game']) else None,
                "assistsPerGame": float(row['assists_per_game']) if pd.notna(row['assists_per_game']) else None,
                "shotEfficiency": float(row['shot_efficiency']) if pd.notna(row['shot_efficiency']) else None,
                "sotPercentage": float(row['sot_percentage']) if pd.notna(row['sot_percentage']) else None,
                "goalContributions": int(row['goal_contributions']) if pd.notna(row['goal_contributions']) else 0,
                "contributionsPerGame": float(row['contributions_per_game']) if pd.notna(row['contributions_per_game']) else None
            }
        }
        players_data.append(player)
    
    # Save to JSON
    output_file = os.path.join(output_dir, 'sample_players.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(players_data, f, indent=2, ensure_ascii=False)
    
    print(f"   ✅ Exported {len(players_data)} players to {output_file}")


def export_sample_standings(output_dir):
    """
    Export sample standings JSON (all groups)
    """
    print(f"\n📄 Exporting sample standings...")
    
    # Load standings data
    standings_file = 'data/clean/db_standings_enhanced.csv'
    if not os.path.exists(standings_file):
        print(f"   ❌ File not found: {standings_file}")
        return
    
    df = pd.read_csv(standings_file)
    
    # Group by group
    standings_by_group = {}
    for group in df['group'].unique():
        group_df = df[df['group'] == group].sort_values('rank')
        
        teams = []
        for _, row in group_df.iterrows():
            team = {
                "rank": int(row['rank']),
                "team": str(row['team']),
                "stats": {
                    "played": int(row['played']),
                    "wins": int(row['wins']),
                    "draws": int(row['draws']),
                    "losses": int(row['losses']),
                    "goalsFor": int(row['goals_for']),
                    "goalsAgainst": int(row['goals_against']),
                    "goalDifference": int(row['goal_difference']),
                    "points": int(row['points'])
                },
                "metrics": {
                    "winPercentage": float(row['win_percentage']) if pd.notna(row['win_percentage']) else 0.0,
                    "pointsPerGame": float(row['points_per_game']) if pd.notna(row['points_per_game']) else 0.0,
                    "goalsPerGame": float(row['goals_per_game']) if pd.notna(row['goals_per_game']) else 0.0,
                    "cleanSheets": int(row['clean_sheets']) if pd.notna(row['clean_sheets']) else 0
                }
            }
            teams.append(team)
        
        standings_by_group[str(group)] = teams
    
    # Save to JSON
    output_file = os.path.join(output_dir, 'sample_standings.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(standings_by_group, f, indent=2, ensure_ascii=False)
    
    print(f"   ✅ Exported {len(standings_by_group)} groups to {output_file}")


def export_sample_teams(output_dir, limit=50):
    """
    Export sample teams JSON
    """
    print(f"\n📄 Exporting sample teams...")
    
    # Load teams data
    teams_file = 'data/clean/ref_teams.csv'
    if not os.path.exists(teams_file):
        print(f"   ❌ File not found: {teams_file}")
        return
    
    df = pd.read_csv(teams_file)
    sample_df = df.head(limit)
    
    # Convert to JSON-friendly format
    teams_data = []
    for _, row in sample_df.iterrows():
        team = {
            "teamId": int(row['team_id']),
            "name": row['team_name'],
            "type": row['competition_type'],
            "country": row['country'] if pd.notna(row['country']) else None,
            "competition": row['primary_competition']
        }
        teams_data.append(team)
    
    # Save to JSON
    output_file = os.path.join(output_dir, 'sample_teams.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(teams_data, f, indent=2, ensure_ascii=False)
    
    print(f"   ✅ Exported {len(teams_data)} teams to {output_file}")


def generate_all_samples():
    """
    Generate all JSON samples
    """
    print("\n" + "="*80)
    print("GENERATING JSON SAMPLES FOR FRONTEND".center(80))
    print("="*80)
    
    output_dir = create_json_directory()
    
    try:
        export_sample_matches(output_dir, limit=20)
        export_sample_players(output_dir, limit=30)
        export_sample_standings(output_dir)
        export_sample_teams(output_dir, limit=50)
        
        print("\n" + "="*80)
        print("✅ JSON SAMPLES GENERATED SUCCESSFULLY".center(80))
        print("="*80)
        print(f"\n📁 Files saved to: {output_dir}/")
        print("\n💡 Usage in React/Frontend:")
        print("   import matches from './data/json_samples/sample_matches.json';")
        print("   import players from './data/json_samples/sample_players.json';")
        print("   import standings from './data/json_samples/sample_standings.json';")
        print("   import teams from './data/json_samples/sample_teams.json';")
        print("\n✨ No backend needed - start development immediately!")
        
    except Exception as e:
        print(f"\n❌ Error generating JSON samples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    generate_all_samples()
