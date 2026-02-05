"""
Derive Performance Metrics
Calculates derived metrics like pass accuracy, shot accuracy, etc.
Makes the app "smart" like SofaScore and FotMob
"""

import pandas as pd
import os


def derive_match_metrics(output_dir='data/clean'):
    """
    Calculate derived metrics for matches
    """
    print("="*80)
    print("DERIVING MATCH PERFORMANCE METRICS")
    print("="*80)
    
    # Load base matches
    base_file = os.path.join(output_dir, 'db_matches_base.csv')
    if not os.path.exists(base_file):
        print("❌ Base matches file not found.")
        return None
    
    df = pd.read_csv(base_file)
    
    # Derive basic metrics
    df['goal_difference'] = df['home_goals'] - df['away_goals']
    df['total_goals'] = df['home_goals'] + df['away_goals']
    df['total_cards'] = (df['home_yellow'].fillna(0) + df['away_yellow'].fillna(0) + 
                         df['home_red'].fillna(0) + df['away_red'].fillna(0))
    
    # Shot accuracy
    df['home_shot_accuracy'] = df.apply(
        lambda row: (row['home_sot'] / row['home_shots'] * 100) 
        if pd.notna(row['home_shots']) and row['home_shots'] > 0 else None,
        axis=1
    )
    df['away_shot_accuracy'] = df.apply(
        lambda row: (row['away_sot'] / row['away_shots'] * 100)
        if pd.notna(row['away_shots']) and row['away_shots'] > 0 else None,
        axis=1
    )
    
    # Match result
    df['result'] = df.apply(
        lambda row: 'H' if row['home_goals'] > row['away_goals']
        else ('A' if row['away_goals'] > row['home_goals'] else 'D'),
        axis=1
    )
    
    # Save enhanced base matches
    output_file = os.path.join(output_dir, 'db_matches_base_enhanced.csv')
    df.to_csv(output_file, index=False)
    
    print(f"✅ Derived metrics for {len(df)} matches")
    print(f"   Added: goal_difference, total_goals, total_cards, shot_accuracy, result")
    print(f"   Saved to: {output_file}")
    
    # Load advanced stats if available
    advanced_file = os.path.join(output_dir, 'db_match_stats_advanced.csv')
    advanced_df = None
    
    if os.path.exists(advanced_file):
        advanced_df = pd.read_csv(advanced_file)
        
        # Pass accuracy
        advanced_df['home_pass_accuracy'] = advanced_df.apply(
            lambda row: (row['home_passes_completed'] / row['home_passes_attempted'] * 100)
            if pd.notna(row['home_passes_attempted']) and row['home_passes_attempted'] > 0 else None,
            axis=1
        )
        advanced_df['away_pass_accuracy'] = advanced_df.apply(
            lambda row: (row['away_passes_completed'] / row['away_passes_attempted'] * 100)
            if pd.notna(row['away_passes_attempted']) and row['away_passes_attempted'] > 0 else None,
            axis=1
        )
        
        # Possession delta (dominance)
        advanced_df['possession_delta'] = advanced_df.apply(
            lambda row: (row['home_possession'] - row['away_possession'])
            if pd.notna(row['home_possession']) and pd.notna(row['away_possession']) else None,
            axis=1
        )
        
        # xG difference (expected goal dominance)
        advanced_df['xg_difference'] = advanced_df.apply(
            lambda row: (row['home_xg'] - row['away_xg'])
            if pd.notna(row['home_xg']) and pd.notna(row['away_xg']) else None,
            axis=1
        )
        
        # Save enhanced advanced stats
        output_file = os.path.join(output_dir, 'db_match_stats_advanced_enhanced.csv')
        advanced_df.to_csv(output_file, index=False)
        
        print(f"✅ Derived advanced metrics for {len(advanced_df)} matches")
        print(f"   Added: pass_accuracy, possession_delta, xg_difference")
        print(f"   Saved to: {output_file}")
    
    print("="*80)
    
    return df, advanced_df


def derive_player_metrics(output_dir='data/clean'):
    """
    Calculate derived metrics for players
    """
    print("="*80)
    print("DERIVING PLAYER PERFORMANCE METRICS")
    print("="*80)
    
    # Load normalized player data
    players_file = os.path.join(output_dir, 'normalized_wc_players.csv')
    if not os.path.exists(players_file):
        print("❌ Player data file not found.")
        return None
    
    df = pd.read_csv(players_file)
    
    # Goals per game
    df['goals_per_game'] = df.apply(
        lambda row: (row['goals'] / row['games'])
        if pd.notna(row['games']) and row['games'] > 0 else None,
        axis=1
    )
    
    # Assists per game
    df['assists_per_game'] = df.apply(
        lambda row: (row['assists'] / row['games'])
        if pd.notna(row['games']) and row['games'] > 0 else None,
        axis=1
    )
    
    # Shot efficiency (goals per shot)
    df['shot_efficiency'] = df.apply(
        lambda row: (row['goals'] / row['shots'] * 100)
        if pd.notna(row['shots']) and row['shots'] > 0 else None,
        axis=1
    )
    
    # Shots on target percentage
    df['sot_percentage'] = df.apply(
        lambda row: (row['shots_on_target'] / row['shots'] * 100)
        if pd.notna(row['shots']) and row['shots'] > 0 else None,
        axis=1
    )
    
    # Goal contributions (goals + assists)
    df['goal_contributions'] = df.apply(
        lambda row: (row['goals'] if pd.notna(row['goals']) else 0) + 
                    (row['assists'] if pd.notna(row['assists']) else 0),
        axis=1
    )
    
    # Goal contributions per game
    df['contributions_per_game'] = df.apply(
        lambda row: (row['goal_contributions'] / row['games'])
        if pd.notna(row['games']) and row['games'] > 0 else None,
        axis=1
    )
    
    # Save enhanced player data
    output_file = os.path.join(output_dir, 'db_players_stats_enhanced.csv')
    df.to_csv(output_file, index=False)
    
    print(f"✅ Derived metrics for {len(df)} players")
    print(f"   Added: goals_per_game, assists_per_game, shot_efficiency,")
    print(f"          sot_percentage, goal_contributions, contributions_per_game")
    print(f"   Saved to: {output_file}")
    print("="*80)
    
    return df


def derive_standings_metrics(output_dir='data/clean'):
    """
    Calculate additional standings metrics
    """
    print("="*80)
    print("DERIVING STANDINGS METRICS")
    print("="*80)
    
    standings_file = os.path.join(output_dir, 'clean_wc_standings.csv')
    if not os.path.exists(standings_file):
        print("❌ Standings file not found.")
        return None
    
    df = pd.read_csv(standings_file)
    
    # Win percentage
    df['win_percentage'] = df.apply(
        lambda row: (row['wins'] / row['played'] * 100)
        if pd.notna(row['played']) and row['played'] > 0 else None,
        axis=1
    )
    
    # Points per game
    df['points_per_game'] = df.apply(
        lambda row: (row['points'] / row['played'])
        if pd.notna(row['played']) and row['played'] > 0 else None,
        axis=1
    )
    
    # Goals per game
    df['goals_per_game'] = df.apply(
        lambda row: (row['goals_for'] / row['played'])
        if pd.notna(row['played']) and row['played'] > 0 else None,
        axis=1
    )
    
    # Clean sheet percentage (matches without conceding)
    df['clean_sheets'] = df.apply(
        lambda row: row['played'] - row['losses']  # Simplification
        if pd.notna(row['played']) and pd.notna(row['losses']) else None,
        axis=1
    )
    
    # Save enhanced standings
    output_file = os.path.join(output_dir, 'db_standings_enhanced.csv')
    df.to_csv(output_file, index=False)
    
    print(f"✅ Derived metrics for {len(df)} teams")
    print(f"   Added: win_percentage, points_per_game, goals_per_game, clean_sheets")
    print(f"   Saved to: {output_file}")
    print("="*80)
    
    return df


def derive_all_metrics(output_dir='data/clean'):
    """
    Derive all performance metrics
    """
    matches_df, advanced_df = derive_match_metrics(output_dir)
    players_df = derive_player_metrics(output_dir)
    standings_df = derive_standings_metrics(output_dir)
    
    return matches_df, advanced_df, players_df, standings_df


if __name__ == '__main__':
    derive_all_metrics()
