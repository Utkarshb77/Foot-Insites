# 📋 Data Contract Documentation

## Database Schema Specification

This document defines the exact structure, types, and constraints for all output files.

---

## 🔹 Dimension Tables (Reference Data)

### `ref_teams.csv`

**Purpose**: Team reference table with unique identifiers  
**Primary Key**: team_id  
**Row Count**: ~130 teams

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `team_id` | INT | PRIMARY KEY, NOT NULL, UNIQUE | Unique team identifier (auto-incremented) |
| `team_name` | TEXT | NOT NULL | Official team name (standardized) |
| `competition_type` | TEXT | NOT NULL, IN ('league', 'international') | Team classification |
| `country` | TEXT | NULLABLE | Country for international teams |
| `primary_competition` | TEXT | NOT NULL | Main competition (e.g., 'Bundesliga', 'World Cup') |

**Example Row**:
```
2,Bayern Munich,league,,Bundesliga
114,Australia,international,Australia,World Cup
```

---

### `ref_players.csv`

**Purpose**: Player reference table with team relationships  
**Primary Key**: player_id  
**Foreign Keys**: team_id → ref_teams.team_id  
**Row Count**: ~680 players (World Cup 2022)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `player_id` | INT | PRIMARY KEY, NOT NULL, UNIQUE | Unique player identifier (auto-incremented) |
| `player_name` | TEXT | NOT NULL | Player full name |
| `team_id` | INT | FOREIGN KEY, NOT NULL | References ref_teams.team_id |
| `position` | TEXT | NULLABLE | Player position (GK, DF, MF, FW) |
| `age` | FLOAT | NULLABLE, >= 15, <= 45 | Player age at tournament time |

**Example Row**:
```
1,Aaron Mooy,114,MF,
523,Lionel Messi,103,FW,35.0
```

---

## 🔹 Fact Tables (Transactional Data)

### `db_matches_base_enhanced.csv`

**Purpose**: Core match data for ALL competitions (leagues + World Cup)  
**Primary Key**: match_id  
**Foreign Keys**: home_team_id, away_team_id → ref_teams.team_id  
**Row Count**: ~1,890 matches

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `match_id` | INT | PRIMARY KEY, NOT NULL, UNIQUE | Unique match identifier (auto-incremented) |
| `competition_name` | TEXT | NOT NULL | Competition name (e.g., 'Premier League', 'World Cup') |
| `season` | TEXT | NOT NULL | Season identifier (e.g., '2022-23') |
| `date` | DATE | NOT NULL, FORMAT: YYYY-MM-DD | Match date |
| `time` | TIME | NULLABLE, FORMAT: HH:MM:SS | Match kickoff time |
| `home_team_id` | INT | FOREIGN KEY, NOT NULL | References ref_teams.team_id |
| `away_team_id` | INT | FOREIGN KEY, NOT NULL | References ref_teams.team_id |
| `home_goals` | INT | NOT NULL, >= 0 | Home team goals scored |
| `away_goals` | INT | NOT NULL, >= 0 | Away team goals scored |
| `home_shots` | INT | NULLABLE, >= 0 | Home team total shots |
| `away_shots` | INT | NULLABLE, >= 0 | Away team total shots |
| `home_sot` | INT | NULLABLE, >= 0, <= home_shots | Home team shots on target |
| `away_sot` | INT | NULLABLE, >= 0, <= away_shots | Away team shots on target |
| `home_fouls` | INT | NULLABLE, >= 0 | Home team fouls committed |
| `away_fouls` | INT | NULLABLE, >= 0 | Away team fouls committed |
| `home_corners` | INT | NULLABLE, >= 0 | Home team corners |
| `away_corners` | INT | NULLABLE, >= 0 | Away team corners |
| `home_yellow` | INT | NULLABLE, >= 0 | Home team yellow cards |
| `away_yellow` | INT | NULLABLE, >= 0 | Away team yellow cards |
| `home_red` | INT | NULLABLE, >= 0 | Home team red cards |
| `away_red` | INT | NULLABLE, >= 0 | Away team red cards |
| `venue` | TEXT | NULLABLE | Stadium/venue name |
| `referee` | TEXT | NULLABLE | Referee name |
| **Derived Columns** ||||
| `goal_difference` | INT | CALCULATED | home_goals - away_goals |
| `total_goals` | INT | CALCULATED, >= 0 | home_goals + away_goals |
| `total_cards` | INT | CALCULATED, >= 0 | Sum of all yellow and red cards |
| `home_shot_accuracy` | FLOAT | NULLABLE, 0-100 | (home_sot / home_shots) × 100 |
| `away_shot_accuracy` | FLOAT | NULLABLE, 0-100 | (away_sot / away_shots) × 100 |
| `result` | TEXT | IN ('H', 'A', 'D') | Match result: Home win / Away win / Draw |

**Example Row**:
```
1,Bundesliga,2022-23,2023-08-18,19:30:00,1,2,0,4,...,-4,4,3,16.67,40.0,A
```

**Business Rules**:
- home_team_id ≠ away_team_id (no team plays itself)
- home_sot <= home_shots
- shot_accuracy only calculated when shots > 0

---

### `db_match_stats_advanced_enhanced.csv`

**Purpose**: Advanced match statistics (World Cup ONLY)  
**Primary Key**: match_id  
**Foreign Keys**: match_id → db_matches_base_enhanced.match_id  
**Row Count**: ~64 matches (World Cup games only)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `match_id` | INT | PRIMARY KEY, FOREIGN KEY, NOT NULL | References db_matches_base_enhanced.match_id |
| `home_xg` | FLOAT | NULLABLE, >= 0 | Home team expected goals (xG) |
| `away_xg` | FLOAT | NULLABLE, >= 0 | Away team expected goals (xG) |
| `home_possession` | FLOAT | NULLABLE, 0-100 | Home team possession percentage |
| `away_possession` | FLOAT | NULLABLE, 0-100 | Away team possession percentage |
| `home_passes_completed` | INT | NULLABLE, >= 0 | Home team completed passes |
| `home_passes_attempted` | INT | NULLABLE, >= 0 | Home team attempted passes |
| `away_passes_completed` | INT | NULLABLE, >= 0 | Away team completed passes |
| `away_passes_attempted` | INT | NULLABLE, >= 0 | Away team attempted passes |
| `home_tackles` | INT | NULLABLE, >= 0 | Home team tackles |
| `away_tackles` | INT | NULLABLE, >= 0 | Away team tackles |
| `home_interceptions` | INT | NULLABLE, >= 0 | Home team interceptions |
| `away_interceptions` | INT | NULLABLE, >= 0 | Away team interceptions |
| `home_clearances` | INT | NULLABLE, >= 0 | Home team clearances |
| `away_clearances` | INT | NULLABLE, >= 0 | Away team clearances |
| `home_saves` | INT | NULLABLE, >= 0 | Home goalkeeper saves |
| `away_saves` | INT | NULLABLE, >= 0 | Away goalkeeper saves |
| **Derived Columns** ||||
| `home_pass_accuracy` | FLOAT | NULLABLE, 0-100 | (home_passes_completed / home_passes_attempted) × 100 |
| `away_pass_accuracy` | FLOAT | NULLABLE, 0-100 | (away_passes_completed / away_passes_attempted) × 100 |
| `possession_delta` | FLOAT | CALCULATED, -100 to 100 | home_possession - away_possession |
| `xg_difference` | FLOAT | CALCULATED | home_xg - away_xg |

**Example Row**:
```
1827,0.3,1.2,47.0,53.0,,,,,10,14,2,12,18,7,1,0,,,-6.0,-0.9
```

**Business Rules**:
- home_possession + away_possession ≈ 100 (±2% tolerance for rounding)
- home_passes_completed <= home_passes_attempted
- Only populated for World Cup matches (league matches have no advanced stats)

---

### `db_players_stats_enhanced.csv`

**Purpose**: Player performance statistics with derived metrics  
**Foreign Keys**: player_id → ref_players.player_id, team_id → ref_teams.team_id  
**Row Count**: ~680 players

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `player_id` | INT | FOREIGN KEY, NOT NULL | References ref_players.player_id |
| `team_id` | INT | FOREIGN KEY, NOT NULL | References ref_teams.team_id |
| `player` | TEXT | NOT NULL | Player name (redundant for convenience) |
| `team` | TEXT | NOT NULL | Team name (redundant for convenience) |
| `position` | TEXT | NULLABLE | Player position code |
| `age` | FLOAT | NULLABLE, >= 15, <= 45 | Player age |
| `minutes` | INT | NULLABLE, >= 0 | Total minutes played |
| `games` | INT | NULLABLE, >= 0 | Games played |
| `goals` | INT | NULLABLE, >= 0 | Goals scored |
| `assists` | INT | NULLABLE, >= 0 | Assists provided |
| `shots` | INT | NULLABLE, >= 0 | Total shots |
| `shots_on_target` | INT | NULLABLE, >= 0, <= shots | Shots on target |
| `passes_completed` | INT | NULLABLE, >= 0 | Passes completed |
| `passes` | INT | NULLABLE, >= 0 | Passes attempted |
| `passes_pct` | FLOAT | NULLABLE, 0-100 | Pass completion percentage |
| `tackles` | INT | NULLABLE, >= 0 | Tackles made |
| `interceptions` | INT | NULLABLE, >= 0 | Interceptions |
| `clearances` | INT | NULLABLE, >= 0 | Clearances |
| `touches` | INT | NULLABLE, >= 0 | Total touches |
| `dispossessed` | INT | NULLABLE, >= 0 | Times dispossessed |
| `xg` | FLOAT | NULLABLE, >= 0 | Expected goals |
| `xg_assist` | FLOAT | NULLABLE, >= 0 | Expected assists |
| **Derived Columns** ||||
| `goals_per_game` | FLOAT | NULLABLE, >= 0 | goals / games |
| `assists_per_game` | FLOAT | NULLABLE, >= 0 | assists / games |
| `shot_efficiency` | FLOAT | NULLABLE, 0-100 | (goals / shots) × 100 |
| `sot_percentage` | FLOAT | NULLABLE, 0-100 | (shots_on_target / shots) × 100 |
| `goal_contributions` | INT | CALCULATED, >= 0 | goals + assists |
| `contributions_per_game` | FLOAT | NULLABLE, >= 0 | goal_contributions / games |

**Example Row**:
```
1,114,Aaron Mooy,Australia,MF,,4,4,0,0,,0,170.0,217.0,78.3,...,0,0.0
```

**Business Rules**:
- shots_on_target <= shots
- passes_completed <= passes
- goals <= shots (you can't score more than you shoot)
- Per-game metrics only calculated when games > 0

---

### `db_standings_enhanced.csv`

**Purpose**: Group stage standings with performance metrics  
**Row Count**: ~32 teams (World Cup group stage)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `group` | TEXT | NOT NULL | Group identifier (e.g., 'Group A') |
| `rank` | INT | NOT NULL, 1-4 | Position in group |
| `team` | TEXT | NOT NULL | Team name |
| `played` | INT | NOT NULL, >= 0 | Matches played |
| `wins` | INT | NOT NULL, >= 0, <= played | Matches won |
| `draws` | INT | NOT NULL, >= 0, <= played | Matches drawn |
| `losses` | INT | NOT NULL, >= 0, <= played | Matches lost |
| `goals_for` | INT | NOT NULL, >= 0 | Goals scored |
| `goals_against` | INT | NOT NULL, >= 0 | Goals conceded |
| `goal_difference` | INT | CALCULATED | goals_for - goals_against |
| `points` | INT | NOT NULL, >= 0 | Total points (Win=3, Draw=1) |
| **Derived Columns** ||||
| `win_percentage` | FLOAT | NULLABLE, 0-100 | (wins / played) × 100 |
| `points_per_game` | FLOAT | NULLABLE, >= 0 | points / played |
| `goals_per_game` | FLOAT | NULLABLE, >= 0 | goals_for / played |
| `clean_sheets` | INT | CALCULATED, >= 0 | Count of matches with 0 goals_against |

**Example Row**:
```
Group A,1,Netherlands,3,2,1,0,5,1,4,7,66.67,2.33,1.67,2
```

**Business Rules**:
- wins + draws + losses = played
- points = (wins × 3) + draws
- rank is unique within each group

---

## 🔹 Legacy Files (String-Based, Deprecated)

These files are kept for backward compatibility but should NOT be used in production:

- `clean_league_matches.csv` - Use `db_matches_base_enhanced.csv` instead
- `clean_wc_matches.csv` - Use `db_matches_base_enhanced.csv` + `db_match_stats_advanced_enhanced.csv`
- `clean_wc_standings.csv` - Use `db_standings_enhanced.csv` instead
- `clean_wc_players.csv` - Use `db_players_stats_enhanced.csv` instead
- `normalized_*.csv` - Intermediate files, use `db_*` versions

---

## 📊 Relationships Diagram

```
┌─────────────┐
│  ref_teams  │◄──┐
└─────────────┘   │
       ▲          │
       │          │
       │          │
┌──────┴──────────┴──────┐
│  db_matches_base       │
│  (1,890 rows)          │
└────────┬───────────────┘
         │
         │ 1:1 (WC only)
         │
┌────────▼───────────────┐
│ db_match_stats_adv     │
│ (64 rows)              │
└────────────────────────┘

┌─────────────┐
│ ref_players │
└──────┬──────┘
       │
       │ 1:N
       │
┌──────▼──────────────────┐
│ db_players_stats        │
│ (680 rows)              │
└─────────────────────────┘
```

---

## ⚠️ Critical Constraints

### Must-Pass Validations

1. **No NULL team_id or player_id** (foreign keys are mandatory)
2. **No negative numeric values** (goals, shots, minutes, etc.)
3. **Percentages ≤ 100** (possession, accuracy, win_percentage)
4. **shots_on_target ≤ shots**
5. **home_team_id ≠ away_team_id** (no self-matches)
6. **wins + draws + losses = played** (standings)
7. **Unique match_id** (no duplicate matches)
8. **Dates in valid format** (YYYY-MM-DD)
9. **Expected row counts** (1,890 matches, 130 teams, 680 players)

### Data Quality Thresholds

- League match count per competition: 300-400 (typical full season)
- World Cup matches: exactly 64
- World Cup teams: exactly 32
- Players per World Cup team: ~20-26

---

## 🔄 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-05 | Initial data contract with dimensional modeling |

---

## 📞 Support

For questions about this schema, contact the data engineering team or refer to [README.md](README.md).
