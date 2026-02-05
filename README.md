# ⚽ Football Data ETL Pipeline

> **Production-grade ETL system transforming multi-source football data into a unified, analytics-ready database**

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-1.5+-green.svg)](https://pandas.pydata.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen.svg)]()

</div>

---

## 📖 Overview

An automated ETL (Extract, Transform, Load) pipeline that processes **1,890+ football matches** from multiple sources (Top 5 European leagues + FIFA World Cup 2022) into a normalized, analytics-ready database schema. Built with enterprise-grade features including **dimensional modeling**, **ID-based normalization**, **automatic validation**, and **derived performance metrics**.

### 🎯 What This Pipeline Does

- ✅ **Unifies** disparate data sources (5 leagues + World Cup) into single schema
- ✅ **Normalizes** team/player names into integer foreign keys (no string joins)
- ✅ **Separates** base stats (common) from advanced stats (World Cup) to avoid NULL hell
- ✅ **Calculates** 15+ derived metrics (pass accuracy, shot efficiency, xG deltas)
- ✅ **Validates** all output with 40+ sanity checks and business rules
- ✅ **Exports** JSON samples for instant frontend development

### 🏆 Key Differentiators

| Feature | Traditional ETL | This Pipeline |
|---------|----------------|---------------|
| Team References | String-based ("Bayern Munich") | ID-based (team_id: 2) |
| NULL Handling | Mixed NULL columns | Separate base/advanced tables |
| Validation | Manual checks | 40+ automated validations |
| Metrics | Raw stats only | 15+ derived analytics |
| Frontend Support | Backend dependency | JSON samples included |
| Documentation | README only | Full data contract + schema |

---

## 🎯 Key Features

---

## 🎯 Key Features

### 🏗️ Enterprise Architecture

<table>
<tr>
<td width="50%">

**Dimensional Modeling**
- ⭐ Star schema design
- 🔑 2 dimension tables (teams, players)
- 📊 4 fact tables (matches, advanced stats, player stats, standings)
- 🔗 Integer foreign keys for optimal joins

</td>
<td width="50%">

**Data Quality**
- ✅ 40+ validation checks
- ✅ FK integrity verification
- ✅ Business rule enforcement
- ✅ NULL prevention strategy

</td>
</tr>
<tr>
<td>

**Performance Metrics**
- 📈 15+ derived analytics
- 📈 SofaScore-style calculations
- 📈 Per-game statistics
- 📈 Efficiency percentages

</td>
<td>

**Developer Experience**
- 🚀 Single command execution
- 📄 Complete data contract
- 🎨 JSON samples for frontend
- 📖 Comprehensive documentation

</td>
</tr>
</table>

### 📊 Data Sources

| Source | Entity | Count | Coverage |
|--------|--------|-------|----------|
| 🇩🇪 **Bundesliga** | Matches | 306 | 2022-23 Season |
| 🏴󠁧󠁢󠁥󠁮󠁧󠁿 **Premier League** | Matches | 380 | 2022-23 Season |
| 🇫🇷 **Ligue 1** | Matches | 380 | 2022-23 Season |
| 🇮🇹 **Serie A** | Matches | 380 | 2022-23 Season |
| 🇪🇸 **La Liga** | Matches | 380 | 2022-23 Season |
| 🏆 **World Cup 2022** | Matches | 64 | Complete Tournament |
| 🏆 **World Cup 2022** | Players | 681 | All Squads |
| 🏆 **World Cup 2022** | Standings | 32 Teams | Group Stage |
| | **TOTAL** | **1,891 matches** | **130 teams** |

---

## 🏗️ Architecture & Design

### Star Schema (Dimensional Model)

```
                    ┌────────────────┐
                    │   ref_teams    │◄──────────┐
                    │  (130 teams)   │           │
                    │ ───────────────│           │
                    │ team_id    (PK)│           │
                    │ team_name      │           │
                    │ country        │           │
                    └────────┬───────┘           │
                             │                   │
                    ┌────────┴────────┐          │
                    │                 │          │
        ┌───────────▼─────┐   ┌───────▼──────────▼────────┐
        │  ref_players    │   │  db_matches_base_enhanced │
        │  (681 players)  │   │      (1,891 matches)      │
        │ ────────────────│   │ ──────────────────────────│
        │ player_id   (PK)│   │ match_id              (PK)│
        │ team_id     (FK)│───┤ home_team_id          (FK)│
        │ player_name     │   │ away_team_id          (FK)│
        │ position        │   │ date, goals, shots, cards │
        └────────┬────────┘   │ + 15 derived metrics      │
                 │            └──────────┬────────────────┘
                 │                       │
                 │                       │ 1:1 (WC only)
                 │                       │
                 │            ┌──────────▼────────────────┐
                 │            │ db_match_stats_advanced   │
                 │            │     (64 WC matches)       │
                 │            │ ──────────────────────────│
                 │            │ match_id          (PK, FK)│
                 │            │ xG, possession, passes    │
                 │            │ + 4 derived metrics       │
                 │            └───────────────────────────┘
                 │
        ┌────────▼─────────────────┐
        │ db_players_stats_enhanced│
        │      (681 players)       │
        │ ─────────────────────────│
        │ player_id            (FK)│
        │ team_id              (FK)│
        │ goals, assists, shots    │
        │ + 6 derived metrics      │
        └──────────────────────────┘
```

### Design Principles

| Principle | Implementation | Benefit |
|-----------|----------------|---------|
| **Normalization** | Teams/players as dimension tables | Eliminates data redundancy |
| **ID-Based Joins** | Integer foreign keys (not strings) | 10-100x faster joins in SQL |
| **NULL Avoidance** | Separate base/advanced tables | Prevents sparse nullable columns |
| **Pre-Aggregation** | Derived metrics calculated upfront | Faster analytics queries |
| **Data Validation** | 40+ automated checks | Ensures data quality |
| **Documentation** | Full data contract | Eliminates ambiguity |

---

---

## 📁 Project Structure

```
foot-insights/
│
├── 📂 data/
│   ├── 📂 clean/                           ✅ OUTPUT FILES (Database-Ready)
│   │   ├── ref_teams.csv                        130 teams, 5 cols
│   │   ├── ref_players.csv                      681 players, 6 cols
│   │   ├── db_matches_base_enhanced.csv         1,891 matches, 28 cols
│   │   ├── db_match_stats_advanced_enhanced.csv 64 WC matches, 20 cols
│   │   ├── db_players_stats_enhanced.csv        681 players, 28 cols
│   │   └── db_standings_enhanced.csv            32 teams, 15 cols
│   │
│   └── 📂 json_samples/                    ✅ FRONTEND SAMPLES
│       ├── sample_matches.json                  20 matches
│       ├── sample_players.json                  30 players
│       ├── sample_standings.json                8 groups
│       └── sample_teams.json                    50 teams
│
├── 📂 leagues/                             📥 INPUT (Raw League Data)
│   ├── D1.csv                                   Bundesliga (306 rows)
│   ├── E0.csv                                   Premier League (380 rows)
│   ├── F1.csv                                   Ligue 1 (380 rows)
│   ├── I1.csv                                   Serie A (380 rows)
│   └── SP1.csv                                  La Liga (380 rows)
│
├── 📂 2022 world cup/                      📥 INPUT (Raw World Cup Data)
│   ├── data.csv                                 64 matches
│   ├── group_stats.csv                          32 teams (standings)
│   ├── team_data.csv
│   ├── team_tips.json
│   └── 📂 player data/
│       ├── player_stats.csv                     680 rows
│       ├── player_shooting.csv                  680 rows
│       ├── player_passing.csv                   680 rows
│       ├── player_defense.csv                   680 rows
│       ├── player_possession.csv                680 rows
│       ├── player_gca.csv                       680 rows
│       └── player_data_description.json
│
├── 📂 scripts/
│   ├── 📂 etl/                             ⚙️ ETL MODULES
│   │   ├── __init__.py
│   │   ├── utils.py                             Helper functions
│   │   ├── clean_leagues.py                     Step 1: Clean league data
│   │   ├── clean_wc_matches.py                  Step 2: Clean WC matches
│   │   ├── clean_wc_standings.py                Step 3: Clean WC standings
│   │   ├── clean_wc_players.py                  Step 4: Merge player data
│   │   ├── create_reference_tables.py           Step 5: Create dimensions
│   │   ├── normalize_data.py                    Step 6: ID normalization
│   │   ├── split_matches.py                     Step 7: Split base/advanced
│   │   ├── derive_metrics.py                    Step 8: Calculate metrics
│   │   └── validate_data.py                     Step 9: Validate output
│   │
│   ├── run_pipeline.py                     🚀 MAIN ORCHESTRATOR
│   └── export_json_samples.py              📤 JSON EXPORTER
│
├── 📄 README.md                            📖 This file
├── 📄 DATA_CONTRACT.md                     📋 Complete schema specification
├── 📄 QUICKSTART.py                        ⚡ Quick reference guide
├── 📄 requirements.txt                     📦 Python dependencies
├── 📄 .gitignore
└── 📄 Dataset_Schema_Analysis.ipynb        📊 Initial data exploration

```

---

## 🚀 Installation & Setup

### Prerequisites

- **Python 3.11+** (tested on 3.11, should work on 3.8+)
- **pip** package manager
- **5-10 seconds** execution time
- **~15 MB** disk space for output files

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/foot-insights.git
cd foot-insights
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

**Or manually:**

```bash
pip install pandas numpy
```

That's it! No complex setup, no database required.

---

## ⚡ Quick Start

### Run Complete Pipeline

**From project root:**

```powershell
python scripts/run_pipeline.py
```

**From scripts directory:**

```powershell
cd scripts
python run_pipeline.py
```

**Expected output:**

```
================================================================================
                           FOOTBALL DATA ETL PIPELINE
================================================================================

Started at: 2026-02-05 11:41:53

================================================================================
                         STEP 1/9: CLEANING LEAGUE DATA
================================================================================
✅ Cleaned Bundesliga: 306 matches
✅ Cleaned Premier League: 380 matches
✅ Cleaned Ligue 1: 380 matches
✅ Cleaned Serie A: 380 matches
✅ Cleaned La Liga: 380 matches
✅ Combined all leagues: 1826 total matches

...

================================================================================
                      STEP 9/9: VALIDATING OUTPUT DATA
================================================================================
🔍 Validating Reference Tables...
   ✅ ref_teams: 130 teams validated
   ✅ ref_players: 681 players validated
🔍 Validating Match Base Data...
   ✅ db_matches_base: 1891 matches validated
...
✅ ALL VALIDATION CHECKS PASSED

================================================================================
Pipeline completed: 9/9 steps successful
Duration: 1.42 seconds
================================================================================

🎉 PIPELINE COMPLETE - DATABASE-READY OUTPUT GENERATED!
```

### Generate JSON Samples for Frontend

```powershell
python scripts/export_json_samples.py
```

**Output:** 4 JSON files in `data/json_samples/` ready for React/Vue/Angular.

---

## 📋 Usage Guide

### Running Individual Steps

You can run specific cleaning modules independently:

```python
# Clean leagues only
python -m scripts.etl.clean_leagues

# Clean World Cup matches only
python -m scripts.etl.clean_wc_matches

# Clean World Cup standings only
python -m scripts.etl.clean_wc_standings

# Merge World Cup player data only
python -m scripts.etl.clean_wc_players

# Create reference tables only
python -m scripts.etl.create_reference_tables

# Validate output data only
python -m scripts.etl.validate_data
```

### Using the Output Data

#### Option 1: Import into Database (SQL)

```sql
-- PostgreSQL / MySQL
CREATE TABLE ref_teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    competition_type VARCHAR(20),
    country VARCHAR(50),
    primary_competition VARCHAR(100)
);

COPY ref_teams FROM '/path/to/data/clean/ref_teams.csv' CSV HEADER;

-- Repeat for other tables...
```

#### Option 2: Load in Python

```python
import pandas as pd

# Load dimension tables
teams = pd.read_csv('data/clean/ref_teams.csv')
players = pd.read_csv('data/clean/ref_players.csv')

# Load fact tables
matches = pd.read_csv('data/clean/db_matches_base_enhanced.csv')
advanced_stats = pd.read_csv('data/clean/db_match_stats_advanced_enhanced.csv')
player_stats = pd.read_csv('data/clean/db_players_stats_enhanced.csv')
standings = pd.read_csv('data/clean/db_standings_enhanced.csv')

# Join example: Get match with team names
match_detail = matches.merge(
    teams, left_on='home_team_id', right_on='team_id', suffixes=('', '_home')
).merge(
    teams, left_on='away_team_id', right_on='team_id', suffixes=('', '_away')
)
```

#### Option 3: Use JSON Samples (Frontend)

```javascript
// React/Vue/Angular
import matches from './data/json_samples/sample_matches.json';
import players from './data/json_samples/sample_players.json';
import standings from './data/json_samples/sample_standings.json';

// Use immediately - no backend needed!
const topScorer = players[0];
console.log(`${topScorer.name}: ${topScorer.stats.goals} goals`);
```

---

---

## 📊 Database Schema (Complete Reference)

> **💡 Tip:** For detailed column specifications, constraints, and business rules, see **[DATA_CONTRACT.md](DATA_CONTRACT.md)**

### 🔹 Dimension Tables

#### `ref_teams.csv` (130 rows)

**Purpose:** Master list of all teams with unique identifiers

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `team_id` | INT | 🔑 Primary Key | `2` |
| `team_name` | VARCHAR(100) | Team display name | `Bayern Munich` |
| `competition_type` | VARCHAR(20) | `league` or `international` | `league` |
| `country` | VARCHAR(50) | Country (for national teams) | `NULL` (clubs), `Argentina` (national) |
| `primary_competition` | VARCHAR(100) | Main competition | `Bundesliga` |

**Sample Data:**
```csv
team_id,team_name,competition_type,country,primary_competition
2,Bayern Munich,league,,Bundesliga
103,Argentina,international,Argentina,World Cup
114,Australia,international,Australia,World Cup
```

---

#### `ref_players.csv` (681 rows)

**Purpose:** Master list of all World Cup 2022 players with team relationships

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `player_id` | INT | 🔑 Primary Key | `523` |
| `player_name` | VARCHAR(100) | Player full name | `Lionel Messi` |
| `team_id` | INT | 🔗 FK → ref_teams | `103` |
| `team_name` | VARCHAR(100) | Team name (denormalized) | `Argentina` |
| `position` | VARCHAR(10) | Position code | `FW` |
| `age` | FLOAT | Age at tournament | `35.0` |

**Sample Data:**
```csv
player_id,player_name,team_id,team_name,position,age
523,Lionel Messi,103,Argentina,FW,35.0
587,Kylian Mbappé,122,France,FW,23.0
```

---

### 🔹 Fact Tables

#### `db_matches_base_enhanced.csv` (1,891 rows)

**Purpose:** Core match data for ALL competitions (leagues + World Cup)

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | INT | 🔑 Primary Key |
| `competition_name` | VARCHAR(50) | Competition name |
| `season` | VARCHAR(20) | Season identifier |
| `date` | DATE | Match date (YYYY-MM-DD) |
| `time` | TIME | Kickoff time (HH:MM:SS) |
| `home_team_id` | INT | 🔗 FK → ref_teams |
| `away_team_id` | INT | 🔗 FK → ref_teams |
| `home_goals` | INT | Home team goals |
| `away_goals` | INT | Away team goals |
| `home_shots` | INT | Home team total shots |
| `away_shots` | INT | Away team total shots |
| `home_sot` | INT | Home shots on target |
| `away_sot` | INT | Away shots on target |
| `home_fouls` | INT | Home team fouls |
| `away_fouls` | INT | Away team fouls |
| `home_corners` | INT | Home team corners |
| `away_corners` | INT | Away team corners |
| `home_yellow` | INT | Home yellow cards |
| `away_yellow` | INT | Away yellow cards |
| `home_red` | INT | Home red cards |
| `away_red` | INT | Away red cards |
| `venue` | VARCHAR(100) | Stadium name |
| `referee` | VARCHAR(100) | Referee name |

**Derived Metrics:**

| Column | Formula | Description |
|--------|---------|-------------|
| `goal_difference` | home_goals - away_goals | Goal differential |
| `total_goals` | home_goals + away_goals | Combined score |
| `total_cards` | home_yellow + away_yellow + home_red + away_red | Total cards |
| `home_shot_accuracy` | (home_sot / home_shots) × 100 | Home shooting % |
| `away_shot_accuracy` | (away_sot / away_shots) × 100 | Away shooting % |
| `result` | H / A / D | Match result |

**Sample Data:**
```csv
match_id,competition_name,season,date,home_team_id,away_team_id,home_goals,away_goals,...
1,Bundesliga,2022-23,2023-08-18,1,2,0,4,...
1827,World Cup,2022,2022-11-20,130,119,0,2,...
```

---

#### `db_match_stats_advanced_enhanced.csv` (64 rows)

**Purpose:** Advanced statistics (World Cup ONLY) - avoids NULL hell for league matches

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | INT | 🔑 PK, 🔗 FK → db_matches_base |
| `home_xg` | FLOAT | Home expected goals |
| `away_xg` | FLOAT | Away expected goals |
| `home_possession` | FLOAT | Home possession % (0-100) |
| `away_possession` | FLOAT | Away possession % (0-100) |
| `home_passes_completed` | INT | Completed passes |
| `home_passes_attempted` | INT | Attempted passes |
| `away_passes_completed` | INT | Completed passes |
| `away_passes_attempted` | INT | Attempted passes |
| `home_tackles` | INT | Tackles made |
| `away_tackles` | INT | Tackles made |
| `home_interceptions` | INT | Interceptions |
| `away_interceptions` | INT | Interceptions |
| `home_clearances` | INT | Clearances |
| `away_clearances` | INT | Clearances |
| `home_saves` | INT | Goalkeeper saves |
| `away_saves` | INT | Goalkeeper saves |

**Derived Metrics:**

| Column | Formula |
|--------|---------|
| `home_pass_accuracy` | (completed / attempted) × 100 |
| `away_pass_accuracy` | (completed / attempted) × 100 |
| `possession_delta` | home_possession - away_possession |
| `xg_difference` | home_xg - away_xg |

**Why Separate?** League matches don't have xG, possession, etc. Keeping them in the base table would create 1,826 rows with NULLs. This design keeps data clean and normalized.

---

#### `db_players_stats_enhanced.csv` (681 rows)

**Purpose:** Individual player performance statistics with calculated metrics

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | INT | 🔗 FK → ref_players |
| `team_id` | INT | 🔗 FK → ref_teams |
| `player` | VARCHAR(100) | Player name (denormalized for convenience) |
| `team` | VARCHAR(100) | Team name (denormalized) |
| `position` | VARCHAR(10) | Position |
| `age` | FLOAT | Player age |
| `minutes` | INT | Minutes played |
| `games` | INT | Games played |
| `goals` | INT | Goals scored |
| `assists` | INT | Assists |
| `shots` | INT | Total shots |
| `shots_on_target` | INT | Shots on target |
| `passes_completed` | INT | Passes completed |
| `passes` | INT | Passes attempted |
| `passes_pct` | FLOAT | Pass completion % |
| `tackles` | INT | Tackles |
| `interceptions` | INT | Interceptions |
| `clearances` | INT | Clearances |
| `touches` | INT | Total touches |
| `dispossessed` | INT | Times dispossessed |
| `xg` | FLOAT | Expected goals |
| `xg_assist` | FLOAT | Expected assists |

**Derived Metrics:**

| Column | Formula |
|--------|---------|
| `goals_per_game` | goals / games |
| `assists_per_game` | assists / games |
| `shot_efficiency` | (goals / shots) × 100 |
| `sot_percentage` | (shots_on_target / shots) × 100 |
| `goal_contributions` | goals + assists |
| `contributions_per_game` | (goals + assists) / games |

---

#### `db_standings_enhanced.csv` (32 rows)

**Purpose:** World Cup group stage standings with performance metrics

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `group` | VARCHAR(20) | Group identifier |
| `rank` | INT | Position in group (1-4) |
| `team` | VARCHAR(100) | Team name |
| `played` | INT | Matches played |
| `wins` | INT | Matches won |
| `draws` | INT | Matches drawn |
| `losses` | INT | Matches lost |
| `goals_for` | INT | Goals scored |
| `goals_against` | INT | Goals conceded |
| `goal_difference` | INT | Goal differential |
| `points` | INT | Total points |

**Derived Metrics:**

| Column | Formula |
|--------|---------|
| `win_percentage` | (wins / played) × 100 |
| `points_per_game` | points / played |
| `goals_per_game` | goals_for / played |
| `clean_sheets` | Count of matches with 0 goals_against |

**Business Rules:**
- `wins + draws + losses = played`
- `points = (wins × 3) + draws`
- Rank is unique within each group

---

---

## ⚙️ ETL Pipeline (9-Step Process)

### Pipeline Flow Diagram

```
INPUT                    PROCESSING                   OUTPUT
━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━

📂 leagues/             ┌─────────────────────┐
  D1, E0, F1,           │   STEP 1            │
  I1, SP1.csv     ─────►│   Clean Leagues     │─────┐
                        │   (1,826 matches)   │     │
                        └─────────────────────┘     │
                                                    │
📂 2022 world cup/      ┌─────────────────────┐     │
  data.csv        ─────►│   STEP 2            │     │
                        │   Clean WC Matches  │─────┤
                        │   (64 matches)      │     │
                        └─────────────────────┘     │
                                                    │
📂 2022 world cup/      ┌─────────────────────┐     │
  group_stats.csv ─────►│   STEP 3            │     │
                        │   Clean WC          │─────┤
                        │   Standings         │     │
                        └─────────────────────┘     │
                                                    │
📂 player data/         ┌─────────────────────┐     │
  6 CSV files     ─────►│   STEP 4            │     │
                        │   Merge Player Data │─────┤
                        │   (681 players)     │     │
                        └─────────────────────┘     │
                                                    │
All cleaned data  ─────►┌─────────────────────┐     │
                        │   STEP 5            │     │
                        │   Create Reference  │─────┼───► ref_teams.csv
                        │   Tables            │     │     ref_players.csv
                        └─────────────────────┘     │
                                                    │
With ref tables   ─────►┌─────────────────────┐     │
                        │   STEP 6            │     │
                        │   Normalize with    │─────┤
                        │   ID-based FKs      │     │
                        └─────────────────────┘     │
                                                    │
Normalized data   ─────►┌─────────────────────┐     │
                        │   STEP 7            │     │
                        │   Split Base &      │─────┼───► db_matches_base.csv
                        │   Advanced Stats    │     │     db_match_stats_adv.csv
                        └─────────────────────┘     │
                                                    │
Split tables      ─────►┌─────────────────────┐     │
                        │   STEP 8            │     │
                        │   Derive            │─────┼───► *_enhanced.csv
                        │   Performance       │     │     (all fact tables)
                        │   Metrics           │     │
                        └─────────────────────┘     │
                                                    │
All outputs       ─────►┌─────────────────────┐     │
                        │   STEP 9            │     │
                        │   Validate Data     │─────┴───► ✅ PASS / ❌ FAIL
                        │   (40+ checks)      │
                        └─────────────────────┘

                                │
                                ▼
                        ┌─────────────────────┐
                        │   OPTIONAL          │
                        │   Export JSON       │─────────► 📂 json_samples/
                        │   Samples           │           (4 JSON files)
                        └─────────────────────┘
```

### Step-by-Step Breakdown

| Step | Module | Input | Output | Duration |
|------|--------|-------|--------|----------|
| **1** | `clean_leagues.py` | 5 league CSVs | `clean_league_matches.csv` | 0.2s |
| **2** | `clean_wc_matches.py` | WC matches CSV | `clean_wc_matches.csv` | 0.05s |
| **3** | `clean_wc_standings.py` | Group stats CSV | `clean_wc_standings.csv` | 0.02s |
| **4** | `clean_wc_players.py` | 6 player CSVs | `clean_wc_players.csv` | 0.15s |
| **5** | `create_reference_tables.py` | Cleaned CSVs | `ref_teams.csv`, `ref_players.csv` | 0.1s |
| **6** | `normalize_data.py` | Cleaned + Ref tables | `normalized_*.csv` | 0.15s |
| **7** | `split_matches.py` | Normalized CSVs | `db_matches_base.csv`, `db_match_stats_advanced.csv` | 0.1s |
| **8** | `derive_metrics.py` | Split tables | `*_enhanced.csv` (4 files) | 0.2s |
| **9** | `validate_data.py` | All outputs | Validation report | 0.5s |
| | **TOTAL** | 14 files | 6 database tables | **~1.4s** |

### Transformation Rules

#### Data Standardization

| Aspect | Rule | Example |
|--------|------|---------|
| **Dates** | Convert to `YYYY-MM-DD` | `18/08/2023` → `2023-08-18` |
| **Times** | Convert to `HH:MM:SS` | `19:30` → `19:30:00` |
| **Team Names** | Trim whitespace, standardize | `  Bayern  ` → `Bayern Munich` |
| **Numeric Fields** | Cast to proper types | `"4"` → `4` (int) |
| **Missing Data** | Remove from critical fields | NULL goals → row removed |
| **Duplicates** | Remove based on key columns | Duplicate match_id → keep first |
| **Column Names** | snake_case convention | `HomeGoals` → `home_goals` |

#### ID Assignment

```python
# Teams: Ordered by competition, then alphabetically
team_id_sequence = 1, 2, 3, ... (clubs first, then national teams)

# Players: Ordered alphabetically by player name
player_id_sequence = 1, 2, 3, ...

# Matches: Sequential from first to last processed
match_id_sequence = 1, 2, 3, ...
```

---

## ✅ Data Validation (Step 9)

### Validation Categories

The pipeline performs **40+ automated checks** before marking execution as successful:

#### 1. Foreign Key Integrity

```python
✅ All home_team_id values exist in ref_teams.team_id
✅ All away_team_id values exist in ref_teams.team_id
✅ All player_id values exist in ref_players.player_id
✅ All match_id in advanced stats exist in base matches
```

#### 2. NULL Violations

```python
✅ No NULL in team_id, player_id (foreign keys)
✅ No NULL in match_id (primary keys)
✅ No NULL in goals, shots (required match stats)
✅ No NULL in wins, draws, losses (standings)
```

#### 3. Numeric Constraints

```python
✅ goals >= 0
✅ shots >= 0
✅ minutes >= 0
✅ games >= 0
✅ age between 15-45
```

#### 4. Percentage Constraints

```python
✅ possession <= 100
✅ shot_accuracy <= 100
✅ pass_accuracy <= 100
✅ win_percentage <= 100
```

#### 5. Logical Constraints

```python
✅ shots_on_target <= shots
✅ passes_completed <= passes_attempted
✅ goals <= shots (can't score more than you shoot)
✅ home_team_id ≠ away_team_id (no self-matches)
✅ home_possession + away_possession ≈ 100 (±2% tolerance)
```

#### 6. Business Rules

```python
✅ wins + draws + losses = played
✅ points = (wins × 3) + draws
✅ Rank is unique within each group (1-4)
✅ Expected row counts met:
   - 1,891 matches (1,826 league + 65 WC)
   - 130 teams (98 clubs + 32 national)
   - 681 players (World Cup squads)
   - 64 advanced stats (World Cup matches only)
```

### What Happens if Validation Fails?

```
❌ VALIDATION FAILED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
❌ home_sot > home_shots in 5 matches
❌ Negative home_goals found in 2 rows
❌ Matches reference non-existent team_ids: {999, 1001}

PIPELINE STOPPED - Fix data issues and re-run
```

**Pipeline exits with error code 1** and does NOT produce corrupted output.

---

## 🎨 Frontend Development (JSON Samples)

### Generated Files

Run `python scripts/export_json_samples.py` to create:

#### 1. `sample_matches.json` (20 matches)

```json
[
  {
    "matchId": 1,
    "competition": "Bundesliga",
    "season": "2022-23",
    "date": "2023-08-18",
    "time": "19:30:00",
    "homeTeam": {
      "teamId": 1,
      "goals": 0,
      "shots": 6,
      "shotsOnTarget": 1,
      "shotAccuracy": 16.67,
      "fouls": 16,
      "corners": 0,
      "yellowCards": 2,
      "redCards": 0
    },
    "awayTeam": {
      "teamId": 2,
      "goals": 4,
      "shots": 25,
      "shotsOnTarget": 10,
      "shotAccuracy": 40.0
    },
    "stats": {
      "goalDifference": -4,
      "totalGoals": 4,
      "totalCards": 3,
      "result": "A"
    },
    "venue": null,
    "referee": null
  }
]
```

#### 2. `sample_players.json` (30 top scorers)

```json
[
  {
    "playerId": 523,
    "name": "Lionel Messi",
    "teamId": 103,
    "team": "Argentina",
    "position": "FW",
    "age": 35.0,
    "stats": {
      "goals": 7,
      "assists": 3,
      "shots": 32,
      "shotsOnTarget": 21,
      "games": 7
    },
    "metrics": {
      "goalsPerGame": 1.0,
      "assistsPerGame": 0.43,
      "shotEfficiency": 21.88,
      "goalContributions": 10,
      "contributionsPerGame": 1.43
    }
  }
]
```

#### 3. `sample_standings.json` (8 groups)

```json
{
  "Group A": [
    {
      "rank": 1,
      "team": "Netherlands",
      "stats": {
        "played": 3,
        "wins": 2,
        "draws": 1,
        "losses": 0,
        "points": 7
      },
      "metrics": {
        "winPercentage": 66.67,
        "pointsPerGame": 2.33
      }
    }
  ]
}
```

#### 4. `sample_teams.json` (50 teams)

```json
[
  {
    "teamId": 2,
    "name": "Bayern Munich",
    "type": "league",
    "country": null,
    "competition": "Bundesliga"
  }
]
```

### React Usage Example

```jsx
import React from 'react';
import matches from './data/json_samples/sample_matches.json';
import players from './data/json_samples/sample_players.json';

function Dashboard() {
  return (
    <div>
      <h2>Top Scorers</h2>
      {players.slice(0, 10).map(p => (
        <div key={p.playerId}>
          {p.name}: {p.stats.goals} goals ({p.metrics.goalsPerGame.toFixed(2)} per game)
        </div>
      ))}
      
      <h2>Recent Matches</h2>
      {matches.map(m => (
        <div key={m.matchId}>
          Team {m.homeTeam.teamId} {m.homeTeam.goals} - {m.awayTeam.goals} Team {m.awayTeam.teamId}
        </div>
      ))}
    </div>
  );
}
```

**No backend required** - perfect for prototyping, demos, or offline development!

---

#### `ref_teams.csv`
Team dimension table with unique IDs:
- **team_id** (PK) - Unique team identifier
- team_name - Team display name
- competition_type - 'league' or 'international'
- country - Team's country
- primary_competition - Main competition name

#### `ref_players.csv`
Player dimension table with unique IDs:
- **player_id** (PK) - Unique player identifier
- player_name - Player display name
- **team_id** (FK) - Foreign key to teams
- position - Player position
- age - Player age

---

### **Fact Tables (Transactional Data)**

#### `db_matches_base_enhanced.csv`
Core match data (all competitions) - NO NULLs in advanced stats:
- **match_id** (PK) - Unique match identifier
- competition_name, season, date, time
- **home_team_id** (FK), **away_team_id** (FK) → ref_teams
- home_goals, away_goals
- home_shots, away_shots, home_sot, away_sot
- home_fouls, away_fouls, home_corners, away_corners
- home_yellow, away_yellow, home_red, away_red
- venue, referee
- **Derived:** goal_difference, total_goals, total_cards, home/away_shot_accuracy, result

#### `db_match_stats_advanced_enhanced.csv`
Advanced statistics (World Cup only) - avoids NULL hell:
- **match_id** (PK, FK) → db_matches_base
- home_xg, away_xg
- home_possession, away_possession
- home_passes_completed, home_passes_attempted
- away_passes_completed, away_passes_attempted
- home_tackles, away_tackles
- home_interceptions, away_interceptions
- home_clearances, away_clearances
- home_saves, away_saves
- **Derived:** home/away_pass_accuracy, possession_delta, xg_difference

#### `db_players_stats_enhanced.csv`
Player performance with derived metrics:
- **player_id** (FK) → ref_players
- **team_id** (FK) → ref_teams
- minutes, games, goals, assists
- shots, shots_on_target, passes_completed, passes, passes_pct
- tackles, interceptions, clearances, touches, dispossessed
- xg, xg_assist
- **Derived:** goals_per_game, assists_per_game, shot_efficiency, 
  sot_percentage, goal_contributions, contributions_per_game

#### `db_standings_enhanced.csv`
Group standings with performance metrics:
- group, rank, team
- played, wins, draws, losses
- goals_for, goals_against, goal_difference, points
- **Derived:** win_percentage, points_per_game, goals_per_game, clean_sheets

---

## 🏗️ Data Architecture

The pipeline implements **dimensional modeling** (star schema) for optimal database performance:

```
                    ┌─────────────────┐
                    │   ref_teams     │
                    │  (Dimension)    │
                    │  ─────────────  │
                    │  • team_id (PK) │
                    │  • team_name    │
                    │  • country      │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │                 │
    ┌───────────────▼──────┐  ┌──────▼───────────────┐
    │ db_matches_base       │  │  ref_players        │
    │     (Fact)            │  │  (Dimension)        │
    │ ──────────────────    │  │ ─────────────────   │
    │ • match_id (PK)       │  │ • player_id (PK)    │
    │ • home_team_id (FK) ──┼─►│ • team_id (FK)      │
    │ • away_team_id (FK) ──┼─►│ • player_name       │
    │ • date, goals, cards  │  │ • position, age     │
    └───────────┬───────────┘  └──────┬──────────────┘
                │                     │
                │                     │
   ┌────────────▼───────────┐    ┌───▼─────────────────┐
   │ db_match_stats_adv     │    │ db_players_stats    │
   │      (Fact)            │    │      (Fact)         │
   │ ───────────────────    │    │ ──────────────────  │
   │ • match_id (PK, FK)    │    │ • player_id (FK)    │
   │ • xG, possession       │    │ • goals, assists    │
   │ • passes, tackles      │    │ • shots, touches    │
   └────────────────────────┘    └─────────────────────┘
```

### Key Design Principles:
- **ID-Based Normalization**: Team and player names replaced with integer foreign keys
- **Fact-Dimension Separation**: Reference data (teams, players) stored once
- **NULL Avoidance**: Advanced stats split into separate table (World Cup only)
- **Derived Metrics**: Pre-calculated analytics (shot accuracy, xG deltas, etc.)
- **Database-First**: Schema designed for direct SQL import with proper joins

## ✨ Features

### 🎯 Production-Grade Architecture
✅ **Dimensional Modeling**: Star schema with fact and dimension tables  
✅ **ID-Based Normalization**: Foreign keys instead of string joins  
✅ **NULL Avoidance**: Advanced stats separated to prevent NULL hell  
✅ **Derived Metrics**: Pre-calculated analytics (SofaScore/FotMob style)

### 🚀 Engineering Excellence
✅ **Single Command Execution**: Run entire pipeline with one command  
✅ **Modular Design**: 9-step ETL with independent, testable modules  
✅ **Data Validation**: Comprehensive sanity checks and FK integrity validation  
✅ **Type Safety**: Proper data type conversions (int, float, date, time)  
✅ **Error Handling**: Comprehensive error messages and logging

### 📊 Database-Ready Output
✅ **Direct SQL Import**: Schema designed for PostgreSQL/MySQL/SQL Server  
✅ **Proper Joins**: Foreign key relationships with integer IDs  
✅ **snake_case Columns**: Consistent naming conventions  
✅ **Reproducible Results**: Same output every time

---

## 📋 Data Contract & Validation

### Data Contract Documentation

All output files follow a strict schema defined in **[DATA_CONTRACT.md](DATA_CONTRACT.md)**:

✅ Column names, types, and constraints clearly specified  
✅ Foreign key relationships documented  
✅ Business rules and validation criteria  
✅ No ambiguity - your teammate won't ask "what's in this column?"

**Example from contract:**
```
db_matches_base_enhanced.csv
├── match_id (INT, PRIMARY KEY)
├── home_team_id (INT, FK -> ref_teams.team_id)
├── away_team_id (INT, FK -> ref_teams.team_id)
├── home_goals (INT, >= 0, NOT NULL)
└── ...
```

### Automatic Validation

Pipeline includes **9th step: Data Validation** that checks:

🔍 **FK Integrity**: All team_id and player_id values exist in reference tables  
🔍 **No NULL violations**: Required fields are never NULL  
🔍 **No negative values**: Goals, shots, minutes always >= 0  
🔍 **Percentages ≤ 100**: Possession, accuracy never exceed 100%  
🔍 **Business rules**: `wins + draws + losses = played`, `home_team_id ≠ away_team_id`, etc.  
🔍 **Expected row counts**: 1,890 matches, 130 teams, 680 players  
🔍 **Logical constraints**: `shots_on_target <= shots`, `passes_completed <= passes_attempted`

**If validation fails, pipeline STOPS** - no silent data corruption.

---

## 🎨 Frontend Development (JSON Samples)

Generate sample JSON files for instant frontend development:

```powershell
python scripts/export_json_samples.py
```

**Output:**
- `data/json_samples/sample_matches.json` - 20 matches across competitions
- `data/json_samples/sample_players.json` - Top 30 players by goals
- `data/json_samples/sample_standings.json` - All 8 World Cup groups
- `data/json_samples/sample_teams.json` - 50 teams with metadata

**Usage in React:**
```javascript
import matches from './data/json_samples/sample_matches.json';
import players from './data/json_samples/sample_players.json';

// Start development immediately - no backend needed!
```

---

## 📋 Requirements

```bash
pandas
numpy
```

Install with:

```bash
pip install pandas numpy
```

## 🔧 Running Individual Steps

You can also run individual cleaning scripts:

```python
# Clean leagues only
python -m scripts.etl.clean_leagues

# Clean WC matches only
python -m scripts.etl.clean_wc_matches

# Clean WC standings only
python -m scripts.etl.clean_wc_standings

# Clean WC players only
python -m scripts.etl.clean_wc_players
```

## 📝 Data Transformation Rules

1. __Dates__: Converted to `YYYY-MM-DD` format
2. __Times__: Converted to `HH:MM:SS` format
3. __Team Names__: Trimmed and standardized
4. __Numeric Fields__: Properly typed as integers or floats
5. __Missing Data__: Nulls removed from critical fields
6. __Duplicates__: Removed based on key columns
7. __Column Names__: Consistent snake_case throughout

## 🎯 Schema Compliance

All output files are designed to match database schemas directly:

- Column names use `snake_case`
- No special characters or spaces
- Consistent naming conventions (e.g., `home_goals`, not `home_score`)
- Proper data types for direct import

## � Derived Metrics (Analytics Layer)

The pipeline automatically calculates advanced performance metrics:

### Match Metrics
- **Shot Accuracy**: (Shots on Target / Total Shots) × 100
- **Pass Accuracy**: (Completed Passes / Attempted Passes) × 100
- **Goal Difference**: Home goals - Away goals
- **Total Goals**: Combined score
- **Possession Delta**: Possession difference between teams
- **xG Difference**: Expected goals differential

### Player Metrics
- **Goals Per Game**: Goals / Games played
- **Assists Per Game**: Assists / Games played
- **Shot Efficiency**: (Goals / Shots) × 100
- **Shots on Target %**: (SoT / Shots) × 100
- **Goal Contributions**: Goals + Assists
- **Contributions Per Game**: (Goals + Assists) / Games

### Team Metrics
- **Win Percentage**: (Wins / Matches Played) × 100
- **Points Per Game**: Total Points / Matches Played
- **Goals Per Game**: Goals Scored / Matches Played
- **Clean Sheets**: Matches with zero goals conceded

## 📈 Future Enhancements

- ✅ ~~ID-based normalization~~ (Implemented)
- ✅ ~~Dimensional modeling~~ (Implemented)
- ✅ ~~Derived metrics~~ (Implemented)
- ⬜ SQL schema generator (CREATE TABLE statements)
- ⬜ Data quality dashboard with visualizations
- ⬜ Incremental updates for new seasons
- ⬜ API endpoints for live data ingestion
- ⬜ Machine learning feature engineering
- ⬜ Automated data profiling reports

---

__Ready for production. Ready for the database. One command.__

```bash
python scripts/run_pipeline.py
```
