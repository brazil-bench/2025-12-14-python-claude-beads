# Brazilian Soccer MCP Server

An MCP (Model Context Protocol) server that provides a knowledge graph interface for Brazilian soccer data using Neo4j. The server enables natural language queries about players, teams, matches, and competitions.

## Features

### Phase 1: Neo4j Data Layer
- Neo4j knowledge graph with schema setup (constraints and indexes)
- Data loading from 6 CSV files containing:
  - 666 teams
  - 18,207 FIFA players
  - 23,954 matches across 3 competitions
- Team name normalization for consistent matching
- Multiple date format handling

### Phase 2: MCP Server with Query Tools
- **Match Queries**
  - `find_matches`: Search by team, season, competition
  - `get_head_to_head`: Get matches between two teams with statistics

- **Team Queries**
  - `get_team_stats`: Comprehensive statistics (overall, home, away)
  - `list_teams`: List all teams in database

- **Player Queries**
  - `find_players`: Search by name, nationality, club, position, rating
  - `get_top_players`: Top-rated players with filters

### Phase 3: Advanced Analytics
- **Competition Analysis**
  - `get_standings`: Calculate league standings for a season
  - `get_league_stats`: Aggregate statistics (goals, win rates)
  - `get_competition_winners`: Historical winners
  - `get_biggest_wins`: Matches with largest margins

## Installation

```bash
# Install dependencies
pip install -e ".[dev]"

# Start Neo4j (Docker)
docker run -d --name neo4j-soccer \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password123 \
  neo4j:latest

# Initialize database with data
python -c "from brazilian_soccer_mcp.server import initialize_database; initialize_database()"
```

## Usage

### Start the MCP Server

```bash
# Initialize and start
brazilian-soccer-mcp --init

# Or just start (if already initialized)
brazilian-soccer-mcp
```

### Example Queries

```python
from brazilian_soccer_mcp.database import Neo4jConnection
from brazilian_soccer_mcp.queries import QueryExecutor

conn = Neo4jConnection()
conn.connect()
executor = QueryExecutor(conn)

# Head-to-head between rivals
result = executor.find_matches_between_teams("Flamengo", "Fluminense")
print(f"Total matches: {result['total_matches']}")
print(f"Flamengo wins: {result['team1_wins']}")

# Team statistics
stats = executor.get_team_statistics("Palmeiras", season=2019)
print(f"Win rate: {stats['overall']['win_rate']}%")

# Top Brazilian players
players = executor.get_top_players_by_rating(nationality="Brazil", limit=5)
for p in players['players']:
    print(f"{p['name']} - Overall: {p['overall']}")
```

## Testing

Tests use BDD (Behavior-Driven Development) with pytest-bdd:

```bash
# Run all tests
pytest tests/ -v

# Run specific test category
pytest tests/test_match_queries.py -v
pytest tests/test_player_queries.py -v
pytest tests/test_analytics.py -v
```

### Test Coverage

- **18 BDD scenarios** covering:
  - Match queries (head-to-head, filtering, home/away)
  - Team statistics (comprehensive stats, season filtering)
  - Player searches (nationality, club, ratings)
  - Analytics (standings, biggest wins, league stats, winners)

## Project Structure

```
brazilian-soccer-mcp/
├── src/brazilian_soccer_mcp/
│   ├── __init__.py      # Package exports
│   ├── database.py      # Neo4j connection and data loading
│   ├── models.py        # Data models (Team, Player, Match, Competition)
│   ├── queries.py       # Query builders and executors
│   └── server.py        # MCP server implementation
├── tests/
│   ├── features/        # BDD feature files
│   │   ├── match_queries.feature
│   │   ├── team_queries.feature
│   │   ├── player_queries.feature
│   │   └── analytics.feature
│   ├── conftest.py      # Test fixtures
│   ├── test_match_queries.py
│   ├── test_team_queries.py
│   ├── test_player_queries.py
│   └── test_analytics.py
├── data/kaggle/         # CSV data files
└── pyproject.toml       # Project configuration
```

## Data Sources

All data is freely available under various open licenses:

| File | Records | Source | License |
|------|---------|--------|---------|
| Brasileirao_Matches.csv | 4,180 | [Kaggle](https://www.kaggle.com/datasets/ricardomattos05/jogos-do-campeonato-brasileiro) | CC BY 4.0 |
| Brazilian_Cup_Matches.csv | 1,337 | [Kaggle](https://www.kaggle.com/datasets/ricardomattos05/jogos-do-campeonato-brasileiro) | CC BY 4.0 |
| Libertadores_Matches.csv | 1,255 | [Kaggle](https://www.kaggle.com/datasets/ricardomattos05/jogos-do-campeonato-brasileiro) | CC BY 4.0 |
| BR-Football-Dataset.csv | 10,296 | [Kaggle](https://www.kaggle.com/datasets/cuecacuela/brazilian-football-matches) | CC0 Public Domain |
| novo_campeonato_brasileiro.csv | 6,886 | [Kaggle](https://www.kaggle.com/datasets/macedojleo/campeonato-brasileiro-2003-a-2019) | CC BY 4.0 |
| fifa_data.csv | 18,207 | [Kaggle](https://www.kaggle.com/datasets/youssefelbadry10/fifa-players-data) | Apache 2.0 |

## Neo4j Schema

### Nodes
- `Team`: name, state, original_names
- `Player`: id, name, age, nationality, overall, potential, club, position
- `Match`: id, datetime, home_team, away_team, home_goals, away_goals, competition, season, round, result
- `Competition`: name, short_name, country, competition_type
- `Season`: year

### Relationships
- `(Match)-[:HOME_TEAM]->(Team)`
- `(Match)-[:AWAY_TEAM]->(Team)`
- `(Match)-[:PART_OF]->(Competition)`
- `(Match)-[:IN_SEASON]->(Season)`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| NEO4J_URI | bolt://localhost:7687 | Neo4j connection URI |
| NEO4J_USER | neo4j | Database username |
| NEO4J_PASSWORD | password123 | Database password |

## Specification

See [brazilian-soccer-mcp-guide.md](brazilian-soccer-mcp-guide.md) for the full specification document.
