"""
================================================================================
CONTEXT BLOCK
================================================================================
File: database.py
Module: brazilian_soccer_mcp.database
Purpose: Neo4j database connection, schema setup, and data loading

Description:
    Provides the data layer for the Brazilian Soccer MCP server. This module
    handles:
    1. Neo4j connection management
    2. Schema creation (constraints and indexes)
    3. Data loading from CSV files into the knowledge graph
    4. Entity deduplication and relationship creation

Neo4j Schema:
    Nodes:
        - (:Team {name, state, original_names})
        - (:Player {id, name, age, nationality, overall, ...})
        - (:Match {id, datetime, home_goals, away_goals, ...})
        - (:Competition {name, short_name, type})
        - (:Season {year})

    Relationships:
        - (Match)-[:HOME_TEAM]->(Team)
        - (Match)-[:AWAY_TEAM]->(Team)
        - (Match)-[:PART_OF]->(Competition)
        - (Match)-[:IN_SEASON]->(Season)
        - (Player)-[:PLAYS_FOR]->(Team)

Data Sources Loaded:
    1. Brasileirao_Matches.csv -> Brasileirao Serie A matches
    2. Brazilian_Cup_Matches.csv -> Copa do Brasil matches
    3. Libertadores_Matches.csv -> Copa Libertadores matches
    4. BR-Football-Dataset.csv -> Extended match statistics
    5. novo_campeonato_brasileiro.csv -> Historical Brasileirao 2003-2019
    6. fifa_data.csv -> FIFA player database

Created: 2025-12-14
================================================================================
"""

import os
import logging
from datetime import datetime
from typing import Optional, Generator
from contextlib import contextmanager

import pandas as pd
from neo4j import GraphDatabase, Driver, Session
from dateutil import parser as date_parser

from .models import (
    Team, Player, Match, Competition,
    normalize_team_name,
    BRASILEIRAO, COPA_DO_BRASIL, LIBERTADORES
)

logger = logging.getLogger(__name__)


class Neo4jConnection:
    """
    Manages Neo4j database connection and provides query execution methods.

    Usage:
        with Neo4jConnection(uri, user, password) as conn:
            conn.execute("MATCH (n) RETURN n LIMIT 10")
    """

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "password123"
    ):
        """
        Initialize Neo4j connection.

        Args:
            uri: Neo4j bolt URI
            user: Database username
            password: Database password
        """
        self.uri = uri
        self.user = user
        self.password = password
        self._driver: Optional[Driver] = None

    def connect(self) -> None:
        """Establish connection to Neo4j database."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password)
            )
            logger.info(f"Connected to Neo4j at {self.uri}")

    def close(self) -> None:
        """Close the database connection."""
        if self._driver:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j connection closed")

    def __enter__(self) -> "Neo4jConnection":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Get a database session context manager."""
        if not self._driver:
            self.connect()
        session = self._driver.session()
        try:
            yield session
        finally:
            session.close()

    def execute(self, query: str, parameters: dict = None) -> list:
        """
        Execute a Cypher query and return results as a list.

        Args:
            query: Cypher query string
            parameters: Query parameters

        Returns:
            List of result records as dictionaries
        """
        with self.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]

    def execute_write(self, query: str, parameters: dict = None) -> None:
        """
        Execute a write transaction.

        Args:
            query: Cypher query string
            parameters: Query parameters
        """
        with self.session() as session:
            session.execute_write(
                lambda tx: tx.run(query, parameters or {})
            )

    def setup_schema(self) -> None:
        """Create indexes and constraints for the knowledge graph schema."""
        schema_queries = [
            # Constraints (also create indexes)
            "CREATE CONSTRAINT team_name IF NOT EXISTS FOR (t:Team) REQUIRE t.name IS UNIQUE",
            "CREATE CONSTRAINT player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT match_id IF NOT EXISTS FOR (m:Match) REQUIRE m.id IS UNIQUE",
            "CREATE CONSTRAINT competition_name IF NOT EXISTS FOR (c:Competition) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT season_year IF NOT EXISTS FOR (s:Season) REQUIRE s.year IS UNIQUE",

            # Additional indexes for common queries
            "CREATE INDEX team_state IF NOT EXISTS FOR (t:Team) ON (t.state)",
            "CREATE INDEX player_nationality IF NOT EXISTS FOR (p:Player) ON (p.nationality)",
            "CREATE INDEX player_club IF NOT EXISTS FOR (p:Player) ON (p.club)",
            "CREATE INDEX player_overall IF NOT EXISTS FOR (p:Player) ON (p.overall)",
            "CREATE INDEX match_datetime IF NOT EXISTS FOR (m:Match) ON (m.datetime)",
            "CREATE INDEX match_season IF NOT EXISTS FOR (m:Match) ON (m.season)",
            "CREATE INDEX match_competition IF NOT EXISTS FOR (m:Match) ON (m.competition)",
        ]

        for query in schema_queries:
            try:
                self.execute_write(query)
                logger.debug(f"Executed schema query: {query[:50]}...")
            except Exception as e:
                logger.warning(f"Schema query failed (may already exist): {e}")

        logger.info("Schema setup complete")

    def clear_database(self) -> None:
        """Remove all nodes and relationships from the database."""
        self.execute_write("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared")


class DataLoader:
    """
    Loads Brazilian soccer data from CSV files into Neo4j knowledge graph.

    The loader handles:
    - Multiple date formats across different data sources
    - Team name normalization and deduplication
    - Match deduplication using composite keys
    - Relationship creation between entities
    """

    def __init__(self, connection: Neo4jConnection, data_dir: str = "data/kaggle"):
        """
        Initialize the data loader.

        Args:
            connection: Neo4jConnection instance
            data_dir: Path to directory containing CSV files
        """
        self.conn = connection
        self.data_dir = data_dir
        self._teams_cache: dict[str, Team] = {}

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse date string handling multiple formats.

        Handles:
        - ISO format: "2023-09-24"
        - Brazilian format: "29/03/2003"
        - With time: "2012-05-19 18:30:00"

        Args:
            date_str: Date string in various formats

        Returns:
            datetime object or None if parsing fails
        """
        if pd.isna(date_str) or not date_str:
            return None

        try:
            # Try common formats
            return date_parser.parse(str(date_str), dayfirst=True)
        except Exception:
            logger.warning(f"Failed to parse date: {date_str}")
            return None

    def _safe_int(self, value, default: int = 0) -> int:
        """Safely convert value to int, handling NaN and None."""
        if pd.isna(value) or value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _get_or_create_team(self, name: str, state: str = None) -> str:
        """
        Get or create a team, returning the normalized name.

        Args:
            name: Team name (will be normalized)
            state: State abbreviation

        Returns:
            Normalized team name
        """
        normalized = normalize_team_name(name)
        if normalized not in self._teams_cache:
            self._teams_cache[normalized] = Team(
                name=normalized,
                state=state,
                original_names={name}
            )
        else:
            self._teams_cache[normalized].add_original_name(name)

        return normalized

    def _generate_match_id(
        self,
        datetime_obj: datetime,
        home: str,
        away: str,
        competition: str
    ) -> str:
        """Generate unique match ID from composite key."""
        date_str = datetime_obj.strftime("%Y%m%d") if datetime_obj else "unknown"
        home_norm = normalize_team_name(home)[:10]
        away_norm = normalize_team_name(away)[:10]
        comp_short = competition[:5]
        return f"{date_str}_{home_norm}_{away_norm}_{comp_short}"

    def load_all(self) -> dict:
        """
        Load all CSV data into Neo4j.

        Returns:
            Dictionary with counts of loaded entities
        """
        stats = {
            "teams": 0,
            "players": 0,
            "matches": 0,
            "competitions": 0,
        }

        # Create competitions first
        self._create_competitions()
        stats["competitions"] = 3

        # Load match data from all sources
        stats["matches"] += self._load_brasileirao_matches()
        stats["matches"] += self._load_copa_brasil_matches()
        stats["matches"] += self._load_libertadores_matches()
        stats["matches"] += self._load_historical_brasileirao()
        stats["matches"] += self._load_extended_statistics()

        # Create team nodes from cache
        self._create_teams()
        stats["teams"] = len(self._teams_cache)

        # Load player data
        stats["players"] = self._load_fifa_players()

        logger.info(f"Data loading complete: {stats}")
        return stats

    def _create_competitions(self) -> None:
        """Create competition nodes."""
        for comp in [BRASILEIRAO, COPA_DO_BRASIL, LIBERTADORES]:
            query = """
            MERGE (c:Competition {name: $name})
            SET c.short_name = $short_name,
                c.country = $country,
                c.competition_type = $competition_type
            """
            self.conn.execute_write(query, comp.to_dict())
        logger.info("Created competition nodes")

    def _create_teams(self) -> None:
        """Create team nodes from accumulated cache."""
        query = """
        MERGE (t:Team {name: $name})
        SET t.state = $state,
            t.original_names = $original_names
        """
        for team in self._teams_cache.values():
            self.conn.execute_write(query, team.to_dict())
        logger.info(f"Created {len(self._teams_cache)} team nodes")

    def _load_brasileirao_matches(self) -> int:
        """Load Brasileirao Serie A matches."""
        filepath = os.path.join(self.data_dir, "Brasileirao_Matches.csv")
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return 0

        df = pd.read_csv(filepath)
        count = 0

        for _, row in df.iterrows():
            dt = self._parse_date(row.get('datetime'))
            home = str(row.get('home_team', ''))
            away = str(row.get('away_team', ''))

            if not home or not away:
                continue

            # Track teams
            self._get_or_create_team(home, row.get('home_team_state'))
            self._get_or_create_team(away, row.get('away_team_state'))

            match = Match(
                id=self._generate_match_id(dt, home, away, "Brasileirao"),
                datetime=dt,
                home_team=home,
                away_team=away,
                home_goals=self._safe_int(row.get('home_goal')),
                away_goals=self._safe_int(row.get('away_goal')),
                competition=BRASILEIRAO.name,
                season=self._safe_int(row.get('season')) if pd.notna(row.get('season')) else None,
                round=str(row.get('round')) if pd.notna(row.get('round')) else None,
            )

            self._create_match_node(match)
            count += 1

        logger.info(f"Loaded {count} Brasileirao matches")
        return count

    def _load_copa_brasil_matches(self) -> int:
        """Load Copa do Brasil matches."""
        filepath = os.path.join(self.data_dir, "Brazilian_Cup_Matches.csv")
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return 0

        df = pd.read_csv(filepath)
        count = 0

        for _, row in df.iterrows():
            dt = self._parse_date(row.get('datetime'))
            home = str(row.get('home_team', ''))
            away = str(row.get('away_team', ''))

            if not home or not away:
                continue

            self._get_or_create_team(home)
            self._get_or_create_team(away)

            match = Match(
                id=self._generate_match_id(dt, home, away, "CopaBrasil"),
                datetime=dt,
                home_team=home,
                away_team=away,
                home_goals=self._safe_int(row.get('home_goal')),
                away_goals=self._safe_int(row.get('away_goal')),
                competition=COPA_DO_BRASIL.name,
                season=self._safe_int(row.get('season')) if pd.notna(row.get('season')) else None,
                round=str(row.get('round')) if pd.notna(row.get('round')) else None,
            )

            self._create_match_node(match)
            count += 1

        logger.info(f"Loaded {count} Copa do Brasil matches")
        return count

    def _load_libertadores_matches(self) -> int:
        """Load Copa Libertadores matches."""
        filepath = os.path.join(self.data_dir, "Libertadores_Matches.csv")
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return 0

        df = pd.read_csv(filepath)
        count = 0

        for _, row in df.iterrows():
            dt = self._parse_date(row.get('datetime'))
            home = str(row.get('home_team', ''))
            away = str(row.get('away_team', ''))

            if not home or not away:
                continue

            self._get_or_create_team(home)
            self._get_or_create_team(away)

            match = Match(
                id=self._generate_match_id(dt, home, away, "Libertadores"),
                datetime=dt,
                home_team=home,
                away_team=away,
                home_goals=self._safe_int(row.get('home_goal')),
                away_goals=self._safe_int(row.get('away_goal')),
                competition=LIBERTADORES.name,
                season=self._safe_int(row.get('season')) if pd.notna(row.get('season')) else None,
                round=str(row.get('stage')) if pd.notna(row.get('stage')) else None,
            )

            self._create_match_node(match)
            count += 1

        logger.info(f"Loaded {count} Libertadores matches")
        return count

    def _load_historical_brasileirao(self) -> int:
        """Load historical Brasileirao data (2003-2019)."""
        filepath = os.path.join(self.data_dir, "novo_campeonato_brasileiro.csv")
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return 0

        df = pd.read_csv(filepath)
        count = 0

        for _, row in df.iterrows():
            dt = self._parse_date(row.get('Data'))
            home = str(row.get('Equipe_mandante', ''))
            away = str(row.get('Equipe_visitante', ''))

            if not home or not away:
                continue

            self._get_or_create_team(home, row.get('Mandante_UF'))
            self._get_or_create_team(away, row.get('Visitante_UF'))

            match = Match(
                id=self._generate_match_id(dt, home, away, "BrasHist"),
                datetime=dt,
                home_team=home,
                away_team=away,
                home_goals=self._safe_int(row.get('Gols_mandante')),
                away_goals=self._safe_int(row.get('Gols_visitante')),
                competition=BRASILEIRAO.name,
                season=self._safe_int(row.get('Ano')) if pd.notna(row.get('Ano')) else None,
                round=str(row.get('Rodada')) if pd.notna(row.get('Rodada')) else None,
                stadium=str(row.get('Arena')) if pd.notna(row.get('Arena')) else None,
            )

            self._create_match_node(match)
            count += 1

        logger.info(f"Loaded {count} historical Brasileirao matches")
        return count

    def _load_extended_statistics(self) -> int:
        """Load extended match statistics dataset."""
        filepath = os.path.join(self.data_dir, "BR-Football-Dataset.csv")
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return 0

        df = pd.read_csv(filepath)
        count = 0

        for _, row in df.iterrows():
            dt = self._parse_date(row.get('date'))
            home = str(row.get('home', ''))
            away = str(row.get('away', ''))
            tournament = str(row.get('tournament', 'Unknown'))

            if not home or not away:
                continue

            self._get_or_create_team(home)
            self._get_or_create_team(away)

            # Map tournament to competition
            comp_name = BRASILEIRAO.name
            if 'copa' in tournament.lower() and 'brasil' in tournament.lower():
                comp_name = COPA_DO_BRASIL.name
            elif 'libertadores' in tournament.lower():
                comp_name = LIBERTADORES.name

            statistics = {}
            for stat in ['home_corner', 'away_corner', 'home_attack', 'away_attack',
                         'home_shots', 'away_shots', 'total_corners']:
                if pd.notna(row.get(stat)):
                    statistics[stat] = self._safe_int(row.get(stat))

            match = Match(
                id=self._generate_match_id(dt, home, away, "Extended"),
                datetime=dt,
                home_team=home,
                away_team=away,
                home_goals=self._safe_int(row.get('home_goal')),
                away_goals=self._safe_int(row.get('away_goal')),
                competition=comp_name,
                statistics=statistics,
            )

            self._create_match_node(match)
            count += 1

        logger.info(f"Loaded {count} extended statistics matches")
        return count

    def _load_fifa_players(self) -> int:
        """Load FIFA player database."""
        filepath = os.path.join(self.data_dir, "fifa_data.csv")
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return 0

        df = pd.read_csv(filepath)
        count = 0

        # Process in batches for efficiency
        batch_size = 500
        batch = []

        for _, row in df.iterrows():
            player_id = row.get('ID')
            if pd.isna(player_id):
                continue

            player = Player(
                id=self._safe_int(player_id),
                name=str(row.get('Name', '')),
                age=self._safe_int(row.get('Age')) if pd.notna(row.get('Age')) else None,
                nationality=str(row.get('Nationality')) if pd.notna(row.get('Nationality')) else None,
                overall=self._safe_int(row.get('Overall')) if pd.notna(row.get('Overall')) else None,
                potential=self._safe_int(row.get('Potential')) if pd.notna(row.get('Potential')) else None,
                club=str(row.get('Club')) if pd.notna(row.get('Club')) else None,
                position=str(row.get('Position')) if pd.notna(row.get('Position')) else None,
            )

            batch.append(player.to_dict())

            if len(batch) >= batch_size:
                self._create_player_batch(batch)
                count += len(batch)
                batch = []

        # Create remaining players
        if batch:
            self._create_player_batch(batch)
            count += len(batch)

        logger.info(f"Loaded {count} players")
        return count

    def _create_match_node(self, match: Match) -> None:
        """Create a match node with relationships."""
        query = """
        MERGE (m:Match {id: $id})
        SET m.datetime = $datetime,
            m.home_team = $home_team,
            m.away_team = $away_team,
            m.home_goals = $home_goals,
            m.away_goals = $away_goals,
            m.competition = $competition,
            m.season = $season,
            m.round = $round,
            m.stadium = $stadium,
            m.result = $result,
            m.total_goals = $total_goals

        WITH m
        MATCH (ht:Team {name: $home_team})
        MERGE (m)-[:HOME_TEAM]->(ht)

        WITH m
        MATCH (at:Team {name: $away_team})
        MERGE (m)-[:AWAY_TEAM]->(at)

        WITH m
        MATCH (c:Competition {name: $competition})
        MERGE (m)-[:PART_OF]->(c)
        """

        # Create season node if season is specified
        if match.season:
            query += """
            WITH m
            MERGE (s:Season {year: $season})
            MERGE (m)-[:IN_SEASON]->(s)
            """

        self.conn.execute_write(query, match.to_dict())

    def _create_player_batch(self, players: list[dict]) -> None:
        """Create player nodes in batch."""
        query = """
        UNWIND $players AS player
        MERGE (p:Player {id: player.id})
        SET p.name = player.name,
            p.age = player.age,
            p.nationality = player.nationality,
            p.overall = player.overall,
            p.potential = player.potential,
            p.club = player.club,
            p.position = player.position,
            p.jersey_number = player.jersey_number,
            p.height_cm = player.height_cm,
            p.weight_kg = player.weight_kg,
            p.preferred_foot = player.preferred_foot
        """
        self.conn.execute_write(query, {"players": players})
