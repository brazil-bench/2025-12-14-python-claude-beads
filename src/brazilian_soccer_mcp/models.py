"""
================================================================================
CONTEXT BLOCK
================================================================================
File: models.py
Module: brazilian_soccer_mcp.models
Purpose: Data models for Brazilian Soccer knowledge graph entities

Description:
    Defines dataclass models representing the core entities in the Brazilian
    soccer domain: Teams, Players, Matches, and Competitions. These models
    are used for data validation, serialization, and as a contract between
    the data loading and query layers.

Entity Relationships in Neo4j:
    (Team)-[:PLAYS_IN]->(Competition)
    (Player)-[:PLAYS_FOR]->(Team)
    (Match)-[:HOME_TEAM]->(Team)
    (Match)-[:AWAY_TEAM]->(Team)
    (Match)-[:PART_OF]->(Competition)
    (Match)-[:IN_SEASON]->(Season)

Team Name Normalization:
    Brazilian team names appear in multiple formats across data sources:
    - With state suffix: "Palmeiras-SP", "Flamengo-RJ"
    - Without suffix: "Palmeiras", "Flamengo"
    - Full names: "Sport Club Corinthians Paulista"

    The normalize_team_name() function handles these variations.

Created: 2025-12-14
================================================================================
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from unidecode import unidecode
import re


def normalize_team_name(name: str) -> str:
    """
    Normalize team name for consistent matching across datasets.

    Handles:
    - State suffixes (e.g., "Palmeiras-SP" -> "Palmeiras")
    - Accented characters (e.g., "Grêmio" -> "Gremio")
    - Extra whitespace
    - Common abbreviations

    Args:
        name: Raw team name from dataset

    Returns:
        Normalized team name suitable for matching
    """
    if not name:
        return ""

    # Remove state suffix pattern (e.g., "-SP", "-RJ")
    name = re.sub(r'-[A-Z]{2}$', '', name.strip())

    # Remove accents for consistent matching
    name = unidecode(name)

    # Normalize whitespace
    name = ' '.join(name.split())

    # Common team name mappings for consistency
    team_mappings = {
        "Atletico Mineiro": "Atletico-MG",
        "Atletico-MG": "Atletico-MG",
        "Atletico MG": "Atletico-MG",
        "Atletico Paranaense": "Athletico-PR",
        "Athletico Paranaense": "Athletico-PR",
        "Athletico-PR": "Athletico-PR",
        "Atletico Goianiense": "Atletico-GO",
        "Sport Club Corinthians Paulista": "Corinthians",
        "Sociedade Esportiva Palmeiras": "Palmeiras",
        "Sao Paulo FC": "Sao Paulo",
        "Sao Paulo Futebol Clube": "Sao Paulo",
    }

    return team_mappings.get(name, name)


@dataclass
class Team:
    """
    Represents a soccer team in the knowledge graph.

    Attributes:
        name: Canonical team name (normalized)
        original_names: Set of original name variations found in data
        state: Brazilian state abbreviation (e.g., "SP", "RJ")
        founded: Year team was founded (if available)
    """
    name: str
    original_names: set = field(default_factory=set)
    state: Optional[str] = None
    founded: Optional[int] = None

    def __post_init__(self):
        self.name = normalize_team_name(self.name)

    def add_original_name(self, name: str) -> None:
        """Track an original name variation for this team."""
        self.original_names.add(name)

    def to_dict(self) -> dict:
        """Convert to dictionary for Neo4j node properties."""
        return {
            "name": self.name,
            "original_names": list(self.original_names),
            "state": self.state,
            "founded": self.founded,
        }


@dataclass
class Player:
    """
    Represents a soccer player from FIFA dataset.

    Attributes:
        id: Unique FIFA player ID
        name: Player's full name
        age: Player's age
        nationality: Country of origin
        overall: FIFA overall rating (0-99)
        potential: FIFA potential rating (0-99)
        club: Current club name
        position: Primary playing position
        jersey_number: Shirt number
        height_cm: Height in centimeters
        weight_kg: Weight in kilograms
        preferred_foot: Left or Right
        skill_ratings: Dictionary of specific skill ratings
    """
    id: int
    name: str
    age: Optional[int] = None
    nationality: Optional[str] = None
    overall: Optional[int] = None
    potential: Optional[int] = None
    club: Optional[str] = None
    position: Optional[str] = None
    jersey_number: Optional[int] = None
    height_cm: Optional[int] = None
    weight_kg: Optional[int] = None
    preferred_foot: Optional[str] = None
    skill_ratings: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for Neo4j node properties."""
        return {
            "id": self.id,
            "name": self.name,
            "age": self.age,
            "nationality": self.nationality,
            "overall": self.overall,
            "potential": self.potential,
            "club": normalize_team_name(self.club) if self.club else None,
            "position": self.position,
            "jersey_number": self.jersey_number,
            "height_cm": self.height_cm,
            "weight_kg": self.weight_kg,
            "preferred_foot": self.preferred_foot,
        }


@dataclass
class Match:
    """
    Represents a soccer match.

    Attributes:
        id: Unique match identifier (generated if not in data)
        datetime: Match date and time
        home_team: Home team name (normalized)
        away_team: Away team name (normalized)
        home_goals: Goals scored by home team
        away_goals: Goals scored by away team
        competition: Competition name
        season: Season year
        round: Round number or stage name
        stadium: Stadium/arena name (if available)
        statistics: Extended match statistics (if available)
    """
    id: str
    datetime: datetime
    home_team: str
    away_team: str
    home_goals: int
    away_goals: int
    competition: str
    season: Optional[int] = None
    round: Optional[str] = None
    stadium: Optional[str] = None
    statistics: dict = field(default_factory=dict)

    def __post_init__(self):
        self.home_team = normalize_team_name(self.home_team)
        self.away_team = normalize_team_name(self.away_team)

    @property
    def result(self) -> str:
        """Returns match result: 'home_win', 'away_win', or 'draw'."""
        if self.home_goals > self.away_goals:
            return "home_win"
        elif self.away_goals > self.home_goals:
            return "away_win"
        return "draw"

    @property
    def total_goals(self) -> int:
        """Returns total goals scored in match."""
        return self.home_goals + self.away_goals

    def to_dict(self) -> dict:
        """Convert to dictionary for Neo4j node properties."""
        return {
            "id": self.id,
            "datetime": self.datetime.isoformat() if self.datetime else None,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "home_goals": self.home_goals,
            "away_goals": self.away_goals,
            "competition": self.competition,
            "season": self.season,
            "round": self.round,
            "stadium": self.stadium,
            "result": self.result,
            "total_goals": self.total_goals,
        }


@dataclass
class Competition:
    """
    Represents a soccer competition/tournament.

    Attributes:
        name: Competition name (e.g., "Brasileirao Serie A")
        short_name: Abbreviated name (e.g., "Brasileirao")
        country: Country (Brazil for domestic, International for Libertadores)
        competition_type: 'league' or 'cup'
    """
    name: str
    short_name: Optional[str] = None
    country: str = "Brazil"
    competition_type: str = "league"

    def to_dict(self) -> dict:
        """Convert to dictionary for Neo4j node properties."""
        return {
            "name": self.name,
            "short_name": self.short_name or self.name,
            "country": self.country,
            "competition_type": self.competition_type,
        }


# Pre-defined competition instances
BRASILEIRAO = Competition(
    name="Brasileirao Serie A",
    short_name="Brasileirao",
    country="Brazil",
    competition_type="league"
)

COPA_DO_BRASIL = Competition(
    name="Copa do Brasil",
    short_name="Copa do Brasil",
    country="Brazil",
    competition_type="cup"
)

LIBERTADORES = Competition(
    name="Copa Libertadores",
    short_name="Libertadores",
    country="International",
    competition_type="cup"
)
