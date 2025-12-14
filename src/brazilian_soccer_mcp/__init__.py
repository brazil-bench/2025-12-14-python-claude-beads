"""
================================================================================
CONTEXT BLOCK
================================================================================
File: __init__.py
Module: brazilian_soccer_mcp
Purpose: Package initialization for Brazilian Soccer MCP Server

Description:
    This package provides an MCP (Model Context Protocol) server that exposes
    Brazilian soccer data through a Neo4j knowledge graph. The server enables
    natural language queries about players, teams, matches, and competitions.

Data Sources:
    - Brasileirao_Matches.csv: Serie A matches (4,180 records)
    - Brazilian_Cup_Matches.csv: Copa do Brasil (1,337 matches)
    - Libertadores_Matches.csv: Copa Libertadores (1,255 matches)
    - BR-Football-Dataset.csv: Extended match statistics (10,296 matches)
    - novo_campeonato_brasileiro.csv: Historical data 2003-2019 (6,886 matches)
    - fifa_data.csv: FIFA player database (18,207 players)

Architecture:
    - database.py: Neo4j connection, schema setup, data loading
    - models.py: Data models for teams, players, matches
    - queries.py: Cypher query builders and executors
    - server.py: MCP server with tool definitions

Created: 2025-12-14
================================================================================
"""

__version__ = "0.1.0"
__author__ = "Claude Code"

from .database import Neo4jConnection, DataLoader
from .models import Team, Player, Match, Competition

__all__ = [
    "Neo4jConnection",
    "DataLoader",
    "Team",
    "Player",
    "Match",
    "Competition",
]
