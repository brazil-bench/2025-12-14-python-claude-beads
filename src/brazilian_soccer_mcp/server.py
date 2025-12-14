"""
================================================================================
CONTEXT BLOCK
================================================================================
File: server.py
Module: brazilian_soccer_mcp.server
Purpose: MCP Server implementation for Brazilian Soccer knowledge graph

Description:
    Implements the Model Context Protocol (MCP) server that exposes Brazilian
    soccer data through a set of tools. The server connects to a Neo4j database
    containing match, team, player, and competition data.

MCP Tools Provided:
    Phase 1 (Match Queries):
        - find_matches: Search matches by team, date, competition
        - get_head_to_head: Get matches between two teams

    Phase 2 (Team & Player Queries):
        - get_team_stats: Comprehensive team statistics
        - find_players: Search player database
        - get_top_players: Top-rated players by criteria

    Phase 3 (Analytics):
        - get_standings: League standings for a season
        - get_biggest_wins: Matches with largest margins
        - get_league_stats: Aggregate competition statistics
        - get_competition_winners: Historical winners

Usage:
    # Start server directly
    python -m brazilian_soccer_mcp.server

    # Or via entry point
    brazilian-soccer-mcp

Environment Variables:
    NEO4J_URI: Neo4j connection URI (default: bolt://localhost:7687)
    NEO4J_USER: Database username (default: neo4j)
    NEO4J_PASSWORD: Database password (default: password123)

Created: 2025-12-14
================================================================================
"""

import os
import sys
import json
import logging
import asyncio
from typing import Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from .database import Neo4jConnection, DataLoader
from .queries import QueryExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Server instance
server = Server("brazilian-soccer-mcp")

# Global connection and executor (initialized on startup)
_connection: Optional[Neo4jConnection] = None
_executor: Optional[QueryExecutor] = None


def get_connection() -> Neo4jConnection:
    """Get or create Neo4j connection."""
    global _connection
    if _connection is None:
        _connection = Neo4jConnection(
            uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            user=os.environ.get("NEO4J_USER", "neo4j"),
            password=os.environ.get("NEO4J_PASSWORD", "password123")
        )
        _connection.connect()
    return _connection


def get_executor() -> QueryExecutor:
    """Get or create query executor."""
    global _executor
    if _executor is None:
        _executor = QueryExecutor(get_connection())
    return _executor


def format_result(data: dict) -> str:
    """Format query result for readable output."""
    return json.dumps(data, indent=2, default=str)


# =============================================================================
# TOOL DEFINITIONS
# =============================================================================

@server.list_tools()
async def list_tools() -> list[Tool]:
    """List all available tools."""
    return [
        # Phase 1: Match Queries
        Tool(
            name="find_matches",
            description="Find soccer matches by team, competition, or season. "
                       "Use this to search for specific matches or get a team's match history.",
            inputSchema={
                "type": "object",
                "properties": {
                    "team": {
                        "type": "string",
                        "description": "Team name to search for (e.g., 'Flamengo', 'Palmeiras')"
                    },
                    "season": {
                        "type": "integer",
                        "description": "Season year (e.g., 2023)"
                    },
                    "competition": {
                        "type": "string",
                        "description": "Competition name (e.g., 'Brasileirao', 'Copa do Brasil', 'Libertadores')"
                    },
                    "home_only": {
                        "type": "boolean",
                        "description": "Only return home matches"
                    },
                    "away_only": {
                        "type": "boolean",
                        "description": "Only return away matches"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of matches to return (default: 50)"
                    }
                },
                "required": ["team"]
            }
        ),
        Tool(
            name="get_head_to_head",
            description="Get head-to-head record between two teams, including all matches "
                       "and statistics like wins, draws, and goals.",
            inputSchema={
                "type": "object",
                "properties": {
                    "team1": {
                        "type": "string",
                        "description": "First team name"
                    },
                    "team2": {
                        "type": "string",
                        "description": "Second team name"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of matches to return (default: 50)"
                    }
                },
                "required": ["team1", "team2"]
            }
        ),

        # Phase 2: Team & Player Queries
        Tool(
            name="get_team_stats",
            description="Get comprehensive statistics for a team, including overall record, "
                       "home/away splits, goals, and win rates.",
            inputSchema={
                "type": "object",
                "properties": {
                    "team": {
                        "type": "string",
                        "description": "Team name"
                    },
                    "season": {
                        "type": "integer",
                        "description": "Filter by season year"
                    },
                    "competition": {
                        "type": "string",
                        "description": "Filter by competition"
                    }
                },
                "required": ["team"]
            }
        ),
        Tool(
            name="find_players",
            description="Search for players by name, nationality, club, position, or FIFA rating. "
                       "Useful for finding Brazilian players or players at Brazilian clubs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Player name (partial match)"
                    },
                    "nationality": {
                        "type": "string",
                        "description": "Player nationality (e.g., 'Brazil')"
                    },
                    "club": {
                        "type": "string",
                        "description": "Club name"
                    },
                    "position": {
                        "type": "string",
                        "description": "Playing position (e.g., 'ST', 'GK', 'CB')"
                    },
                    "min_overall": {
                        "type": "integer",
                        "description": "Minimum FIFA overall rating"
                    },
                    "max_overall": {
                        "type": "integer",
                        "description": "Maximum FIFA overall rating"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 50)"
                    }
                }
            }
        ),
        Tool(
            name="get_top_players",
            description="Get top-rated players, optionally filtered by nationality or club.",
            inputSchema={
                "type": "object",
                "properties": {
                    "nationality": {
                        "type": "string",
                        "description": "Filter by nationality"
                    },
                    "club": {
                        "type": "string",
                        "description": "Filter by club"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of players to return (default: 20)"
                    }
                }
            }
        ),

        # Phase 3: Analytics
        Tool(
            name="get_standings",
            description="Get league standings for a specific season, calculated from match results. "
                       "Includes points, wins, goals, and position for each team.",
            inputSchema={
                "type": "object",
                "properties": {
                    "competition": {
                        "type": "string",
                        "description": "Competition name (e.g., 'Brasileirao')"
                    },
                    "season": {
                        "type": "integer",
                        "description": "Season year"
                    }
                },
                "required": ["competition", "season"]
            }
        ),
        Tool(
            name="get_biggest_wins",
            description="Find matches with the largest goal differences (biggest victories).",
            inputSchema={
                "type": "object",
                "properties": {
                    "competition": {
                        "type": "string",
                        "description": "Filter by competition"
                    },
                    "season": {
                        "type": "integer",
                        "description": "Filter by season"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results (default: 20)"
                    }
                }
            }
        ),
        Tool(
            name="get_league_stats",
            description="Get aggregate statistics for a competition, including total matches, "
                       "goals, average goals per match, and home/away win rates.",
            inputSchema={
                "type": "object",
                "properties": {
                    "competition": {
                        "type": "string",
                        "description": "Competition name"
                    },
                    "season": {
                        "type": "integer",
                        "description": "Filter by season"
                    }
                },
                "required": ["competition"]
            }
        ),
        Tool(
            name="get_competition_winners",
            description="Get historical winners of a competition across seasons.",
            inputSchema={
                "type": "object",
                "properties": {
                    "competition": {
                        "type": "string",
                        "description": "Competition name"
                    },
                    "start_year": {
                        "type": "integer",
                        "description": "Start of year range"
                    },
                    "end_year": {
                        "type": "integer",
                        "description": "End of year range"
                    }
                },
                "required": ["competition"]
            }
        ),
        Tool(
            name="list_teams",
            description="List all teams in the database.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]


# =============================================================================
# TOOL IMPLEMENTATIONS
# =============================================================================

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""
    try:
        executor = get_executor()
        result = {}

        # Phase 1: Match Queries
        if name == "find_matches":
            result = executor.find_matches_by_team(
                team=arguments["team"],
                season=arguments.get("season"),
                competition=arguments.get("competition"),
                home_only=arguments.get("home_only", False),
                away_only=arguments.get("away_only", False),
                limit=arguments.get("limit", 50)
            )

        elif name == "get_head_to_head":
            result = executor.find_matches_between_teams(
                team1=arguments["team1"],
                team2=arguments["team2"],
                limit=arguments.get("limit", 50)
            )

        # Phase 2: Team & Player Queries
        elif name == "get_team_stats":
            result = executor.get_team_statistics(
                team=arguments["team"],
                season=arguments.get("season"),
                competition=arguments.get("competition")
            )

        elif name == "find_players":
            result = executor.find_players(
                name=arguments.get("name"),
                nationality=arguments.get("nationality"),
                club=arguments.get("club"),
                position=arguments.get("position"),
                min_overall=arguments.get("min_overall"),
                max_overall=arguments.get("max_overall"),
                limit=arguments.get("limit", 50)
            )

        elif name == "get_top_players":
            result = executor.get_top_players_by_rating(
                nationality=arguments.get("nationality"),
                club=arguments.get("club"),
                limit=arguments.get("limit", 20)
            )

        # Phase 3: Analytics
        elif name == "get_standings":
            result = executor.get_season_standings(
                competition=arguments["competition"],
                season=arguments["season"]
            )

        elif name == "get_biggest_wins":
            result = executor.get_biggest_wins(
                competition=arguments.get("competition"),
                season=arguments.get("season"),
                limit=arguments.get("limit", 20)
            )

        elif name == "get_league_stats":
            result = executor.get_league_statistics(
                competition=arguments["competition"],
                season=arguments.get("season")
            )

        elif name == "get_competition_winners":
            result = executor.get_competition_winners(
                competition=arguments["competition"],
                start_year=arguments.get("start_year"),
                end_year=arguments.get("end_year")
            )

        elif name == "list_teams":
            result = {"teams": executor.get_all_teams()}

        else:
            result = {"error": f"Unknown tool: {name}"}

        return [TextContent(type="text", text=format_result(result))]

    except Exception as e:
        logger.error(f"Error executing tool {name}: {e}")
        return [TextContent(
            type="text",
            text=json.dumps({"error": str(e)})
        )]


# =============================================================================
# INITIALIZATION AND DATA LOADING
# =============================================================================

def initialize_database(data_dir: str = None) -> dict:
    """
    Initialize the database with schema and load data.

    Args:
        data_dir: Path to data directory (default: data/kaggle)

    Returns:
        Dictionary with loading statistics
    """
    if data_dir is None:
        # Find data directory relative to project root
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        data_dir = os.path.join(project_root, "data", "kaggle")

    conn = get_connection()
    conn.setup_schema()

    loader = DataLoader(conn, data_dir)
    stats = loader.load_all()

    return stats


def main():
    """Main entry point for the MCP server."""
    import argparse

    parser = argparse.ArgumentParser(description="Brazilian Soccer MCP Server")
    parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize database and load data before starting server"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Path to data directory containing CSV files"
    )
    args = parser.parse_args()

    if args.init:
        logger.info("Initializing database...")
        stats = initialize_database(args.data_dir)
        logger.info(f"Database initialized: {stats}")

    # Run the MCP server
    logger.info("Starting Brazilian Soccer MCP Server...")
    asyncio.run(run_server())


async def run_server():
    """Run the MCP server with stdio transport."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    main()
