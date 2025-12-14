"""
================================================================================
CONTEXT BLOCK
================================================================================
File: conftest.py
Module: tests.conftest
Purpose: Pytest fixtures and configuration for BDD tests

Description:
    Provides shared fixtures for all test modules including:
    - Neo4j database connection
    - Query executor instance
    - Test data fixtures

Fixtures:
    - db_connection: Singleton Neo4j connection for all tests
    - query_executor: QueryExecutor instance for query tests
    - sample_teams: List of well-known Brazilian teams
    - sample_players: List of famous players

Notes:
    Tests use the actual database with loaded data, not mocks.
    This provides realistic integration testing.

Created: 2025-12-14
================================================================================
"""

import pytest
from brazilian_soccer_mcp.database import Neo4jConnection
from brazilian_soccer_mcp.queries import QueryExecutor


@pytest.fixture(scope="session")
def db_connection():
    """
    Provide a Neo4j connection for the entire test session.

    Given: The Neo4j database is running with loaded data
    Then: A connection is established and yielded to tests
    """
    conn = Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="password123"
    )
    conn.connect()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def query_executor(db_connection):
    """
    Provide a QueryExecutor instance for the test session.

    Given: A valid database connection
    Then: A QueryExecutor is created and yielded
    """
    return QueryExecutor(db_connection)


@pytest.fixture
def sample_brazilian_teams():
    """
    Provide a list of well-known Brazilian teams for testing.

    These teams are expected to have match data in the database.
    """
    return [
        "Flamengo",
        "Fluminense",
        "Palmeiras",
        "Corinthians",
        "Sao Paulo",
        "Santos",
        "Gremio",
        "Internacional",
        "Atletico-MG",
        "Cruzeiro",
    ]


@pytest.fixture
def classic_derbies():
    """
    Provide classic derby matchups for head-to-head testing.
    """
    return [
        ("Flamengo", "Fluminense"),      # Fla-Flu
        ("Palmeiras", "Corinthians"),     # Derby Paulista
        ("Sao Paulo", "Santos"),          # Classico San-Sao
        ("Gremio", "Internacional"),      # Gre-Nal
        ("Atletico-MG", "Cruzeiro"),      # Classico Mineiro
    ]


@pytest.fixture
def sample_seasons():
    """Provide a list of seasons expected to have data."""
    return [2015, 2016, 2017, 2018, 2019]


@pytest.fixture
def competitions():
    """Provide competition names."""
    return {
        "league": "Brasileirao",
        "cup": "Copa do Brasil",
        "continental": "Libertadores"
    }
