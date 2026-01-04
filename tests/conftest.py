"""
================================================================================
CONTEXT BLOCK
================================================================================
File: conftest.py
Module: tests.conftest
Purpose: Pytest fixtures and configuration for BDD tests

Description:
    Provides shared fixtures for all test modules including:
    - Self-contained Neo4j container using testcontainers
    - Query executor instance
    - Test data fixtures
    - Performance timing utilities

Fixtures:
    - neo4j_container: Starts a Neo4j container for tests
    - db_connection: Neo4j connection to the container
    - query_executor: QueryExecutor instance for query tests
    - sample_teams: List of well-known Brazilian teams
    - sample_players: List of famous players

Notes:
    Tests are self-contained using testcontainers. No external Neo4j required.
    Data is loaded into the container at session start.

Created: 2025-12-14
Updated: 2026-01-04 - Added testcontainers for self-contained tests
================================================================================
"""

import os
import time
import pytest
from testcontainers.neo4j import Neo4jContainer

from brazilian_soccer_mcp.database import Neo4jConnection, DataLoader
from brazilian_soccer_mcp.queries import QueryExecutor


@pytest.fixture(scope="session")
def neo4j_container():
    """
    Start a Neo4j container for the test session.

    Given: Docker is available
    Then: A Neo4j container is started and yielded
    """
    # Configure Neo4j container with password
    container = Neo4jContainer("neo4j:5", password="testpassword")

    with container as neo4j:
        # Wait for Neo4j to be fully ready
        time.sleep(3)
        yield neo4j


@pytest.fixture(scope="session")
def db_connection(neo4j_container):
    """
    Provide a Neo4j connection for the entire test session.

    Given: A running Neo4j container
    Then: A connection is established and data is loaded
    """
    # Get connection URL from container
    bolt_url = neo4j_container.get_connection_url()

    conn = Neo4jConnection(
        uri=bolt_url,
        user="neo4j",
        password="testpassword"
    )
    conn.connect()

    # Setup schema and load data
    conn.setup_schema()

    # Find data directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    data_dir = os.path.join(project_root, "data", "kaggle")

    # Load data
    loader = DataLoader(conn, data_dir)
    loader.load_all()

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


class PerformanceTimer:
    """Utility class for measuring query performance."""

    def __init__(self):
        self.start_time = None
        self.end_time = None

    def start(self):
        """Start the timer."""
        self.start_time = time.perf_counter()

    def stop(self):
        """Stop the timer and return elapsed time in seconds."""
        self.end_time = time.perf_counter()
        return self.elapsed

    @property
    def elapsed(self):
        """Return elapsed time in seconds."""
        if self.start_time is None:
            return 0
        end = self.end_time if self.end_time else time.perf_counter()
        return end - self.start_time


@pytest.fixture
def performance_timer():
    """Provide a performance timer for benchmark tests."""
    return PerformanceTimer()
