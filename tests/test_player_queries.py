"""
================================================================================
CONTEXT BLOCK
================================================================================
File: test_player_queries.py
Module: tests.test_player_queries
Purpose: BDD tests for player query functionality

Description:
    Tests the player search and information retrieval capabilities
    of the Brazilian Soccer MCP server using pytest-bdd.

Test Scenarios:
    - Search players by nationality
    - Search top-rated players
    - Search players by club
    - Search players by name
    - Get top Brazilian players

Dependencies:
    - pytest-bdd: BDD test framework
    - brazilian_soccer_mcp: The module under test

Created: 2025-12-14
================================================================================
"""

import pytest
from pytest_bdd import scenario, given, when, then, parsers


# =============================================================================
# SCENARIOS
# =============================================================================

@scenario(
    "features/player_queries.feature",
    "Search players by nationality"
)
def test_search_by_nationality():
    """Test searching players by nationality."""
    pass


@scenario(
    "features/player_queries.feature",
    "Search top-rated players"
)
def test_top_rated_players():
    """Test searching for top-rated players."""
    pass


@scenario(
    "features/player_queries.feature",
    "Search players by club"
)
def test_search_by_club():
    """Test searching players by club."""
    pass


@scenario(
    "features/player_queries.feature",
    "Search players by name"
)
def test_search_by_name():
    """Test searching players by name."""
    pass


@scenario(
    "features/player_queries.feature",
    "Get top Brazilian players"
)
def test_top_brazilian_players():
    """Test getting top Brazilian players."""
    pass


# =============================================================================
# GIVEN STEPS
# =============================================================================

@given("the player data is loaded in the database")
def player_data_loaded(query_executor):
    """
    Verify player data is available in the database.

    Given: The database has been populated with player data
    Then: We should be able to query for players
    """
    result = query_executor.find_players(limit=1)
    assert result["total_found"] > 0, "No player data found in database"
    return query_executor


# =============================================================================
# WHEN STEPS
# =============================================================================

@when(
    parsers.parse('I search for players from "{nationality}"'),
    target_fixture="nationality_result"
)
def search_by_nationality(query_executor, nationality):
    """
    When: I search for players by nationality
    """
    return query_executor.find_players(nationality=nationality, limit=50)


@when(
    parsers.parse('I request top players with minimum rating {rating:d}'),
    target_fixture="top_rated_result"
)
def search_top_rated(query_executor, rating):
    """
    When: I request top players with minimum rating
    """
    return query_executor.find_players(min_overall=rating, limit=50)


@when(
    parsers.parse('I search for players at club "{club}"'),
    target_fixture="club_result"
)
def search_by_club(query_executor, club):
    """
    When: I search for players by club
    """
    return query_executor.find_players(club=club, limit=50)


@when(
    parsers.parse('I search for player named "{name}"'),
    target_fixture="name_result"
)
def search_by_name(query_executor, name):
    """
    When: I search for a player by name
    """
    return query_executor.find_players(name=name, limit=10)


@when(
    parsers.parse('I request top players from "{nationality}"'),
    target_fixture="top_nationality_result"
)
def search_top_by_nationality(query_executor, nationality):
    """
    When: I request top players from a specific nationality
    """
    return query_executor.get_top_players_by_rating(nationality=nationality, limit=20)


# =============================================================================
# THEN STEPS
# =============================================================================

@then("I should receive multiple Brazilian players")
def verify_multiple_brazilian_players(nationality_result):
    """
    Then: Verify multiple players were found
    """
    assert nationality_result["total_found"] > 10, "Expected multiple Brazilian players"


@then(parsers.parse('all players should have nationality "{nationality}"'))
def verify_nationality(nationality_result, nationality):
    """
    Then: Verify all players have the specified nationality
    """
    players = nationality_result.get("players", [])
    for player in players:
        assert nationality.lower() in player.get("nationality", "").lower()


@then(parsers.parse('all returned players should have overall rating {rating:d} or higher'))
def verify_min_rating(top_rated_result, rating):
    """
    Then: Verify all players meet minimum rating
    """
    players = top_rated_result.get("players", [])
    for player in players:
        assert player.get("overall", 0) >= rating


@then("players should be sorted by rating descending")
def verify_rating_sort(top_rated_result):
    """
    Then: Verify players are sorted by rating in descending order
    """
    players = top_rated_result.get("players", [])
    if len(players) > 1:
        for i in range(len(players) - 1):
            assert players[i].get("overall", 0) >= players[i+1].get("overall", 0)


@then("I should receive players from Santos")
def verify_santos_players(club_result):
    """
    Then: Verify players from Santos were found
    """
    assert club_result["total_found"] > 0, "Expected players from Santos"


@then("each player should have club information")
def verify_club_info(club_result):
    """
    Then: Verify each player has club information
    """
    players = club_result.get("players", [])
    for player in players:
        assert player.get("club") is not None


@then("I should find Neymar Jr")
def verify_neymar_found(name_result):
    """
    Then: Verify Neymar was found
    """
    players = name_result.get("players", [])
    found = any("neymar" in p.get("name", "").lower() for p in players)
    assert found, "Neymar not found in results"


@then("the player should have complete profile data")
def verify_complete_profile(name_result):
    """
    Then: Verify player has complete profile data
    """
    players = name_result.get("players", [])
    if players:
        player = players[0]
        assert player.get("name") is not None
        assert player.get("overall") is not None
        assert player.get("nationality") is not None


@then("I should receive top-rated Brazilian players")
def verify_top_brazilian(top_nationality_result):
    """
    Then: Verify top Brazilian players were returned
    """
    assert top_nationality_result["total"] > 0


@then("the list should include well-known players")
def verify_known_players(top_nationality_result):
    """
    Then: Verify list includes well-known Brazilian players
    """
    players = top_nationality_result.get("players", [])
    player_names = [p.get("name", "").lower() for p in players]

    # Check for some famous Brazilian players
    known_players = ["neymar", "thiago silva", "marcelo", "coutinho", "casemiro"]
    found = sum(1 for kp in known_players if any(kp in pn for pn in player_names))

    assert found >= 2, f"Expected at least 2 well-known players, found {found}"
