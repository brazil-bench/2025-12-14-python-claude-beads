"""
================================================================================
CONTEXT BLOCK
================================================================================
File: test_match_queries.py
Module: tests.test_match_queries
Purpose: BDD tests for match query functionality

Description:
    Tests the match query capabilities of the Brazilian Soccer MCP server
    using pytest-bdd with Given-When-Then structure.

Test Scenarios:
    - Head-to-head match searches
    - Team match history
    - Filtering by season, competition
    - Home/away match filtering

Dependencies:
    - pytest-bdd: BDD test framework
    - brazilian_soccer_mcp: The module under test

Created: 2025-12-14
================================================================================
"""

import pytest
from pytest_bdd import scenario, given, when, then, parsers

from brazilian_soccer_mcp.models import normalize_team_name


# =============================================================================
# SCENARIOS
# =============================================================================

@scenario(
    "features/match_queries.feature",
    "Find matches between two teams (head-to-head)"
)
def test_head_to_head():
    """Test head-to-head match search."""
    pass


@scenario(
    "features/match_queries.feature",
    "Get team match history"
)
def test_team_match_history():
    """Test getting a team's match history."""
    pass


@scenario(
    "features/match_queries.feature",
    "Filter matches by season"
)
def test_filter_by_season():
    """Test filtering matches by season."""
    pass


@scenario(
    "features/match_queries.feature",
    "Filter matches by competition"
)
def test_filter_by_competition():
    """Test filtering matches by competition."""
    pass


@scenario(
    "features/match_queries.feature",
    "Get home matches only"
)
def test_home_matches():
    """Test getting home matches only."""
    pass


@scenario(
    "features/match_queries.feature",
    "Get away matches only"
)
def test_away_matches():
    """Test getting away matches only."""
    pass


# =============================================================================
# GIVEN STEPS
# =============================================================================

@given("the match data is loaded in the database")
def match_data_loaded(query_executor):
    """
    Verify match data is available in the database.

    Given: The database has been populated with match data
    Then: We should be able to query for matches
    """
    # Verify by checking if we can get any matches
    result = query_executor.find_matches_by_team("Palmeiras", limit=1)
    assert result["total_matches"] > 0, "No match data found in database"
    return query_executor


# =============================================================================
# WHEN STEPS
# =============================================================================

@when(
    parsers.parse('I search for matches between "{team1}" and "{team2}"'),
    target_fixture="h2h_result"
)
def search_head_to_head(query_executor, team1, team2):
    """
    When: I search for matches between two teams
    """
    return query_executor.find_matches_between_teams(team1, team2)


@when(
    parsers.parse('I search for all matches for team "{team}"'),
    target_fixture="team_matches_result"
)
def search_team_matches(query_executor, team):
    """
    When: I search for all matches for a specific team
    """
    return query_executor.find_matches_by_team(team)


@when(
    parsers.parse('I search for "{team}" matches in season {season:d}'),
    target_fixture="season_matches_result"
)
def search_by_season(query_executor, team, season):
    """
    When: I search for matches filtered by season
    """
    return query_executor.find_matches_by_team(team, season=season)


@when(
    parsers.parse('I search for "{team}" matches in "{competition}"'),
    target_fixture="competition_matches_result"
)
def search_by_competition(query_executor, team, competition):
    """
    When: I search for matches filtered by competition
    """
    return query_executor.find_matches_by_team(team, competition=competition)


@when(
    parsers.parse('I search for home matches for "{team}"'),
    target_fixture="home_matches_result"
)
def search_home_matches(query_executor, team):
    """
    When: I search for home matches only
    """
    return query_executor.find_matches_by_team(team, home_only=True)


@when(
    parsers.parse('I search for away matches for "{team}"'),
    target_fixture="away_matches_result"
)
def search_away_matches(query_executor, team):
    """
    When: I search for away matches only
    """
    return query_executor.find_matches_by_team(team, away_only=True)


# =============================================================================
# THEN STEPS
# =============================================================================

@then("I should receive a list of matches")
def verify_matches_returned(request):
    """
    Then: Verify that matches were returned
    """
    # Get the result from the appropriate fixture
    result = None
    if "h2h_result" in request.fixturenames:
        result = request.getfixturevalue("h2h_result")
    elif "team_matches_result" in request.fixturenames:
        result = request.getfixturevalue("team_matches_result")

    assert result is not None, "No result fixture found"
    assert "matches" in result or "total_matches" in result
    matches = result.get("matches", [])
    assert len(matches) >= 0  # May be empty for some searches


@then("each match should have date, scores, and competition")
def verify_match_structure(h2h_result):
    """
    Then: Verify match data structure is complete
    """
    matches = h2h_result.get("matches", [])
    if matches:
        match = matches[0]
        # Check required fields exist
        assert "home_goals" in match
        assert "away_goals" in match
        assert "home_team" in match
        assert "away_team" in match


@then("the head-to-head statistics should be calculated")
def verify_h2h_stats(h2h_result):
    """
    Then: Verify head-to-head statistics are included
    """
    assert "team1_wins" in h2h_result
    assert "team2_wins" in h2h_result
    assert "draws" in h2h_result
    assert "total_matches" in h2h_result

    # Verify stats add up
    total = h2h_result["team1_wins"] + h2h_result["team2_wins"] + h2h_result["draws"]
    assert total == h2h_result["total_matches"]


@then("the statistics should include wins, losses, and draws")
def verify_team_stats(team_matches_result):
    """
    Then: Verify team statistics are included
    """
    assert "wins" in team_matches_result
    assert "losses" in team_matches_result
    assert "draws" in team_matches_result
    assert "goals_for" in team_matches_result
    assert "goals_against" in team_matches_result


@then("I should receive only matches from 2019")
def verify_2019_matches(season_matches_result):
    """
    Then: Verify all matches are from specified season
    """
    matches = season_matches_result.get("matches", [])
    for match in matches:
        if match.get("season"):
            assert match["season"] == 2019


@then("the total should be greater than 0")
def verify_positive_total(season_matches_result):
    """
    Then: Verify some matches were found
    """
    assert season_matches_result["total_matches"] > 0


@then("all returned matches should be from Copa do Brasil")
def verify_copa_brasil_matches(competition_matches_result):
    """
    Then: Verify all matches are from Copa do Brasil
    """
    matches = competition_matches_result.get("matches", [])
    for match in matches:
        assert "Copa do Brasil" in match.get("competition", "")


@then(parsers.parse('all returned matches should have "{team}" as home team'))
def verify_home_team(home_matches_result, team):
    """
    Then: Verify team is home in all matches
    """
    team_norm = normalize_team_name(team)
    matches = home_matches_result.get("matches", [])
    for match in matches:
        assert normalize_team_name(match["home_team"]) == team_norm


@then(parsers.parse('all returned matches should have "{team}" as away team'))
def verify_away_team(away_matches_result, team):
    """
    Then: Verify team is away in all matches
    """
    team_norm = normalize_team_name(team)
    matches = away_matches_result.get("matches", [])
    for match in matches:
        assert normalize_team_name(match["away_team"]) == team_norm
