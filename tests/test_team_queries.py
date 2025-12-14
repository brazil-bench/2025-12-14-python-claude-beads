"""
================================================================================
CONTEXT BLOCK
================================================================================
File: test_team_queries.py
Module: tests.test_team_queries
Purpose: BDD tests for team query functionality

Description:
    Tests the team statistics and information retrieval capabilities
    of the Brazilian Soccer MCP server using pytest-bdd.

Test Scenarios:
    - Comprehensive team statistics
    - Season-specific team stats
    - Team listing

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
    "features/team_queries.feature",
    "Get comprehensive team statistics"
)
def test_comprehensive_stats():
    """Test getting comprehensive team statistics."""
    pass


@scenario(
    "features/team_queries.feature",
    "Get team statistics for specific season"
)
def test_season_stats():
    """Test getting season-specific team statistics."""
    pass


@scenario(
    "features/team_queries.feature",
    "List all teams"
)
def test_list_teams():
    """Test listing all teams."""
    pass


# =============================================================================
# GIVEN STEPS (reuse from match_queries)
# =============================================================================

@given("the match data is loaded in the database")
def match_data_loaded(query_executor):
    """
    Verify match data is available in the database.
    """
    result = query_executor.find_matches_by_team("Palmeiras", limit=1)
    assert result["total_matches"] > 0, "No match data found in database"
    return query_executor


# =============================================================================
# WHEN STEPS
# =============================================================================

@when(
    parsers.parse('I request statistics for team "{team}"'),
    target_fixture="team_stats_result"
)
def request_team_stats(query_executor, team):
    """
    When: I request comprehensive statistics for a team
    """
    return query_executor.get_team_statistics(team)


@when(
    parsers.parse('I request statistics for "{team}" in season {season:d}'),
    target_fixture="season_stats_result"
)
def request_season_stats(query_executor, team, season):
    """
    When: I request statistics for a team in a specific season
    """
    return query_executor.get_team_statistics(team, season=season)


@when("I request the list of all teams", target_fixture="teams_list_result")
def request_teams_list(query_executor):
    """
    When: I request the list of all teams
    """
    return query_executor.get_all_teams()


# =============================================================================
# THEN STEPS
# =============================================================================

@then("I should receive overall statistics")
def verify_overall_stats(team_stats_result):
    """
    Then: Verify overall statistics are included
    """
    assert "overall" in team_stats_result
    overall = team_stats_result["overall"]
    assert "matches" in overall
    assert "wins" in overall
    assert "draws" in overall
    assert "losses" in overall
    assert "goals_for" in overall
    assert "goals_against" in overall


@then("I should receive home statistics")
def verify_home_stats(team_stats_result):
    """
    Then: Verify home statistics are included
    """
    assert "home" in team_stats_result
    home = team_stats_result["home"]
    assert "matches" in home
    assert "wins" in home


@then("I should receive away statistics")
def verify_away_stats(team_stats_result):
    """
    Then: Verify away statistics are included
    """
    assert "away" in team_stats_result
    away = team_stats_result["away"]
    assert "matches" in away
    assert "wins" in away


@then("win rate should be calculated")
def verify_win_rate(team_stats_result):
    """
    Then: Verify win rate is calculated
    """
    overall = team_stats_result["overall"]
    assert "win_rate" in overall
    assert 0 <= overall["win_rate"] <= 100


@then("the statistics should be for 2019 only")
def verify_season_filter(season_stats_result):
    """
    Then: Verify season filter is applied
    """
    assert season_stats_result.get("season") == 2019


@then("the match count should be reasonable for one season")
def verify_reasonable_match_count(season_stats_result):
    """
    Then: Verify match count is reasonable for a single season

    A Brazilian league season typically has 38 rounds, plus cup matches.
    So we expect between 10-100 matches per team per season.
    """
    overall = season_stats_result["overall"]
    matches = overall["matches"]
    # Should have some matches but not too many for a single season
    assert 0 < matches < 150, f"Expected reasonable match count, got {matches}"


@then("I should receive multiple teams")
def verify_multiple_teams(teams_list_result):
    """
    Then: Verify multiple teams are returned
    """
    assert len(teams_list_result) > 10, "Expected at least 10 teams"


@then("major Brazilian teams should be included")
def verify_major_teams(teams_list_result, sample_brazilian_teams):
    """
    Then: Verify major Brazilian teams are in the list
    """
    team_names = [t.get("name", "") for t in teams_list_result]

    # Check for some major teams (allowing for name normalization)
    major_teams_found = 0
    for major_team in sample_brazilian_teams:
        for name in team_names:
            if major_team.lower() in name.lower():
                major_teams_found += 1
                break

    assert major_teams_found >= 5, f"Expected at least 5 major teams, found {major_teams_found}"
