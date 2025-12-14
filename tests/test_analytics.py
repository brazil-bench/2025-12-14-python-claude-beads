"""
================================================================================
CONTEXT BLOCK
================================================================================
File: test_analytics.py
Module: tests.test_analytics
Purpose: BDD tests for analytics and statistics functionality

Description:
    Tests the analytical and statistical capabilities of the Brazilian Soccer
    MCP server using pytest-bdd. These tests cover standings calculation,
    aggregate statistics, and historical analysis.

Test Scenarios:
    - League standings calculation
    - Biggest wins finding
    - League aggregate statistics
    - Competition winners history

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
    "features/analytics.feature",
    "Calculate league standings for a season"
)
def test_league_standings():
    """Test calculating league standings."""
    pass


@scenario(
    "features/analytics.feature",
    "Find biggest wins in competition"
)
def test_biggest_wins():
    """Test finding biggest wins."""
    pass


@scenario(
    "features/analytics.feature",
    "Get league aggregate statistics"
)
def test_league_stats():
    """Test getting league aggregate statistics."""
    pass


@scenario(
    "features/analytics.feature",
    "Get competition winners history"
)
def test_winners_history():
    """Test getting competition winners history."""
    pass


# =============================================================================
# GIVEN STEPS
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
    parsers.parse('I request standings for "{competition}" season {season:d}'),
    target_fixture="standings_result"
)
def request_standings(query_executor, competition, season):
    """
    When: I request standings for a competition and season
    """
    return query_executor.get_season_standings(competition, season)


@when(
    parsers.parse('I request biggest wins in "{competition}"'),
    target_fixture="biggest_wins_result"
)
def request_biggest_wins(query_executor, competition):
    """
    When: I request biggest wins in a competition
    """
    return query_executor.get_biggest_wins(competition=competition, limit=20)


@when(
    parsers.parse('I request statistics for "{competition}"'),
    target_fixture="league_stats_result"
)
def request_league_stats(query_executor, competition):
    """
    When: I request statistics for a competition
    """
    return query_executor.get_league_statistics(competition)


@when(
    parsers.parse('I request winners of "{competition}" from {start:d} to {end:d}'),
    target_fixture="winners_result"
)
def request_winners(query_executor, competition, start, end):
    """
    When: I request winners history for a competition
    """
    return query_executor.get_competition_winners(
        competition, start_year=start, end_year=end
    )


# =============================================================================
# THEN STEPS
# =============================================================================

@then("I should receive a ranked list of teams")
def verify_ranked_list(standings_result):
    """
    Then: Verify standings contains a ranked list of teams
    """
    assert "standings" in standings_result
    standings = standings_result["standings"]
    assert len(standings) > 0, "Expected standings with teams"


@then("each team should have points, wins, draws, losses")
def verify_standings_fields(standings_result):
    """
    Then: Verify each team has required statistics
    """
    standings = standings_result.get("standings", [])
    for team in standings[:5]:  # Check first 5
        assert "team" in team
        assert "points" in team
        assert "wins" in team
        assert "draws" in team
        assert "losses" in team
        assert "goals_for" in team
        assert "goals_against" in team


@then("positions should be ordered by points")
def verify_points_order(standings_result):
    """
    Then: Verify standings are sorted by points descending
    """
    standings = standings_result.get("standings", [])
    if len(standings) > 1:
        for i in range(len(standings) - 1):
            assert standings[i]["points"] >= standings[i+1]["points"], \
                f"Standings not sorted: {standings[i]['points']} < {standings[i+1]['points']}"


@then("I should receive matches with large goal differences")
def verify_big_wins(biggest_wins_result):
    """
    Then: Verify matches with large goal differences are returned
    """
    matches = biggest_wins_result.get("matches", [])
    assert len(matches) > 0, "Expected matches with big wins"

    # First match should have the largest difference
    if matches:
        assert matches[0].get("goal_difference", 0) >= 3, \
            "Expected at least 3 goal difference for biggest wins"


@then("results should be sorted by goal difference")
def verify_goal_diff_sort(biggest_wins_result):
    """
    Then: Verify results are sorted by goal difference descending
    """
    matches = biggest_wins_result.get("matches", [])
    if len(matches) > 1:
        for i in range(len(matches) - 1):
            assert matches[i]["goal_difference"] >= matches[i+1]["goal_difference"]


@then("I should receive total matches count")
def verify_total_matches(league_stats_result):
    """
    Then: Verify total matches count is included
    """
    assert "total_matches" in league_stats_result
    assert league_stats_result["total_matches"] > 0


@then("I should receive total goals scored")
def verify_total_goals(league_stats_result):
    """
    Then: Verify total goals scored is included
    """
    assert "total_goals" in league_stats_result
    assert league_stats_result["total_goals"] > 0


@then("I should receive average goals per match")
def verify_avg_goals(league_stats_result):
    """
    Then: Verify average goals per match is included
    """
    assert "avg_goals_per_match" in league_stats_result
    avg = league_stats_result["avg_goals_per_match"]
    # Average goals per match should be reasonable (1-5)
    assert 1.0 <= avg <= 5.0, f"Average goals {avg} seems unreasonable"


@then("I should receive home/away win rates")
def verify_win_rates(league_stats_result):
    """
    Then: Verify home and away win rates are included
    """
    assert "home_win_rate" in league_stats_result
    assert "away_win_rate" in league_stats_result
    assert "draw_rate" in league_stats_result

    # Rates should sum to approximately 100
    total_rate = (
        league_stats_result["home_win_rate"] +
        league_stats_result["away_win_rate"] +
        league_stats_result["draw_rate"]
    )
    assert 98 <= total_rate <= 102, f"Win rates don't sum to 100: {total_rate}"


@then("I should receive winners for each season")
def verify_winners_list(winners_result):
    """
    Then: Verify winners are returned for each season
    """
    assert "winners" in winners_result
    winners = winners_result["winners"]
    assert len(winners) > 0, "Expected winners list"


@then("each entry should include team name and points")
def verify_winner_fields(winners_result):
    """
    Then: Verify each winner entry has required fields
    """
    winners = winners_result.get("winners", [])
    for winner in winners:
        assert "season" in winner
        assert "winner" in winner
        assert "points" in winner
