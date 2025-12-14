Feature: Team Queries
  As a user of the Brazilian Soccer MCP server
  I want to get team statistics and information
  So that I can analyze team performance

  Background:
    Given the match data is loaded in the database

  Scenario: Get comprehensive team statistics
    When I request statistics for team "Palmeiras"
    Then I should receive overall statistics
    And I should receive home statistics
    And I should receive away statistics
    And win rate should be calculated

  Scenario: Get team statistics for specific season
    When I request statistics for "Flamengo" in season 2019
    Then the statistics should be for 2019 only
    And the match count should be reasonable for one season

  Scenario: List all teams
    When I request the list of all teams
    Then I should receive multiple teams
    And major Brazilian teams should be included
