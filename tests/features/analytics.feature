Feature: Analytics and Statistics
  As a user of the Brazilian Soccer MCP server
  I want to get analytical insights about competitions
  So that I can understand league trends and historical data

  Background:
    Given the match data is loaded in the database

  Scenario: Calculate league standings for a season
    When I request standings for "Brasileirao" season 2019
    Then I should receive a ranked list of teams
    And each team should have points, wins, draws, losses
    And positions should be ordered by points

  Scenario: Find biggest wins in competition
    When I request biggest wins in "Brasileirao"
    Then I should receive matches with large goal differences
    And results should be sorted by goal difference

  Scenario: Get league aggregate statistics
    When I request statistics for "Brasileirao"
    Then I should receive total matches count
    And I should receive total goals scored
    And I should receive average goals per match
    And I should receive home/away win rates

  Scenario: Get competition winners history
    When I request winners of "Brasileirao" from 2015 to 2019
    Then I should receive winners for each season
    And each entry should include team name and points
