Feature: Query Performance
  As a user of the Brazilian Soccer MCP server
  I want queries to respond quickly
  So that I can get answers in a timely manner

  Background:
    Given the match data is loaded in the database

  Scenario: Simple match lookup responds quickly
    When I time a simple match lookup for "Palmeiras"
    Then the query should complete in less than 2 seconds

  Scenario: Head-to-head query responds quickly
    When I time a head-to-head lookup for "Flamengo" vs "Fluminense"
    Then the query should complete in less than 2 seconds

  Scenario: Player search responds quickly
    When I time a player search for Brazilian players
    Then the query should complete in less than 2 seconds

  Scenario: Team statistics responds quickly
    When I time team statistics lookup for "Corinthians"
    Then the query should complete in less than 2 seconds

  Scenario: Aggregate standings calculation responds in time
    When I time standings calculation for "Brasileirao" season 2019
    Then the query should complete in less than 5 seconds

  Scenario: League statistics aggregation responds in time
    When I time league statistics for "Brasileirao"
    Then the query should complete in less than 5 seconds

  Scenario: Biggest wins aggregation responds in time
    When I time biggest wins query for "Brasileirao"
    Then the query should complete in less than 5 seconds
