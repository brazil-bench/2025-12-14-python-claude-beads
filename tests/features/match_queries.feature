Feature: Match Queries
  As a user of the Brazilian Soccer MCP server
  I want to search for match data
  So that I can find information about specific games and team matchups

  Background:
    Given the match data is loaded in the database

  Scenario: Find matches between two teams (head-to-head)
    When I search for matches between "Flamengo" and "Fluminense"
    Then I should receive a list of matches
    And each match should have date, scores, and competition
    And the head-to-head statistics should be calculated

  Scenario: Get team match history
    When I search for all matches for team "Palmeiras"
    Then I should receive a list of matches
    And the statistics should include wins, losses, and draws

  Scenario: Filter matches by season
    When I search for "Corinthians" matches in season 2019
    Then I should receive only matches from 2019
    And the total should be greater than 0

  Scenario: Filter matches by competition
    When I search for "Santos" matches in "Copa do Brasil"
    Then all returned matches should be from Copa do Brasil

  Scenario: Get home matches only
    When I search for home matches for "Flamengo"
    Then all returned matches should have "Flamengo" as home team

  Scenario: Get away matches only
    When I search for away matches for "Gremio"
    Then all returned matches should have "Gremio" as away team
