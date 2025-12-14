Feature: Player Queries
  As a user of the Brazilian Soccer MCP server
  I want to search for player information
  So that I can find data about Brazilian players and their ratings

  Background:
    Given the player data is loaded in the database

  Scenario: Search players by nationality
    When I search for players from "Brazil"
    Then I should receive multiple Brazilian players
    And all players should have nationality "Brazil"

  Scenario: Search top-rated players
    When I request top players with minimum rating 85
    Then all returned players should have overall rating 85 or higher
    And players should be sorted by rating descending

  Scenario: Search players by club
    When I search for players at club "Santos"
    Then I should receive players from Santos
    And each player should have club information

  Scenario: Search players by name
    When I search for player named "Neymar"
    Then I should find Neymar Jr
    And the player should have complete profile data

  Scenario: Get top Brazilian players
    When I request top players from "Brazil"
    Then I should receive top-rated Brazilian players
    And the list should include well-known players
