"""
================================================================================
CONTEXT BLOCK
================================================================================
File: __init__.py
Module: tests
Purpose: Test package initialization for Brazilian Soccer MCP Server

Description:
    This package contains BDD-style tests using pytest-bdd framework.
    Tests are structured using Given-When-Then (GWT) format to clearly
    express test scenarios as user stories.

Test Categories:
    - test_match_queries.py: Match search and head-to-head tests
    - test_team_queries.py: Team statistics and records tests
    - test_player_queries.py: Player search and ratings tests
    - test_analytics.py: Standings, statistics, and analysis tests

Test Data:
    Tests run against the actual Neo4j database populated with:
    - 666 teams
    - 18,207 players (FIFA dataset)
    - 23,954 matches across 3 competitions

Created: 2025-12-14
================================================================================
"""
