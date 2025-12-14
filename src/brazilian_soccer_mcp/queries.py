"""
================================================================================
CONTEXT BLOCK
================================================================================
File: queries.py
Module: brazilian_soccer_mcp.queries
Purpose: Neo4j Cypher query builders and executors for soccer knowledge graph

Description:
    Provides high-level query functions for the Brazilian Soccer MCP server.
    Each function corresponds to a capability required by the MCP server tools.

Query Categories:
    1. Match Queries: Find matches by team, date, competition, head-to-head
    2. Team Queries: Get team statistics, records, performance
    3. Player Queries: Search players by name, club, nationality, ratings
    4. Competition Queries: Standings, winners, tournament info
    5. Statistical Analysis: Aggregations, comparisons, trends

Performance Considerations:
    - Uses parameterized queries to leverage Neo4j query caching
    - Limits result sets for large queries
    - Uses indexes on frequently queried properties

Created: 2025-12-14
================================================================================
"""

from typing import Optional
from datetime import datetime

from .database import Neo4jConnection
from .models import normalize_team_name


class QueryExecutor:
    """
    Executes queries against the Brazilian Soccer knowledge graph.

    All methods return dictionaries suitable for JSON serialization
    and MCP tool responses.
    """

    def __init__(self, connection: Neo4jConnection):
        """
        Initialize query executor.

        Args:
            connection: Active Neo4jConnection instance
        """
        self.conn = connection

    # =========================================================================
    # MATCH QUERIES (Phase 1 & 2)
    # =========================================================================

    def find_matches_between_teams(
        self,
        team1: str,
        team2: str,
        limit: int = 50
    ) -> dict:
        """
        Find all matches between two teams (head-to-head).

        Args:
            team1: First team name
            team2: Second team name
            limit: Maximum number of matches to return

        Returns:
            Dictionary with matches and head-to-head statistics
        """
        team1_norm = normalize_team_name(team1)
        team2_norm = normalize_team_name(team2)

        query = """
        MATCH (m:Match)
        WHERE (m.home_team = $team1 AND m.away_team = $team2)
           OR (m.home_team = $team2 AND m.away_team = $team1)
        RETURN m
        ORDER BY m.datetime DESC
        LIMIT $limit
        """

        results = self.conn.execute(query, {
            "team1": team1_norm,
            "team2": team2_norm,
            "limit": limit
        })

        matches = [r["m"] for r in results]

        # Calculate head-to-head stats
        team1_wins = 0
        team2_wins = 0
        draws = 0
        team1_goals = 0
        team2_goals = 0

        for m in matches:
            home = m.get("home_team")
            hg = m.get("home_goals", 0)
            ag = m.get("away_goals", 0)

            if home == team1_norm:
                team1_goals += hg
                team2_goals += ag
                if hg > ag:
                    team1_wins += 1
                elif ag > hg:
                    team2_wins += 1
                else:
                    draws += 1
            else:
                team2_goals += hg
                team1_goals += ag
                if hg > ag:
                    team2_wins += 1
                elif ag > hg:
                    team1_wins += 1
                else:
                    draws += 1

        return {
            "team1": team1_norm,
            "team2": team2_norm,
            "total_matches": len(matches),
            "team1_wins": team1_wins,
            "team2_wins": team2_wins,
            "draws": draws,
            "team1_goals": team1_goals,
            "team2_goals": team2_goals,
            "matches": matches
        }

    def find_matches_by_team(
        self,
        team: str,
        season: Optional[int] = None,
        competition: Optional[str] = None,
        home_only: bool = False,
        away_only: bool = False,
        limit: int = 100
    ) -> dict:
        """
        Find matches for a specific team.

        Args:
            team: Team name
            season: Filter by season year
            competition: Filter by competition name
            home_only: Only return home matches
            away_only: Only return away matches
            limit: Maximum results

        Returns:
            Dictionary with matches and summary statistics
        """
        team_norm = normalize_team_name(team)

        conditions = []
        params = {"team": team_norm, "limit": limit}

        if home_only:
            conditions.append("m.home_team = $team")
        elif away_only:
            conditions.append("m.away_team = $team")
        else:
            conditions.append("(m.home_team = $team OR m.away_team = $team)")

        if season:
            conditions.append("m.season = $season")
            params["season"] = season

        if competition:
            conditions.append("m.competition CONTAINS $competition")
            params["competition"] = competition

        where_clause = " AND ".join(conditions)

        query = f"""
        MATCH (m:Match)
        WHERE {where_clause}
        RETURN m
        ORDER BY m.datetime DESC
        LIMIT $limit
        """

        results = self.conn.execute(query, params)
        matches = [r["m"] for r in results]

        # Calculate statistics
        wins = 0
        losses = 0
        draws = 0
        goals_for = 0
        goals_against = 0

        for m in matches:
            home = m.get("home_team")
            hg = m.get("home_goals", 0)
            ag = m.get("away_goals", 0)

            if home == team_norm:
                goals_for += hg
                goals_against += ag
                if hg > ag:
                    wins += 1
                elif ag > hg:
                    losses += 1
                else:
                    draws += 1
            else:
                goals_for += ag
                goals_against += hg
                if ag > hg:
                    wins += 1
                elif hg > ag:
                    losses += 1
                else:
                    draws += 1

        return {
            "team": team_norm,
            "total_matches": len(matches),
            "wins": wins,
            "losses": losses,
            "draws": draws,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "goal_difference": goals_for - goals_against,
            "points": wins * 3 + draws,
            "matches": matches
        }

    def find_matches_by_date_range(
        self,
        start_date: str,
        end_date: str,
        competition: Optional[str] = None,
        limit: int = 100
    ) -> dict:
        """
        Find matches within a date range.

        Args:
            start_date: Start date (ISO format)
            end_date: End date (ISO format)
            competition: Filter by competition
            limit: Maximum results

        Returns:
            Dictionary with matches
        """
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        }

        conditions = ["m.datetime >= $start_date", "m.datetime <= $end_date"]

        if competition:
            conditions.append("m.competition CONTAINS $competition")
            params["competition"] = competition

        where_clause = " AND ".join(conditions)

        query = f"""
        MATCH (m:Match)
        WHERE {where_clause}
        RETURN m
        ORDER BY m.datetime DESC
        LIMIT $limit
        """

        results = self.conn.execute(query, params)
        matches = [r["m"] for r in results]

        return {
            "start_date": start_date,
            "end_date": end_date,
            "total_matches": len(matches),
            "matches": matches
        }

    # =========================================================================
    # TEAM QUERIES (Phase 2)
    # =========================================================================

    def get_team_statistics(
        self,
        team: str,
        season: Optional[int] = None,
        competition: Optional[str] = None
    ) -> dict:
        """
        Get comprehensive statistics for a team.

        Args:
            team: Team name
            season: Filter by season
            competition: Filter by competition

        Returns:
            Dictionary with detailed team statistics
        """
        # Get overall stats
        overall = self.find_matches_by_team(team, season, competition, limit=1000)

        # Get home stats
        home = self.find_matches_by_team(
            team, season, competition, home_only=True, limit=1000
        )

        # Get away stats
        away = self.find_matches_by_team(
            team, season, competition, away_only=True, limit=1000
        )

        return {
            "team": overall["team"],
            "season": season,
            "competition": competition,
            "overall": {
                "matches": overall["total_matches"],
                "wins": overall["wins"],
                "draws": overall["draws"],
                "losses": overall["losses"],
                "goals_for": overall["goals_for"],
                "goals_against": overall["goals_against"],
                "goal_difference": overall["goal_difference"],
                "points": overall["points"],
                "win_rate": round(overall["wins"] / overall["total_matches"] * 100, 1)
                if overall["total_matches"] > 0 else 0
            },
            "home": {
                "matches": home["total_matches"],
                "wins": home["wins"],
                "draws": home["draws"],
                "losses": home["losses"],
                "goals_for": home["goals_for"],
                "goals_against": home["goals_against"],
                "win_rate": round(home["wins"] / home["total_matches"] * 100, 1)
                if home["total_matches"] > 0 else 0
            },
            "away": {
                "matches": away["total_matches"],
                "wins": away["wins"],
                "draws": away["draws"],
                "losses": away["losses"],
                "goals_for": away["goals_for"],
                "goals_against": away["goals_against"],
                "win_rate": round(away["wins"] / away["total_matches"] * 100, 1)
                if away["total_matches"] > 0 else 0
            }
        }

    def get_all_teams(self) -> list:
        """Get list of all teams in the database."""
        query = """
        MATCH (t:Team)
        RETURN t.name as name, t.state as state
        ORDER BY t.name
        """
        return self.conn.execute(query)

    # =========================================================================
    # PLAYER QUERIES (Phase 2)
    # =========================================================================

    def find_players(
        self,
        name: Optional[str] = None,
        nationality: Optional[str] = None,
        club: Optional[str] = None,
        position: Optional[str] = None,
        min_overall: Optional[int] = None,
        max_overall: Optional[int] = None,
        limit: int = 50
    ) -> dict:
        """
        Search for players with various filters.

        Args:
            name: Player name (partial match)
            nationality: Player nationality
            club: Club name
            position: Playing position
            min_overall: Minimum FIFA overall rating
            max_overall: Maximum FIFA overall rating
            limit: Maximum results

        Returns:
            Dictionary with matching players
        """
        conditions = []
        params = {"limit": limit}

        if name:
            conditions.append("toLower(p.name) CONTAINS toLower($name)")
            params["name"] = name

        if nationality:
            conditions.append("toLower(p.nationality) CONTAINS toLower($nationality)")
            params["nationality"] = nationality

        if club:
            club_norm = normalize_team_name(club)
            conditions.append("(p.club = $club OR toLower(p.club) CONTAINS toLower($club_raw))")
            params["club"] = club_norm
            params["club_raw"] = club

        if position:
            conditions.append("p.position CONTAINS $position")
            params["position"] = position

        if min_overall:
            conditions.append("p.overall >= $min_overall")
            params["min_overall"] = min_overall

        if max_overall:
            conditions.append("p.overall <= $max_overall")
            params["max_overall"] = max_overall

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        query = f"""
        MATCH (p:Player)
        WHERE {where_clause}
        RETURN p
        ORDER BY p.overall DESC
        LIMIT $limit
        """

        results = self.conn.execute(query, params)
        players = [r["p"] for r in results]

        return {
            "total_found": len(players),
            "filters": {
                "name": name,
                "nationality": nationality,
                "club": club,
                "position": position,
                "min_overall": min_overall,
                "max_overall": max_overall
            },
            "players": players
        }

    def get_top_players_by_rating(
        self,
        nationality: Optional[str] = None,
        club: Optional[str] = None,
        limit: int = 20
    ) -> dict:
        """
        Get top-rated players, optionally filtered.

        Args:
            nationality: Filter by nationality
            club: Filter by club
            limit: Number of players to return

        Returns:
            Dictionary with top-rated players
        """
        conditions = ["p.overall IS NOT NULL"]
        params = {"limit": limit}

        if nationality:
            conditions.append("toLower(p.nationality) CONTAINS toLower($nationality)")
            params["nationality"] = nationality

        if club:
            conditions.append("toLower(p.club) CONTAINS toLower($club)")
            params["club"] = club

        where_clause = " AND ".join(conditions)

        query = f"""
        MATCH (p:Player)
        WHERE {where_clause}
        RETURN p
        ORDER BY p.overall DESC
        LIMIT $limit
        """

        results = self.conn.execute(query, params)
        players = [r["p"] for r in results]

        return {
            "filters": {
                "nationality": nationality,
                "club": club
            },
            "total": len(players),
            "players": players
        }

    # =========================================================================
    # COMPETITION QUERIES (Phase 3)
    # =========================================================================

    def get_season_standings(
        self,
        competition: str,
        season: int
    ) -> dict:
        """
        Calculate standings for a competition season.

        Note: This calculates standings from match results, as the data
        doesn't include pre-computed standings.

        Args:
            competition: Competition name
            season: Season year

        Returns:
            Dictionary with calculated standings
        """
        query = """
        MATCH (m:Match)
        WHERE m.competition CONTAINS $competition AND m.season = $season
        RETURN m
        """

        results = self.conn.execute(query, {
            "competition": competition,
            "season": season
        })

        # Calculate standings from match results
        standings = {}

        for r in results:
            m = r["m"]
            home = m.get("home_team")
            away = m.get("away_team")
            hg = m.get("home_goals", 0)
            ag = m.get("away_goals", 0)

            for team in [home, away]:
                if team not in standings:
                    standings[team] = {
                        "team": team,
                        "matches": 0,
                        "wins": 0,
                        "draws": 0,
                        "losses": 0,
                        "goals_for": 0,
                        "goals_against": 0,
                        "points": 0
                    }

            # Update home team stats
            standings[home]["matches"] += 1
            standings[home]["goals_for"] += hg
            standings[home]["goals_against"] += ag

            # Update away team stats
            standings[away]["matches"] += 1
            standings[away]["goals_for"] += ag
            standings[away]["goals_against"] += hg

            # Determine winner
            if hg > ag:
                standings[home]["wins"] += 1
                standings[home]["points"] += 3
                standings[away]["losses"] += 1
            elif ag > hg:
                standings[away]["wins"] += 1
                standings[away]["points"] += 3
                standings[home]["losses"] += 1
            else:
                standings[home]["draws"] += 1
                standings[home]["points"] += 1
                standings[away]["draws"] += 1
                standings[away]["points"] += 1

        # Sort by points, then goal difference
        sorted_standings = sorted(
            standings.values(),
            key=lambda x: (x["points"], x["goals_for"] - x["goals_against"], x["goals_for"]),
            reverse=True
        )

        # Add position and goal difference
        for i, team in enumerate(sorted_standings, 1):
            team["position"] = i
            team["goal_difference"] = team["goals_for"] - team["goals_against"]

        return {
            "competition": competition,
            "season": season,
            "total_teams": len(sorted_standings),
            "standings": sorted_standings
        }

    def get_competition_winners(
        self,
        competition: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> dict:
        """
        Get winners for each season of a competition.

        For leagues, returns the team with most points.
        For cups, attempts to find final winners.

        Args:
            competition: Competition name
            start_year: Start of year range
            end_year: End of year range

        Returns:
            Dictionary with winners by season
        """
        # Get all seasons for this competition
        query = """
        MATCH (m:Match)
        WHERE m.competition CONTAINS $competition AND m.season IS NOT NULL
        RETURN DISTINCT m.season as season
        ORDER BY m.season
        """
        results = self.conn.execute(query, {"competition": competition})

        winners = []
        for r in results:
            season = r["season"]
            if start_year and season < start_year:
                continue
            if end_year and season > end_year:
                continue

            standings = self.get_season_standings(competition, season)
            if standings["standings"]:
                winner = standings["standings"][0]
                winners.append({
                    "season": season,
                    "winner": winner["team"],
                    "points": winner["points"],
                    "wins": winner["wins"],
                    "goals_for": winner["goals_for"]
                })

        return {
            "competition": competition,
            "total_seasons": len(winners),
            "winners": winners
        }

    # =========================================================================
    # STATISTICAL ANALYSIS (Phase 3)
    # =========================================================================

    def get_biggest_wins(
        self,
        competition: Optional[str] = None,
        season: Optional[int] = None,
        limit: int = 20
    ) -> dict:
        """
        Find matches with the largest goal differences.

        Args:
            competition: Filter by competition
            season: Filter by season
            limit: Number of results

        Returns:
            Dictionary with biggest victories
        """
        conditions = []
        params = {"limit": limit}

        if competition:
            conditions.append("m.competition CONTAINS $competition")
            params["competition"] = competition

        if season:
            conditions.append("m.season = $season")
            params["season"] = season

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        query = f"""
        MATCH (m:Match)
        WHERE {where_clause}
        WITH m, abs(m.home_goals - m.away_goals) as goal_diff
        WHERE goal_diff > 0
        RETURN m, goal_diff
        ORDER BY goal_diff DESC, m.total_goals DESC
        LIMIT $limit
        """

        results = self.conn.execute(query, params)

        matches = []
        for r in results:
            m = r["m"]
            hg = m.get("home_goals", 0)
            ag = m.get("away_goals", 0)
            winner = m.get("home_team") if hg > ag else m.get("away_team")
            loser = m.get("away_team") if hg > ag else m.get("home_team")
            winner_goals = max(hg, ag)
            loser_goals = min(hg, ag)

            matches.append({
                "datetime": m.get("datetime"),
                "winner": winner,
                "loser": loser,
                "score": f"{winner_goals}-{loser_goals}",
                "goal_difference": r["goal_diff"],
                "competition": m.get("competition"),
                "season": m.get("season")
            })

        return {
            "total": len(matches),
            "matches": matches
        }

    def get_league_statistics(
        self,
        competition: str,
        season: Optional[int] = None
    ) -> dict:
        """
        Get aggregate statistics for a league.

        Args:
            competition: Competition name
            season: Filter by season

        Returns:
            Dictionary with aggregate statistics
        """
        conditions = ["m.competition CONTAINS $competition"]
        params = {"competition": competition}

        if season:
            conditions.append("m.season = $season")
            params["season"] = season

        where_clause = " AND ".join(conditions)

        query = f"""
        MATCH (m:Match)
        WHERE {where_clause}
        RETURN
            count(m) as total_matches,
            sum(m.home_goals + m.away_goals) as total_goals,
            avg(m.home_goals + m.away_goals) as avg_goals_per_match,
            sum(CASE WHEN m.home_goals > m.away_goals THEN 1 ELSE 0 END) as home_wins,
            sum(CASE WHEN m.away_goals > m.home_goals THEN 1 ELSE 0 END) as away_wins,
            sum(CASE WHEN m.home_goals = m.away_goals THEN 1 ELSE 0 END) as draws
        """

        results = self.conn.execute(query, params)

        if not results:
            return {"error": "No data found"}

        stats = results[0]
        total = stats.get("total_matches", 0)

        return {
            "competition": competition,
            "season": season,
            "total_matches": total,
            "total_goals": stats.get("total_goals", 0),
            "avg_goals_per_match": round(stats.get("avg_goals_per_match", 0), 2),
            "home_wins": stats.get("home_wins", 0),
            "away_wins": stats.get("away_wins", 0),
            "draws": stats.get("draws", 0),
            "home_win_rate": round(stats.get("home_wins", 0) / total * 100, 1) if total else 0,
            "away_win_rate": round(stats.get("away_wins", 0) / total * 100, 1) if total else 0,
            "draw_rate": round(stats.get("draws", 0) / total * 100, 1) if total else 0
        }

    def get_top_scoring_teams(
        self,
        competition: Optional[str] = None,
        season: Optional[int] = None,
        limit: int = 10
    ) -> dict:
        """
        Find teams with the most goals scored.

        Args:
            competition: Filter by competition
            season: Filter by season
            limit: Number of results

        Returns:
            Dictionary with top-scoring teams
        """
        conditions = []
        params = {"limit": limit}

        if competition:
            conditions.append("m.competition CONTAINS $competition")
            params["competition"] = competition

        if season:
            conditions.append("m.season = $season")
            params["season"] = season

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        # Calculate goals for each team as home and away
        query = f"""
        MATCH (m:Match)
        WHERE {where_clause}
        WITH m.home_team as team, m.home_goals as goals
        RETURN team, sum(goals) as total_goals
        ORDER BY total_goals DESC
        LIMIT $limit

        UNION ALL

        MATCH (m:Match)
        WHERE {where_clause}
        WITH m.away_team as team, m.away_goals as goals
        RETURN team, sum(goals) as total_goals
        ORDER BY total_goals DESC
        LIMIT $limit
        """

        # Use a simpler aggregation approach
        query = f"""
        MATCH (m:Match)
        WHERE {where_clause}
        WITH collect({{team: m.home_team, goals: m.home_goals}}) +
             collect({{team: m.away_team, goals: m.away_goals}}) as all_goals
        UNWIND all_goals as g
        WITH g.team as team, g.goals as goals
        RETURN team, sum(goals) as total_goals
        ORDER BY total_goals DESC
        LIMIT $limit
        """

        results = self.conn.execute(query, params)

        return {
            "competition": competition,
            "season": season,
            "teams": [{"team": r["team"], "goals": r["total_goals"]} for r in results]
        }
