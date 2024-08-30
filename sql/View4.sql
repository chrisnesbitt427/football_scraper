WITH home AS (
  SELECT 
    *,
    AVG(matchweek_points) OVER (
      PARTITION BY Side
      ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64)
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS running_5_week_avg
  FROM 
    `my-project-1706650764881.Bundesliga.Points_Table`
  WHERE 
    Venue = "Home"
),

away AS (
  SELECT 
    *,
    AVG(matchweek_points) OVER (
      PARTITION BY Side
      ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64)
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS running_5_week_avg
  FROM 
    `my-project-1706650764881.Bundesliga.Points_Table`
  WHERE 
    Venue = "Away"
),

matches AS (
  SELECT DISTINCT
    Season,
    Round,
    Side
  FROM 
    `my-project-1706650764881.Bundesliga.Points_Table`
),

home_all AS (
  SELECT 
    matches.Season, 
    matches.Round, 
    matches.Side, 
    home.matchweek_points AS home_matchweek_points, 
    home.running_5_week_avg AS home_running_5_week_avg
  FROM 
    matches
  LEFT JOIN 
    home
  ON 
    matches.Season = home.Season AND matches.Round = home.Round AND matches.Side = home.Side
  ORDER BY 
    matches.Season, 
    matches.Side, 
    CAST(REGEXP_EXTRACT(matches.Round, r'\d+$') AS INT64)
),

filled_home_all AS (
  SELECT 
    Season, 
    Round, 
    Side, 
    home_matchweek_points,
    LAST_VALUE(home_matchweek_points IGNORE NULLS) OVER (
      PARTITION BY Side
      ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS filled_home_matchweek_points,
    home_running_5_week_avg,
    LAST_VALUE(home_running_5_week_avg IGNORE NULLS) OVER (
      PARTITION BY Side
      ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS filled_home_running_5_week_avg
  FROM 
    home_all
),

away_all AS (
  SELECT 
    matches.Season, 
    matches.Round, 
    matches.Side, 
    away.matchweek_points AS away_matchweek_points, 
    away.running_5_week_avg AS away_running_5_week_avg
  FROM 
    matches
  LEFT JOIN 
    away
  ON 
    matches.Season = away.Season AND matches.Round = away.Round AND matches.Side = away.Side
  ORDER BY 
    matches.Season, 
    matches.Side, 
    CAST(REGEXP_EXTRACT(matches.Round, r'\d+$') AS INT64)
),

filled_away_all AS (
  SELECT 
    Season, 
    Round, 
    Side, 
    away_matchweek_points,
    LAST_VALUE(away_matchweek_points IGNORE NULLS) OVER (
      PARTITION BY Side
      ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS filled_away_matchweek_points,
    away_running_5_week_avg,
    LAST_VALUE(away_running_5_week_avg IGNORE NULLS) OVER (
      PARTITION BY Side
      ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS filled_away_running_5_week_avg
  FROM 
    away_all
)

SELECT 
  home.Season, 
  home.Round, 
  home.Side,
  ROUND(home.filled_home_running_5_week_avg,1) as `5_game_home_points_av`,
  ROUND(away.filled_away_running_5_week_avg,1) as `5_game_away_points_av`
FROM 
  filled_home_all home
LEFT JOIN 
  filled_away_all away
ON 
  home.Season = away.Season AND home.Round = away.Round AND home.Side = away.Side
ORDER BY 
  home.Season, 
  home.Side, 
  CAST(REGEXP_EXTRACT(home.Round, r'\d+$') AS INT64);
