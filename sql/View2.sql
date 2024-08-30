WITH table as (

WITH matches AS (
    SELECT 
        Season,
        Round,
        `Home Side`,
        `Away Side`,
        ID,
        home_Gls,
        away_Gls,
        CASE
            WHEN home_Gls > away_Gls THEN 3
            WHEN home_Gls < away_Gls THEN 0
            ELSE 1
        END AS home_points,
        CASE
            WHEN away_Gls > home_Gls THEN 3
            WHEN away_Gls < home_Gls THEN 0
            ELSE 1
        END AS away_points
    FROM `my-project-1706650764881.Bundesliga.All_match_data_final`
),
Home AS (
    SELECT Season, Round, `Home Side` AS Side, home_points AS points, "Home" as Venue
    FROM matches
),
Away AS (
    SELECT Season, Round, `Away Side` AS Side, away_points AS points, "Away" as Venue
    FROM matches
),

combined as (
SELECT * 
FROM Home 
UNION ALL 
SELECT * 
FROM Away
)

SELECT 
    Season,
    Round,
    Side,
    points,
    Venue,
    SUM(points) OVER (PARTITION BY Season, Side ORDER BY CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64)) AS cumulative_points,
    SUM(points) OVER (PARTITION BY Season, Side ORDER BY CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS Points_Sum,
    COUNT(points) OVER (PARTITION BY Season, Side ORDER BY CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS Points_Count
FROM 
    combined



ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64), Side )

SELECT 
    Season,
    Round,
    Side,
    Venue,
    points as matchweek_points,
    cumulative_points as Points,
    RANK() OVER (PARTITION BY Season, Round ORDER BY cumulative_points DESC) AS Rank,
    Points_Sum / Points_Count as `5_game_points`
FROM 
    table
ORDER BY 
    Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64), Rank;

