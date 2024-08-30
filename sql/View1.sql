WITH all_match_data_raw as (

WIth match_lookup as (

With match_lookup as (

WITH matches AS (
SELECT * 
FROM `my-project-1706650764881.Bundesliga.Player_Data_final` 
WHERE 
  Min != 'Match Report' AND
  Gls != 'Match Report' AND
  Sh != 'Match Report' AND
  SoT != 'Match Report' AND
  Touches != 'Match Report' AND
  Tkl != 'Match Report' AND
  xG != 'Match Report'

)
SELECT
  Season,
  Round,
  `Home Side`,
  `Away Side`,
  ROW_NUMBER() OVER (ORDER BY Season, CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64), `Home Side`) AS ID,
FROM matches
GROUP BY 
  Season,
  Round,
  `Home Side`,
  `Away Side`
ORDER BY 
  Season, 
  CAST(REGEXP_EXTRACT(Round, r'\d+$') AS INT64), 
  `Home Side`

)

SELECT * FROM match_lookup

),


Player_data_with_match_id as (

With p as(

SELECT * FROM `my-project-1706650764881.Bundesliga.Player_Data_final` 
WHERE 
  Min != 'Match Report' AND
  Gls != 'Match Report' AND
  Sh != 'Match Report' AND
  SoT != 'Match Report' AND
  Touches != 'Match Report' AND
  Tkl != 'Match Report' AND
  xG != 'Match Report'
)

SELECT 
  p.*,
  m.ID

FROM p

LEFT JOIN match_lookup m

ON
    p.Season = m.Season AND 
    p.Round = m.Round AND 
    p.`Home Side` = m.`Home Side` AND 
    p.`Away Side` = m.`Away Side`

ORDER by m.ID
),


home as (

SELECT 
  ID,
  SUM(CAST(Gls AS NUMERIC)) AS home_Gls,
  SUM(CAST(Ast AS NUMERIC)) AS home_Ast,
  SUM(CAST(Sh AS NUMERIC)) AS home_Sh,
  SUM(CAST(SoT AS NUMERIC)) AS home_SoT,
  SUM(CAST(Touches AS NUMERIC)) AS home_Touches,
  SUM(CAST(Tkl AS NUMERIC)) AS home_Tkl,
  SUM(CAST(xG AS NUMERIC)) AS home_xG

FROM Player_data_with_match_id

GROUP BY
  Venue,
  Round,
  Season,
  `Home Side`,
  `Away SIde`,
  ID

HAVING Venue = 'Home'

ORDER BY ID  
),

away as (

SELECT 
  ID,
  SUM(CAST(Gls AS NUMERIC)) AS away_Gls,
  SUM(CAST(Ast AS NUMERIC)) AS away_Ast,
  SUM(CAST(Sh AS NUMERIC)) AS away_Sh,
  SUM(CAST(SoT AS NUMERIC)) AS away_SoT,
  SUM(CAST(Touches AS NUMERIC)) AS away_Touches,
  SUM(CAST(Tkl AS NUMERIC)) AS away_Tkl,
  SUM(CAST(xG AS NUMERIC)) AS away_xG

FROM Player_data_with_match_id

GROUP BY
  Venue,
  Round,
  Season,
  `Home Side`,
  `Away SIde`,
  ID

HAVING Venue = 'Away'

ORDER BY ID
)

SELECT 
 matches.*,
 home.home_Gls,
 home.home_Ast,
 home.home_Sh,
 home.home_SoT,
 home.home_Touches,
 home.home_Tkl,
 home.home_xG,
 away.away_Gls,
 away.away_Ast,
 away.away_Sh,
 away.away_SoT,
 away.away_Touches,
 away.away_Tkl,
 away.away_xG

FROM match_lookup matches

LEFT JOIN home

ON matches.ID = home.ID

LEFT JOIN away

ON matches.ID = away.ID

ORDER BY matches.ID

  
),

rolling_averages as (

WITH demo as (
SELECT 
Season,
Round as Matchweek,
`Home Side` as Home_Side,
`Away Side` as Away_Side,
home_Gls as Home_Goals,
away_Gls as Away_Goals,
home_Sh as Home_Shots,
away_sh as Away_shots,
home_SoT as Home_SoT,
away_SoT as Away_SoT,
home_Touches as Home_Touches,
away_Touches as Away_Touches,
home_Tkl as Home_Tackles,
away_Tkl as Away_Tackles,
home_xG as Home_xG,
away_xG as Away_xG
FROM all_match_data_raw),

CombinedMatches AS (
  SELECT
    Season,
    Matchweek,
    Home_Side AS Side,
    Home_Goals AS Goals,
    Home_Shots AS Shots,
    Home_SoT AS SoT,
    Home_Touches as Touches,
    Home_Tackles as Tackles,
    Home_xG as xG,
    'home' AS Location
  FROM
    demo
  UNION ALL
  SELECT
    Season,
    Matchweek,
    Away_Side AS Side,
    Away_Goals AS Goals,
    Away_shots AS Shots,
    Away_SoT as SoT,
    Away_Touches as Touches,
    Away_Tackles as Tackles,
    Away_xG as xG,
    'away' AS Location
  FROM
    demo
),

RunningSums AS (
  SELECT
    Season,
    Matchweek,
    Side,
    Goals,
    SUM(Goals) OVER (PARTITION BY Side, Season ORDER BY CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS Goals_Sum,
    SUM(Shots) OVER (PARTITION BY Side, Season ORDER BY CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS Shots_Sum,
    SUM(SoT) OVER (PARTITION BY Side, Season ORDER BY CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS SoT_Sum,
    SUM(Touches) OVER (PARTITION BY Side, Season ORDER BY CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS Touches_Sum,
    SUM(Tackles) OVER (PARTITION BY Side, Season ORDER BY CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS Tackles_Sum,
    SUM(xG) OVER (PARTITION BY Side, Season ORDER BY CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS xG_Sum,
    COUNT(Goals) OVER (PARTITION BY Side, Season ORDER BY CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64) ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS Games_Count
  FROM
    CombinedMatches
)

SELECT
  Season,
  Matchweek,
  Side,
  ROUND(Goals_Sum / Games_Count,1) AS Five_Game_Avg_Goals,
  ROUND(Shots_Sum / Games_Count, 1) AS Five_Game_Avg_Shots,
  ROUND(SoT_Sum / Games_Count, 1) AS Five_Game_Avg_SoT,
  ROUND(Touches_Sum / Games_Count, 1) AS Five_Game_Avg_Touches,
  ROUND(tackles_Sum / Games_Count, 1) AS Five_Game_Avg_tackles,
  ROUND(xG_Sum / Games_Count,1)  AS Five_Game_Avg_xG
FROM
  RunningSums
ORDER BY
  Season,
  Side,
  CAST(REGEXP_EXTRACT(Matchweek, r'\d+$') AS INT64)

)

SELECT 

m.*,
h.Five_Game_Avg_Goals as `5_game_home_goals`,
h.Five_Game_Avg_Shots as `5_game_home_shots`,
h.Five_Game_Avg_SoT as `5_game_home_SoT`,
h.Five_Game_Avg_tackles as `5_game_home_tackles`,
h.Five_Game_Avg_Touches as `5_game_home_touches`,
h.Five_Game_Avg_xG as `5_game_home_xG`,
a.Five_Game_Avg_Goals as `5_game_away_goals`,
a.Five_Game_Avg_Shots as `5_game_away_shots`,
a.Five_Game_Avg_SoT as `5_game_away_SoT`,
a.Five_Game_Avg_tackles as `5_game_away_tackles`,
a.Five_Game_Avg_Touches as `5_game_away_touches`,
a.Five_Game_Avg_xG as `5_game_away_xG`,

FROM all_match_data_raw m

LEFT JOIN rolling_averages h

ON m.Season = h.Season AND m.Round = h.Matchweek AND m.`Home Side` = h.Side

LEFT JOIN rolling_averages a

ON m.Season = a.Season AND m.Round = a.Matchweek AND m.`Away Side` = a.Side

ORDER BY m.ID