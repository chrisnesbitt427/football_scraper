SELECT 

  a.*,
  b.`5_game_home_points_av`,
  b.`5_game_away_points_av`

FROM `my-project-1706650764881.Bundesliga.Points_Table` a

LEFT JOIN `my-project-1706650764881.Bundesliga.Rolling_home_away_performances` b

ON a.Season = b.Season AND a.Round = b.Round AND a.Side = b.Side

ORDER BY SEASON, CAST(REGEXP_EXTRACT(a.Round, r'\d+$') AS INT64), a.Side