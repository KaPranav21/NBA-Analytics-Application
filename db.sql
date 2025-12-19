CREATE TABLE IF NOT EXISTS PlayerStatsRegularSeason (
    player_id INT,
    season_id VARCHAR(10),
    league_id VARCHAR(10),
    team_id INT,
    team_abbreviation VARCHAR(10),
    player_age INT,
    gp INT,
    gs INT,
    min DECIMAL(7,2),        -- increased to avoid out-of-range
    fgm INT,
    fga INT,
    fg_pct DECIMAL(5,3),
    fg3m INT,
    fg3a INT,
    fg3_pct DECIMAL(5,3),
    ftm INT,
    fta INT,
    ft_pct DECIMAL(5,3),
    oreb INT,
    dreb INT,
    reb INT,
    ast INT,
    stl INT,
    blk INT,
    tov INT,
    pf INT,
    pts INT,
    PRIMARY KEY(player_id, season_id, team_id)  -- includes team_id now
);

truncate table playerstatsregularseason;
select * from playerstatsregularseason;