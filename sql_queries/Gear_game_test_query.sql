-- account_id — id игрока
-- mission_id — id миссии
-- result — результат прохождения (win / lose)
-- duration_sec — длительность прохождения в секундах
-- started_at — время начала миссии
-- install_date — дата установки игры
-- Каждая строка — одно прохождение миссии игроком.

-- Исходил из того, что длительность прохождения миссии нужно считать только для игрока,
-- который прошел миссию. Но при этом для расчета длительности нужно брать все его прохождения до первого успешного включительно.
-- Когда игрок уже один раз прошел миссию, то повторные прохождения даются легче, то есть учитывать их не стоит.

-- код писал под PostgreSQL

WITH filtered AS ( --вообще все нужные по задаче сессии
    SELECT
        account_id,
        mission_id,
        duration_sec,
        result,
        started_at
    FROM missions
    WHERE install_date >= DATE '2021-03-01'
),
ranked AS ( -- отранжировали по дате прохождения
    SELECT
        account_id,
        mission_id,
        duration_sec,
        result,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, mission_id
            ORDER BY started_at
        ) AS attempt_rn
    FROM filtered
),
with_first_win AS ( -- определили первую победу
    SELECT
        *,
        MIN(CASE WHEN result = 'win' THEN attempt_rn END) OVER (
            PARTITION BY account_id, mission_id
        ) AS first_win_rn
    FROM ranked
),

attempts_to_first_win AS ( --оставили только тех, кто хоть раз выиигрывал + все попытки ДО выгрышной
    SELECT
        account_id,
        mission_id,
        duration_sec
    FROM with_first_win
    WHERE first_win_rn IS NOT NULL
      AND attempt_rn <= first_win_rn
),

player_time_to_first_win AS (-- Посчитали суммарное время прохождения на все попытки до выгрышной включительно
    SELECT
        mission_id,
        account_id,
        SUM(duration_sec) AS time_to_first_win
    FROM attempts_to_first_win
    GROUP BY mission_id, account_id
),
avg_duration AS ( -- Посчитали время прохождения
    SELECT
        mission_id,
        AVG(time_to_first_win) AS avg_duration_sec
    FROM player_time_to_first_win
    GROUP BY mission_id
),

player_counts AS ( -- теперь отдельно считаем игроков с 3+ успешными прохождениями по каждой миссии (от прошедших хоть раз)
    SELECT
        mission_id,
        account_id,
        COUNT(*) AS win_count
    FROM filtered
    WHERE result = 'win'
    GROUP BY mission_id, account_id
),
player_share AS (
    SELECT
        mission_id,
        COUNT(*) AS total_players,
        COUNT(*) FILTER (WHERE win_count > 3) AS players_gt_3
    FROM player_counts
    GROUP BY mission_id
)
SELECT
    d.mission_id,
    d.avg_duration_sec,
    ps.players_gt_3::numeric / ps.total_players AS share_players_gt_3
FROM avg_duration d
JOIN player_share ps 
  ON d.mission_id = ps.mission_id
ORDER BY d.mission_id;
