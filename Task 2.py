import pandas as pd
import duckdb
df = pd.read_excel("Файл 1.xlsx")
df['time_req_to_start'] = (df['start_time'] - df['request_time']).dt.total_seconds() / 60
df['time_start_end'] = (df['finish_time'] - df['start_time']).dt.total_seconds() / 60
query_time = """
SELECT *, EXTRACT(HOUR FROM request_time) AS hour_of_request, EXTRACT(HOUR FROM start_time) AS hour_of_start
FROM df
"""
TIME_DIFF = duckdb.query(query_time).to_df()
query_group = """
WITH group_REQ AS(
SELECT hour_of_request, COUNT() AS NUM_OF_REQ
FROM TIME_DIFF
GROUP BY hour_of_request),
group_START AS(
SELECT hour_of_start, COUNT() AS NUM_OF_START, AVG(time_start_end) AS AVG_TIME_SOLVE
FROM TIME_DIFF
GROUP BY hour_of_start),

group_REQ_WHOLESALE AS(
SELECT hour_of_request, COUNT() AS NUM_OF_REQ_WHOLESALE
FROM TIME_DIFF
WHERE team = 'wholesale'
GROUP BY hour_of_request),
group_START_WHOLESALE AS(
SELECT hour_of_start, COUNT() AS NUM_OF_START_WHOLESALE, AVG(time_start_end) AS AVG_TIME_SOLVE_WHOLESALE
FROM TIME_DIFF
WHERE team = 'wholesale'
GROUP BY hour_of_start),

group_REQ_RETAIL AS(
SELECT hour_of_request, COUNT() AS NUM_OF_REQ_RETAIL
FROM TIME_DIFF
WHERE team = 'retail'
GROUP BY hour_of_request),
group_START_RETAIL AS(
SELECT hour_of_start, COUNT() AS NUM_OF_START_RETAIL, AVG(time_start_end) AS AVG_TIME_SOLVE_RETAIL
FROM TIME_DIFF
WHERE team = 'retail'
GROUP BY hour_of_start)

SELECT R.hour_of_request, R.NUM_OF_REQ, COALESCE(S.NUM_OF_START, 0) AS NUM_OF_START, 
                            COALESCE(S.AVG_TIME_SOLVE, 0) AS AVG_TIME_SOLVE, 
                            COALESCE(RW.NUM_OF_REQ_WHOLESALE, 0) AS NUM_OF_REQ_WHOLESALE,
                            COALESCE(SW.NUM_OF_START_WHOLESALE, 0) AS NUM_OF_START_WHOLESALE,
                            COALESCE(SW.AVG_TIME_SOLVE_WHOLESALE, 0) AS AVG_TIME_SOLVE_WHOLESALE,
                            COALESCE(RR.NUM_OF_REQ_RETAIL, 0) AS NUM_OF_REQ_RETAIL,
                            COALESCE(SR.NUM_OF_START_RETAIL, 0) AS NUM_OF_START_RETAIL,
                            COALESCE(SR.AVG_TIME_SOLVE_RETAIL, 0) AS AVG_TIME_SOLVE_RETAIL
FROM group_REQ AS R
LEFT JOIN group_START AS S
ON R.hour_of_request = S.hour_of_start

LEFT JOIN group_REQ_WHOLESALE AS RW
ON R.hour_of_request = RW.hour_of_request

LEFT JOIN group_START_WHOLESALE AS SW
ON R.hour_of_request = SW.hour_of_start

LEFT JOIN group_REQ_RETAIL AS RR
ON R.hour_of_request = RR.hour_of_request

LEFT JOIN group_START_RETAIL AS SR
ON R.hour_of_request = SR.hour_of_start

ORDER BY R.hour_of_request
"""
NUM_OF_MADE_PROC_REQ = duckdb.query(query_group).to_df() #таблиця з годинами і кількістю запитів, і час обробки запиту 

query_about_moderators = """
WITH MOD_TABLE AS(
SELECT moderator, COUNT() AS TOT_NUM_REQUESTS, AVG(time_start_end) AS AVG_TIME_SOLVE_BY_MOD, MAX(time_start_end) AS MAX_TIME_SOLVE_BY_MOD, ANY_VALUE(team) AS TEAM
FROM TIME_DIFF
GROUP BY moderator),
MORE_THAN_5_MIN AS(
SELECT moderator, COUNT() AS TOT_NUM_REQUESTS_LONGER_THAN_5
FROM TIME_DIFF
WHERE time_start_end > 5
GROUP BY moderator),
NUM_OF_REQ_PER_DAY AS(
SELECT moderator, CAST(start_time AS DATE), COUNT() AS NUM_OF_REQ_PER_DAY
FROM TIME_DIFF
GROUP BY moderator, CAST(start_time AS DATE)),
AVG_NUM_OF_REQ_PER_DAY AS(
SELECT moderator, AVG(NUM_OF_REQ_PER_DAY) AS AVG_NUM_OF_REQ_PER_DAY, COUNT() AS NUM_WORK_DAYS
FROM NUM_OF_REQ_PER_DAY
GROUP BY moderator)

SELECT MOD_TABLE.*, COALESCE(MORE_THAN_5_MIN.TOT_NUM_REQUESTS_LONGER_THAN_5,0) AS NUM_LONG_5, COALESCE(((MORE_THAN_5_MIN.TOT_NUM_REQUESTS_LONGER_THAN_5)/(MOD_TABLE.TOT_NUM_REQUESTS)*100),0) AS OUT_OF_BOUND_RATE, AVG.AVG_NUM_OF_REQ_PER_DAY, AVG.NUM_WORK_DAYS
FROM MOD_TABLE
LEFT JOIN MORE_THAN_5_MIN
ON MOD_TABLE.moderator = MORE_THAN_5_MIN.moderator
LEFT JOIN AVG_NUM_OF_REQ_PER_DAY AS AVG
ON MOD_TABLE.moderator = AVG.moderator
ORDER BY MOD_TABLE.TOT_NUM_REQUESTS DESC
"""
MOD_TABLE = duckdb.query(query_about_moderators).to_df()

NUM_OF_MADE_PROC_REQ.to_excel(("Робочий стіл\Кількість запитів і виконань на кожну годину.xlsx"))
MOD_TABLE.to_excel(("Оцінка якості роботи кожного модератора.xlsx"))
