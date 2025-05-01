import pandas as pd
import duckdb

df = pd.read_excel("Файл 1.xlsx")
df['time_req_to_start'] = (df['start_time'] - df['request_time']).dt.total_seconds() / 60
df['time_start_end'] = (df['finish_time'] - df['start_time']).dt.total_seconds() / 60
df_without_5 = df[5:]
query_of_requests = """
SELECT COUNT(DISTINCT id_request), COUNT(id_request)
FROM df
""" #ПЕРЕВІРИТИ ЧИ ПОВТОРЮЮТЬСЯ ЗАПИТИ, І ЯКЩО ТАК, ТО ОБЄДНАТИ ЇХ В ОДИН

query_time = """
SELECT *, EXTRACT(HOUR FROM request_time) AS hour_of_request, EXTRACT(HOUR FROM start_time) AS hour_of_start
FROM df
"""
TIME_DIFF = duckdb.query(query_time).to_df()

query_of_growing_solv_req = """
WITH GR_SOLV AS(
SELECT CAST(start_time AS DATE) AS DATE, COUNT(*) AS solved_count, team
FROM TIME_DIFF
WHERE moderator IS NOT NULL AND team IN ('retail', 'wholesale')
GROUP BY CAST(start_time AS DATE), team),
GR_REQ AS(
SELECT CAST(request_time AS DATE) AS DATE, COUNT(*) AS num_of_req, team
FROM TIME_DIFF
GROUP BY CAST(request_time AS DATE), team)

SELECT R.DATE, R.num_of_req, R.team AS DEPARTMENT, COALESCE(S.solved_count,0), COALESCE(S.team,R.team) AS TEAM
FROM GR_REQ AS R
LEFT JOIN GR_SOLV AS S
ON R.DATE = S.DATE 
ORDER BY R.DATE
"""
GROW_REQ_SOLV_BY_DATE = duckdb.query(query_of_growing_solv_req).to_df()
#ДАТА ПО ЯКІЙ РОБОТА СЛУЖБИ ПІДТРИМКИ МОЖЕ БУТИ РЕПРЕЗЕНТАТИВНОЮ ДЛЯ ПРЕДІКТУ - 15.11 ДЛЯ РІТЕЙЛУ І 22.11 ДЛЯ ГУРТУ

query_tor_req_start_by_hour = """
WITH REQ_RET AS(
    SELECT hour_of_request, CAST(request_time AS DATE) AS REQ_DATE, COUNT() AS NUM_OF_REQ_RETAIL
    FROM TIME_DIFF
    WHERE team = 'retail'
    GROUP BY hour_of_request,CAST(request_time AS DATE)),
REQ_RETAIL AS(
        SELECT hour_of_request, SUM(NUM_OF_REQ_RETAIL) AS TOTAL_NUM_OF_REQ_RETAIL, AVG(NUM_OF_REQ_RETAIL) AS AVG_NUM_OF_REQ_RETAIL, MAX(NUM_OF_REQ_RETAIL) AS MAX_NUM_OF_REQ_RETAIL
        FROM REQ_RET
        GROUP BY hour_of_request),
START_RET AS(
    SELECT hour_of_start, CAST(start_time AS DATE) AS REQ_DATE, COUNT() AS NUM_OF_START_RETAIL
    FROM TIME_DIFF
    WHERE team = 'retail'
    GROUP BY hour_of_start,CAST(start_time AS DATE)),
START_RETAIL AS(
    SELECT hour_of_start, SUM(NUM_OF_START_RETAIL) AS TOTAL_NUM_OF_START_RETAIL, AVG(NUM_OF_START_RETAIL) AS AVG_NUM_OF_START_RETAIL, MAX(NUM_OF_START_RETAIL) AS MAX_NUM_OF_START_RETAIL
        FROM START_RET
        GROUP BY hour_of_start),

REQ_WS AS(
    SELECT hour_of_request, CAST(request_time AS DATE) AS REQ_DATE, COUNT() AS NUM_OF_REQ_WHOLESALE
    FROM TIME_DIFF
    WHERE team = 'wholesale'
    GROUP BY hour_of_request,CAST(request_time AS DATE)),
REQ_WHOLESALE AS(
        SELECT hour_of_request, SUM(NUM_OF_REQ_WHOLESALE) AS TOTAL_NUM_OF_REQ_WHOLESALE, AVG(NUM_OF_REQ_WHOLESALE) AS AVG_NUM_OF_REQ_WHOLESALE, MAX(NUM_OF_REQ_WHOLESALE) AS MAX_NUM_OF_REQ_WHOLESALE
        FROM REQ_WS
        GROUP BY hour_of_request),
START_WS AS(
    SELECT hour_of_start, CAST(start_time AS DATE) AS REQ_DATE, COUNT() AS NUM_OF_START_WHOLESALE
    FROM TIME_DIFF
    WHERE team = 'wholesale'
    GROUP BY hour_of_start,CAST(start_time AS DATE)),
START_WHOLESALE AS(
    SELECT hour_of_start, SUM(NUM_OF_START_WHOLESALE) AS TOTAL_NUM_OF_START_WHOLESALE, AVG(NUM_OF_START_WHOLESALE) AS AVG_NUM_OF_START_WHOLESALE, MAX(NUM_OF_START_WHOLESALE) AS MAX_NUM_OF_START_WHOLESALE
    FROM START_WS
    GROUP BY hour_of_start),

REPRESENT_REQ_RET AS(
    SELECT hour_of_request, CAST(request_time AS DATE), COUNT() AS NUM_OF_REQ_RETAIL_REP
    FROM TIME_DIFF
    WHERE (team = 'retail') AND (CAST(request_time AS DATE) >= '2020-11-15')
    GROUP BY hour_of_request, CAST(request_time AS DATE)),
REP_AVG_REQ_RET AS(
    SELECT hour_of_request, AVG(NUM_OF_REQ_RETAIL_REP) AS AVG_REQ_RET_REP, MAX(NUM_OF_REQ_RETAIL_REP) AS MAX_START_RET_REP
    FROM REPRESENT_REQ_RET
    GROUP BY hour_of_request),
REPRESENT_START_RET AS(
    SELECT hour_of_start, CAST(start_time AS DATE), COUNT() AS NUM_OF_START_RETAIL_REP
    FROM TIME_DIFF
    WHERE (team = 'retail') AND (CAST(start_time AS DATE) >= '2020-11-15')
    GROUP BY hour_of_start, CAST(start_time AS DATE)),
REP_AVG_START_RET AS(
    SELECT hour_of_start, AVG(NUM_OF_START_RETAIL_REP) AS AVG_START_RET_REP
    FROM REPRESENT_START_RET
    GROUP BY hour_of_start),

REPRESENT_REQ_WS AS(
    SELECT hour_of_request, CAST(request_time AS DATE), COUNT() AS NUM_OF_REQ_WHOLESALE_REP
    FROM TIME_DIFF
    WHERE (team = 'wholesale') AND (CAST(request_time AS DATE) >= '2020-11-22')
    GROUP BY hour_of_request, CAST(request_time AS DATE)),
REP_AVG_REQ_WS AS(
    SELECT hour_of_request, AVG(NUM_OF_REQ_WHOLESALE_REP) AS AVG_REQ_WS_REP, MAX(NUM_OF_REQ_WHOLESALE_REP) AS MAX_START_WS_REP
    FROM REPRESENT_REQ_WS
    GROUP BY hour_of_request),
REPRESENT_START_WS AS(
    SELECT hour_of_start, CAST(start_time AS DATE), COUNT() AS NUM_OF_START_WHOLESALE_REP
    FROM TIME_DIFF
    WHERE (team = 'wholesale') AND (CAST(start_time AS DATE) >= '2020-11-22')
    GROUP BY hour_of_start, CAST(start_time AS DATE)),
REP_AVG_START_WS AS(
    SELECT hour_of_start, AVG(NUM_OF_START_WHOLESALE_REP) AS AVG_START_WS_REP
    FROM REPRESENT_START_WS
    GROUP BY hour_of_start)



SELECT RR.hour_of_request, 
            RR.TOTAL_NUM_OF_REQ_RETAIL, RR.AVG_NUM_OF_REQ_RETAIL, RR.MAX_NUM_OF_REQ_RETAIL,
            COALESCE(SR.TOTAL_NUM_OF_START_RETAIL,0) ASTOTAL_NUM_OF_START_RETAIL, COALESCE(SR.AVG_NUM_OF_START_RETAIL,0) AS AVG_NUM_OF_START_RETAIL, COALESCE(SR.MAX_NUM_OF_START_RETAIL,0) AS MAX_NUM_OF_START_RETAIL,
            RW.TOTAL_NUM_OF_REQ_WHOLESALE, RW.AVG_NUM_OF_REQ_WHOLESALE, RW.MAX_NUM_OF_REQ_WHOLESALE,
            COALESCE(SW.TOTAL_NUM_OF_START_WHOLESALE,0) AS TOTAL_NUM_OF_START_WHOLESALE, COALESCE(SW.AVG_NUM_OF_START_WHOLESALE,0) AS AVG_NUM_OF_START_WHOLESALE, COALESCE(SW.MAX_NUM_OF_START_WHOLESALE,0) AS MAX_NUM_OF_START_WHOLESALE,
            RARR.AVG_REQ_RET_REP, RARR.MAX_START_RET_REP, COALESCE(RASR.AVG_START_RET_REP, 0) AS AVG_START_RET_REP, 
            RARW.AVG_REQ_WS_REP, RARW.MAX_START_WS_REP, COALESCE(RASW.AVG_START_WS_REP, 0) AS AVG_START_WS_REP

FROM REQ_RETAIL AS RR
LEFT JOIN START_RETAIL AS SR
ON RR.hour_of_request = SR.hour_of_start
LEFT JOIN REQ_WHOLESALE AS RW
ON RR.hour_of_request = RW.hour_of_request
LEFT JOIN START_WHOLESALE AS SW
ON RR.hour_of_request = SW.hour_of_start
LEFT JOIN REP_AVG_REQ_RET AS RARR
ON RR.hour_of_request = RARR.hour_of_request
LEFT JOIN REP_AVG_START_RET AS RASR
ON RR.hour_of_request = RASR.hour_of_start
LEFT JOIN REP_AVG_REQ_WS AS RARW
ON RR.hour_of_request = RARW.hour_of_request
LEFT JOIN REP_AVG_START_WS AS RASW
ON RR.hour_of_request = RASW.hour_of_start

ORDER BY RR.hour_of_request
"""
TOT_NUM_REQ_AND_STARTS_BY_HOURS = duckdb.query(query_tor_req_start_by_hour).to_df()

query_for_hours_info = """
WITH per_day AS(
SELECT moderator, hour_of_start, CAST(start_time AS DATE) as date, COUNT()  AS NUM_OF_MADE_REQ
FROM TIME_DIFF
WHERE hour_of_start >=7 AND hour_of_start <=22
GROUP BY moderator,hour_of_start, CAST(start_time AS DATE))

SELECT moderator, hour_of_start, ROUND(AVG(NUM_OF_MADE_REQ),0) AS AVG_NUM_OF_MADE_REQ
FROM per_day
GROUP BY moderator,hour_of_start
"""


HOURS_DATA = duckdb.query(query_for_hours_info).to_df()
pivot_hours_data = HOURS_DATA.pivot_table(
    index = 'hour_of_start',
    columns = 'moderator',
    values = 'AVG_NUM_OF_MADE_REQ',
    fill_value = 0
)

pivot_hours_data_7_14 = pivot_hours_data[(pivot_hours_data.index>=7)&(pivot_hours_data.index <=14)]
hours_7_14 = pivot_hours_data_7_14.loc[:,(pivot_hours_data_7_14>0).any(axis=0)]
hours_7_14_data = hours_7_14.loc[:, hours_7_14.apply(lambda x: (x == 0).sum() <= 5, axis = 0)]

pivot_hours_data_15_22 = pivot_hours_data[(pivot_hours_data.index>14)&(pivot_hours_data.index <=22)]
hours_15_22 = pivot_hours_data_15_22.loc[:,(pivot_hours_data_15_22>0).any(axis=0)]
hours_15_22_data = hours_15_22.loc[:, hours_15_22.apply(lambda x: (x == 0).sum() <= 5, axis = 0)]

list_7_14 = hours_7_14_data.columns.tolist()
list_15_22 = hours_15_22_data.columns.tolist()
both_list = list(set(list_7_14) & set(list_15_22)) 
only_7_14 = list(set(list_7_14) - set(both_list))
only_15_22 = list(set(list_15_22) - set(both_list))

# df_with_avg_and_sum_per_day_7_14 = hours_7_14_data.copy()
# df_with_avg_and_sum_per_day_7_14.loc['sum_per_day'] = hours_7_14_data.sum(axis=0)
# df_with_avg_and_sum_per_day_7_14.loc['avg_per_day'] = hours_7_14_data.mean(axis=0)

# df_with_avg_and_sum_per_day_15_22 = hours_15_22_data.copy()
# df_with_avg_and_sum_per_day_15_22.loc['sum_per_day'] = hours_15_22_data.sum(axis=0)
# df_with_avg_and_sum_per_day_15_22.loc['avg_per_day'] = hours_15_22_data.mean(axis=0)

info_work_hours = TIME_DIFF.copy()
def in_which_shift(val):
    if val in both_list:
       return 'in both'
    elif val in only_7_14:
       return '7-14'
    elif val in only_15_22:
       return '15-22'
    else:
       return 'Not found'

info_work_hours['shift'] = info_work_hours['moderator'].apply(in_which_shift)


#ОЦІНКА ТОГО, ЯК ПРАЦЮЮТЬ ЛЮДИ,ЯКІ В ДВІ ЗМІНИ - ЦІЛИЙ ДЕНЬ ЧИ ЗМІНЮЮТЬ ЧЕРГИ
query_to_work_hours_in_both = """
WITH COUNTED AS (
    SELECT moderator, CAST(start_time AS DATE) AS WORK_DATE, COUNT(DISTINCT hour_of_start) AS NUM_OCCUR
    FROM info_work_hours
    WHERE shift = 'in both'
    GROUP BY moderator, WORK_DATE)

SELECT moderator, AVG(NUM_OCCUR) AS AVG_COUNT, QUANTILE(NUM_OCCUR, 0.75) AS QRTL3, MAX(NUM_OCCUR) AS MAX_COUNT
FROM COUNTED
GROUP BY moderator"""
# print(duckdb.query(query_to_work_hours_in_both).to_df()) ПРАЦЮЮТЬ ЗМІНЮЮЧИСЬ

query_to_EXCLUDE_IMPLICIT_7_14 = """
WITH LIST_OF_HOURS AS(
   SELECT UNNEST(GENERATE_SERIES(7, 14)) AS hour_of_start
),
LIST_OF_DATES AS(
    SELECT DISTINCT moderator, CAST(start_time AS DATE) AS work_date
    FROM info_work_hours
    WHERE shift = '7-14'),
LIST_WITH_DATES AS(
    SELECT moderator, hour_of_start, CAST(start_time AS DATE) AS work_date, COUNT()  AS NUM_OF_MADE_REQ
    FROM info_work_hours
    WHERE (shift = '7-14') AND (hour_of_start >=7) AND (hour_of_start <=14)
    GROUP BY moderator,hour_of_start, CAST(start_time AS DATE)),
EXPECTED_GRID AS(
    SELECT LD.moderator, H.hour_of_start, LD.work_date
    FROM LIST_OF_HOURS AS H
    CROSS JOIN LIST_OF_DATES AS LD)

   SELECT G.hour_of_start, G.work_date, D.moderator, COALESCE(D.NUM_OF_MADE_REQ, 0) AS NUM_OF_MADE_REQ
   FROM EXPECTED_GRID AS G
   LEFT JOIN LIST_WITH_DATES AS D
   ON G.hour_of_start = D.hour_of_start AND G.work_date = D.work_date AND G.moderator = D.moderator"""

EXCLUDE_IMPLICIT_7_14 = duckdb.query(query_to_EXCLUDE_IMPLICIT_7_14).to_df()
query_to_avg_per_hour_7_14 ="""
WITH REQ_PER_DAY AS (
  SELECT moderator, work_date, SUM(NUM_OF_MADE_REQ) AS TOTAL_REQ_IN_DAY
  FROM EXCLUDE_IMPLICIT_7_14
  GROUP BY moderator, work_date),
AVG_REQ_PER_DAY AS(
    SELECT moderator, AVG(TOTAL_REQ_IN_DAY) AS AVG_REQ_PER_DAY_as_avg_by_date
    FROM REQ_PER_DAY
    GROUP BY moderator
),
AVG_PER_HOUR AS(
SELECT moderator, hour_of_start, ROUND(AVG(NUM_OF_MADE_REQ),0) AS AVG_NUM_OF_MADE_REQ
    FROM EXCLUDE_IMPLICIT_7_14 
    GROUP BY moderator, hour_of_start),
TOTAL_INFO_PER_HOURS AS(
    SELECT moderator, SUM(AVG_NUM_OF_MADE_REQ)  AS TOT_PER_DAY_as_sum_per_hour, AVG(AVG_NUM_OF_MADE_REQ) AS AVG_PER_HOUR
    FROM AVG_PER_HOUR
    GROUP BY moderator)

SELECT PD.moderator, PD.AVG_REQ_PER_DAY_as_avg_by_date, PH.TOT_PER_DAY_as_sum_per_hour, PH.AVG_PER_HOUR
FROM AVG_REQ_PER_DAY AS PD
LEFT JOIN TOTAL_INFO_PER_HOURS AS PH
ON PD.moderator = PH.moderator"""

query_to_EXCLUDE_IMPLICIT_15_22 = """
WITH LIST_OF_HOURS AS(
   SELECT UNNEST(GENERATE_SERIES(15,22)) AS hour_of_start
),
LIST_OF_DATES AS(
    SELECT DISTINCT moderator, CAST(start_time AS DATE) AS work_date
    FROM info_work_hours
    WHERE shift = '15-22'),
LIST_WITH_DATES AS(
    SELECT moderator, hour_of_start, CAST(start_time AS DATE) AS work_date, COUNT()  AS NUM_OF_MADE_REQ
    FROM info_work_hours
    WHERE (shift = '15-22') AND (hour_of_start >=15) AND (hour_of_start <=22)
    GROUP BY moderator,hour_of_start, CAST(start_time AS DATE)),
EXPECTED_GRID AS(
    SELECT LD.moderator, H.hour_of_start, LD.work_date
    FROM LIST_OF_HOURS AS H
    CROSS JOIN LIST_OF_DATES AS LD)

SELECT G.hour_of_start, G.work_date, D.moderator, COALESCE(D.NUM_OF_MADE_REQ, 0) AS NUM_OF_MADE_REQ
FROM EXPECTED_GRID AS G
LEFT JOIN LIST_WITH_DATES AS D
ON G.hour_of_start = D.hour_of_start AND G.work_date = D.work_date AND G.moderator = D.moderator"""
EXCLUDE_IMPLICIT_15_22 = duckdb.query(query_to_EXCLUDE_IMPLICIT_15_22).to_df()

query_to_avg_per_hour_15_22 = """
WITH REQ_PER_DAY AS (
  SELECT moderator, work_date, SUM(NUM_OF_MADE_REQ) AS TOTAL_REQ_IN_DAY
  FROM EXCLUDE_IMPLICIT_15_22
  GROUP BY moderator, work_date),
AVG_REQ_PER_DAY AS(
    SELECT moderator, AVG(TOTAL_REQ_IN_DAY) AS AVG_REQ_PER_DAY_as_avg_by_date
    FROM REQ_PER_DAY
    GROUP BY moderator
),
AVG_PER_HOUR AS(
SELECT moderator, hour_of_start, ROUND(AVG(NUM_OF_MADE_REQ),0) AS AVG_NUM_OF_MADE_REQ
    FROM EXCLUDE_IMPLICIT_15_22
    GROUP BY moderator, hour_of_start),
TOTAL_INFO_PER_HOURS AS(
    SELECT moderator, SUM(AVG_NUM_OF_MADE_REQ)  AS TOT_PER_DAY_as_sum_per_hour, AVG(AVG_NUM_OF_MADE_REQ) AS AVG_PER_HOUR
    FROM AVG_PER_HOUR
    GROUP BY moderator)

SELECT PD.moderator, PD.AVG_REQ_PER_DAY_as_avg_by_date, PH.TOT_PER_DAY_as_sum_per_hour, PH.AVG_PER_HOUR
FROM AVG_REQ_PER_DAY AS PD
LEFT JOIN TOTAL_INFO_PER_HOURS AS PH
ON PD.moderator = PH.moderator"""
AVG_START_PER_HOUR_1_SHIFT = duckdb.query(query_to_avg_per_hour_7_14).to_df().dropna()
AVG_START_PER_HOUR_2_SHIFT = duckdb.query(query_to_avg_per_hour_15_22).to_df().dropna()
query_tot ="""
SELECT *
FROM AVG_START_PER_HOUR_1_SHIFT
UNION 
SELECT *
FROM AVG_START_PER_HOUR_2_SHIFT"""

df_with_tot_and_avg_answers = duckdb.query(query_tot).to_df()

query_for_evaluate_time_solv = '''
WITH TABLE_FOR_ANALYTICS AS(
    SELECT moderator, time_start_end, hour_of_request, team, shift
    FROM info_work_hours),
TIME_SOLV AS(
   SELECT moderator, AVG(time_start_end) AS AVG_TIME_SOLV, MEDIAN(time_start_end) AS MED_TIME_SOLV, MAX(time_start_end) AS MAX_TIME_SOLV
   FROM TABLE_FOR_ANALYTICS
   GROUP BY moderator),
TIME_SOLV_REQ_WORK_HOURS AS(
   SELECT moderator, AVG(time_start_end) AS AVG_TIME_SOLV_WH, MEDIAN(time_start_end) AS MED_TIME_SOLV_WH, MAX(time_start_end) AS MAX_TIME_SOLV_WH
   FROM TABLE_FOR_ANALYTICS
   WHERE hour_of_request BETWEEN 7 AND 22
   GROUP BY moderator
),
TOT_NUM AS(
   SELECT moderator, COUNT() AS TOT_NUM_SOLV
   FROM TABLE_FOR_ANALYTICS
   GROUP BY moderator),
TOT_NUM_OUT_OF_BOUND AS(
   SELECT moderator, COUNT() AS TOT_NUM_SOLV_OUT
   FROM TABLE_FOR_ANALYTICS
   WHERE time_start_end > 5
   GROUP BY moderator)

SELECT DISTINCT T.moderator, TN.TOT_NUM_SOLV, ((OB.TOT_NUM_SOLV_OUT/TN.TOT_NUM_SOLV)*100) AS OUT_OF_5_RATE, TS.AVG_TIME_SOLV, TS.MED_TIME_SOLV, TS.MAX_TIME_SOLV, WH.AVG_TIME_SOLV_WH, WH.MED_TIME_SOLV_WH, WH.MAX_TIME_SOLV_WH, T.team, T.shift
FROM TABLE_FOR_ANALYTICS AS T
LEFT JOIN TIME_SOLV AS TS
ON T.moderator = TS.moderator
LEFT JOIN TOT_NUM AS TN
ON T.moderator = TN.moderator
LEFT JOIN TOT_NUM_OUT_OF_BOUND AS OB
ON T.moderator = OB.moderator
LEFT JOIN TIME_SOLV_REQ_WORK_HOURS AS WH
ON T.moderator = WH.moderator
'''
SOLWING_PROBLEMS_BY_MOD = duckdb.query(query_for_evaluate_time_solv).to_df()

query_for_evaluate_start = """
WITH TABLE_FOR_ANALYTICS AS(
    SELECT moderator, time_req_to_start, hour_of_request, team, shift, CAST(start_time AS DATE) AS DATE
    FROM info_work_hours),
TIME_START AS(
   SELECT moderator, AVG(time_req_to_start) AS AVG_TIME_START, quantile_cont(time_req_to_start, 0.25) AS QUART1_TIME_START, MEDIAN(time_req_to_start) AS MED_TIME_START, quantile_cont(time_req_to_start, 0.75) AS QUART3_TIME_START,MAX(time_req_to_start) AS MAX_TIME_START
   FROM TABLE_FOR_ANALYTICS
   GROUP BY moderator),
TIME_START_REQ_WORK_HOURS AS(
   SELECT moderator, AVG(time_req_to_start) AS AVG_TIME_START_WH, MEDIAN(time_req_to_start) AS MED_TIME_START_WH, MAX(time_req_to_start) AS MAX_TIME_START_WH
   FROM TABLE_FOR_ANALYTICS
   WHERE hour_of_request BETWEEN 7 AND 22
   GROUP BY moderator
),
TIME_START_REPRESENTATIVE AS(
   SELECT moderator, AVG(time_req_to_start) AS AVG_TIME_START_REP, MEDIAN(time_req_to_start) AS MED_TIME_START_REP, MAX(time_req_to_start) AS MAX_TIME_START_REP
   FROM TABLE_FOR_ANALYTICS
   WHERE DATE >= '2020-11-22'
   GROUP BY moderator),
TOT_NUM AS(
   SELECT moderator, COUNT() AS TOT_NUM_START
   FROM TABLE_FOR_ANALYTICS
   GROUP BY moderator),
TOT_NUM_OUT_OF_BOUND AS(
   SELECT moderator, COUNT() AS TOT_NUM_START_OUT
   FROM TABLE_FOR_ANALYTICS
   WHERE time_req_to_start > 45
   GROUP BY moderator)

SELECT DISTINCT T.moderator, TN.TOT_NUM_START, ((OB.TOT_NUM_START_OUT/TN.TOT_NUM_START)*100) AS OUT_OF_5_RATE, TS.AVG_TIME_START, TS.QUART1_TIME_START, TS.MED_TIME_START, TS.QUART1_TIME_START, TS.MAX_TIME_START, WH.AVG_TIME_START_WH, WH.MED_TIME_START_WH, WH.MAX_TIME_START_WH, REP.AVG_TIME_START_REP, REP.MED_TIME_START_REP, REP.MAX_TIME_START_REP, T.team, T.shift
FROM TABLE_FOR_ANALYTICS AS T
LEFT JOIN TIME_START AS TS 
ON T.moderator = TS.moderator
LEFT JOIN TOT_NUM AS TN
ON T.moderator = TN.moderator
LEFT JOIN TIME_START_REPRESENTATIVE AS REP
ON T.moderator = REP.moderator
LEFT JOIN TOT_NUM_OUT_OF_BOUND AS OB
ON T.moderator = OB.moderator
LEFT JOIN TIME_START_REQ_WORK_HOURS AS WH 
ON T.moderator = WH.moderator
"""
START_PROCES_REQ_BY_MOD = duckdb.query(query_for_evaluate_start).to_df()

query_to_count_load ="""
SELECT
  CASE
    WHEN hour_of_request BETWEEN 7 AND 14 THEN '07-14'
    WHEN hour_of_request BETWEEN 15 AND 22 THEN '15-22'
    WHEN hour_of_request >= 23 OR hour_of_request <= 6 THEN '23-06'
  END AS time_slot,
  SUM(AVG_REQ_RET_REP) AS TOTAL_AVG_RET_PER_SHIFT, SUM(MAX_START_RET_REP) AS TOTAL_MAX_RET_PER_SHIFT, SUM(AVG_REQ_WS_REP) AS TOTAL_AVG_WS_PER_SHIFT, SUM(MAX_START_WS_REP) AS TOTAL_MAX_RET_WS_SHIFT,
  CEIL(((TOTAL_AVG_RET_PER_SHIFT+TOTAL_MAX_RET_PER_SHIFT)/2)/10)*10 AS EXPECTED_REQ_RET, CEIL(((TOTAL_AVG_WS_PER_SHIFT+TOTAL_MAX_RET_WS_SHIFT)/2)/10)*10 AS EXPECTED_REQ_WS
FROM TOT_NUM_REQ_AND_STARTS_BY_HOURS
GROUP BY time_slot
ORDER BY 
  CASE time_slot 
    WHEN '07-14' THEN 1
    WHEN '15-22' THEN 2
    WHEN '23-06' THEN 3
  END
"""
EXPECTED_NUM_REQ = duckdb.query(query_to_count_load).to_df()

query_to_info_by_mod ="""
WITH NUM_OF_ANSW_PER_DAY AS(
    SELECT moderator, CAST(start_time AS DATE), COUNT() AS NUM_OF_ANSW_PER_DAY
    FROM TIME_DIFF
    GROUP BY moderator, CAST(start_time AS DATE)),
AVG_NUM_OF_ANSW_PER_DAY AS(
    SELECT moderator, AVG(NUM_OF_ANSW_PER_DAY) AS AVG_NUM_OF_ANSW_PER_DAY, COUNT() AS NUM_WORK_DAYS
    FROM NUM_OF_ANSW_PER_DAY
    GROUP BY moderator)

SELECT ANA.*, COALESCE(DF.AVG_REQ_PER_DAY_as_avg_by_date, ANA.AVG_NUM_OF_ANSW_PER_DAY) AS TOT_PER_DAY, DF.AVG_PER_HOUR
FROM AVG_NUM_OF_ANSW_PER_DAY AS ANA
LEFT JOIN df_with_tot_and_avg_answers AS DF
ON ANA.moderator = DF.moderator"""
MOD_TABLE = duckdb.query(query_to_info_by_mod).to_df()


# GROW_REQ_SOLV_BY_DATE 
# ДАТА ПО ЯКІЙ РОБОТА СЛУЖБИ ПІДТРИМКИ МОЖЕ БУТИ РЕПРЕЗЕНТАТИВНОЮ ДЛЯ ПРЕДІКТУ - 15.11 ДЛЯ РІТЕЙЛУ І 22.11 ДЛЯ ГУРТУ

# TOT_NUM_REQ_AND_STARTS_BY_HOURS 
# ТАБЛИЦЯ, В ЯКІЙ ПО ГОДИНАХ ЗАГАЛЬНА КІЛЬКІСТЬ ЗАПИТІВ, СЕРЕДЕДНЯ І МАКСИМАЛЬНА ПО ДНЯХ. АНАЛОГІЧНО З КІЛЬКІСТЮ ОБРОБЛЕНИХ ЗАПИТІВ.
# А ТАКОЖ ЦІ Ж ДАНІ ПО РЕПРЕЗЕНТАТИВНІЙ КІЛЬКОСТІ ДНІВ

# df_with_tot_and_avg_answers
# ТАБЛИЦЯ З МОДЕРАТОРАМИ І КІЛЬКІСТЮ ЗАПИІВ ЗРОБЛЕНИХ В РОБОЧІ ГОДИНИ

# info_work_hours
# ЯК ТАЙМ ДІФ, ТІЛЬКИ ЗІ ЗМІНАМИ(shift)

# START_PROCES_REQ_BY_MOD
# SOLWING_PROBLEMS_BY_MOD
# СЕРЕДНІЙ, МАКСИМАЛЬНИЙ ЧАС, МЕДІАНА, ВІДНОШЕННЯ ВИХОДУ ЗА ОЧІКУВАНІ РАМКИ

# EXPECTED_NUM_REQ 
# ОЦІНКА КІЛЬКОСТІ ЗАПИТІВ ПО КОЖНІЙ ЗМІНІ

# MOD_TABLE
# ТАБЛИЦЯ З СЕРЕДНЬОЮ КІЛЬКІСТЮ ЗАПИТІВ ДЛЯ МОДЕРАТОРА
query_to_connect_all_mod_info =""" 
SELECT MOD_TABLE.*,START_PROCES_REQ_BY_MOD.*, SOLWING_PROBLEMS_BY_MOD.*
FROM MOD_TABLE
LEFT JOIN START_PROCES_REQ_BY_MOD USING (moderator)
LEFT JOIN SOLWING_PROBLEMS_BY_MOD USING (moderator)"""
TOTAL_MOD_TABLE = duckdb.query(query_to_connect_all_mod_info).to_df()

path_to_excel = r"Повний аналіз роботи служби підтримки.xlsx"

with pd.ExcelWriter(path_to_excel, engine='openpyxl') as writer:
    GROW_REQ_SOLV_BY_DATE.to_excel(writer, sheet_name="Кількісьть запитів і розв'язань", index=False)
    TOT_NUM_REQ_AND_STARTS_BY_HOURS.to_excel(writer, sheet_name='Запити та обробка (год)', index=False)
    TOTAL_MOD_TABLE.to_excel(writer, sheet_name='Вся інформація по модератору', index=False)
    EXPECTED_NUM_REQ.to_excel(writer, sheet_name='Очікувана к-сть запитів', index=False)
    START_PROCES_REQ_BY_MOD.to_excel(writer, sheet_name='Час обробки модератором', index=False)
    SOLWING_PROBLEMS_BY_MOD.to_excel(writer, sheet_name='Проблемні запити', index=False)
    MOD_TABLE.to_excel(writer, sheet_name='Середнє по модераторах', index=False)