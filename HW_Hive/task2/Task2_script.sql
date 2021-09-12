-- Task 2
-- Write hive script to calculate the longest period of stay of couples with children

SELECT stay_period
FROM (
SELECT abs(datediff(srch_ci, srch_co)) AS stay_period
FROM expedia_recommendations.tmp_train 
WHERE srch_adults_cnt = 2
AND srch_children_cnt > 0 ) prd
ORDER BY stay_period DESC
LIMIT 1
;