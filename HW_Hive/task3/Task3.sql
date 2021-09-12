--Task 3:
-- Write hive script to calculate top 3 most popular hotels 
-- (treat hotel as composite key of continent, country and market) which were not booked. 
-- Most popular means most searches in dataset. Not booked means column booked = 0, 
-- make screenshots before and after script execution, copy hive script to archive.

SELECT 
	hotel_continent, 
	hotel_country, 
	hotel_market 
FROM (
	SELECT 
		hotel_continent, 
		hotel_country, 
		hotel_market, 
		COUNT(DISTINCT is_booking) AS nb_cnt, 
		COUNT(CASE WHEN is_booking = 1 THEN NULL ELSE 1 END) AS plr_cnt -- count only cases were hotel was not booked
	FROM expedia_recommendations.tmp_train
	GROUP BY hotel_continent, hotel_country, hotel_market 
	HAVING COUNT(DISTINCT is_booking) = 1 -- check that hotels were either booked or not booked
	ORDER BY plr_cnt DESC 
	LIMIT 3 ) htl
WHERE plr_cnt IS NOT NULL; -- excluding hotels except that which were not booked