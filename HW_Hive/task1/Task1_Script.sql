-- Task 1
-- Write hive script to calculate Top 3 most popular countries where booking is successful (booking = 1)
SELECT hotel_country 
FROM expedia_recommendations.tmp_train 
WHERE is_booking = 1
GROUP BY hotel_country 
ORDER BY COUNT (1) DESC
LIMIT 3