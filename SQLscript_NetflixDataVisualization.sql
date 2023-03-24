-- SQL Script for visualizing the data needed to answer questions regarding the NETFLIX dataset

-- Question 1
-- Graph 1a & 1b
-- Content growth on Netflix over the year 
SELECT 
	EXTRACT (YEAR FROM date_added) AS year_added,
	COUNT(EXTRACT (YEAR FROM date_added)) AS movie_added_count,
	DENSE_RANK() OVER (ORDER BY COUNT(date_added) DESC) AS rank_by_most_content
FROM netflix_titles
GROUP BY EXTRACT(YEAR FROM date_added)
ORDER BY EXTRACT (YEAR FROM date_added);

-- Graph 1c
-- Most popular month for Netflix to add new content
SELECT 
	EXTRACT(MONTH FROM date_added) AS month_added,
	TO_CHAR(TO_TIMESTAMP((EXTRACT(MONTH FROM date_added))::TEXT, 'MM'), 'Month') AS month_name,
	COUNT(EXTRACT (MONTH FROM date_added)) AS movie_added_count,
	RANK() OVER(ORDER BY COUNT(EXTRACT (MONTH FROM date_added)) DESC) AS rank_by_most_content
FROM netflix_titles
GROUP BY EXTRACT(MONTH FROM date_added)
ORDER BY month_added;


-- Question 2
-- Graph 2a & 2b
-- Graph 2a & 2b_Show Type Percentages 
SELECT 
	DISTINCT show_type, 
	COUNT(show_type) OVER(PARTITION BY show_type),
	COUNT(show_type) OVER() AS total,
	TO_CHAR(
		(100 * (((COUNT(show_type) OVER(PARTITION BY show_type))::float /
			 (COUNT(show_type) OVER())::float)))
		, 'FM99.9'
	) AS percentage
FROM netflix_titles;

-- Preparation for next graph
DROP VIEW IF EXISTS genre_split;
CREATE VIEW genre_split AS
SELECT 
	show_type, 
	title, 
	regexp_split_to_table(genre, ', ') AS genre,
	SUBSTRING(duration FROM '[0-9]+')::BIGINT AS duration
FROM netflix_titles;

-- Graph 2a & 2b_Top 5 Genre for Movie 
SELECT 
	show_type, 
	genre, 
	COUNT(genre) AS movie_count,
	RANK() OVER (PARTITION BY show_type ORDER BY COUNT(genre) DESC) AS rank_by_count
FROM genre_split
WHERE show_type = 'Movie'
GROUP BY show_type, genre
ORDER BY COUNT(genre) DESC
LIMIT 5;

-- Graph 2a & 2b_Top 5 Genre for TV Show
SELECT 
	show_type, 
	genre, 
	COUNT(genre) AS tvshow_count,
	RANK() OVER (PARTITION BY show_type ORDER BY COUNT(genre) DESC) AS rank_by_count
FROM genre_split
WHERE show_type = 'TV Show'
GROUP BY show_type, genre
ORDER BY COUNT(genre) DESC
LIMIT 5;

-- Graph 2c_Movie duration variation sorted by duration
SELECT 
	show_type, 
	duration, 
	COUNT(duration)
FROM netflix_titles
WHERE show_type = 'Movie'
GROUP BY show_type, duration
ORDER BY show_type, SUBSTRING(duration FROM '[0-9]+')::BIGINT;

-- Graph 2c_TV Show duration variation sorted by duration
SELECT 
	show_type, 
	duration, 
	COUNT(duration)
FROM netflix_titles
WHERE show_type = 'TV Show'
GROUP BY show_type, duration
ORDER BY show_type, SUBSTRING(duration FROM '[0-9]+')::BIGINT;

-- Graph 2d
DROP VIEW IF EXISTS int_duration;
CREATE VIEW int_duration AS
SELECT 
	show_type, 
	SUBSTRING(duration FROM '[0-9]+')::BIGINT AS duration,
	CASE
		WHEN (SUBSTRING(duration FROM '[0-9]+')::BIGINT) < 40 
				AND show_type = 'Movie' THEN 'short_film'
		WHEN (SUBSTRING(duration FROM '[0-9]+')::BIGINT) >= 40
				AND show_type = 'Movie' THEN 'feature_film'
	END movie_type
FROM netflix_titles;
SELECT * FROM int_duration;

-- Graph 2d_Movie Type Distribution
-- ShortFilm vs Feature Film ratio
SELECT
	DISTINCT movie_type,
	COUNT(movie_type) OVER(PARTITION BY movie_type),
	TO_CHAR(
		100 * (COUNT(movie_type) OVER(PARTITION BY movie_type)::FLOAT /
			   COUNT(movie_type) OVER()::FLOAT)
		, 'FM99.99') AS percentages
FROM int_duration
WHERE show_type = 'Movie';

-- Graph 2d_Top 10 Popular TV Show Genre (TV Shows that reach 5+ seasons)
SELECT 
	genre, 
	COUNT(genre),
	RANK() OVER(ORDER BY COUNT(genre) DESC) AS rank_by_genre_count
FROM genre_split
WHERE show_type = 'TV Show' AND duration >= 5
GROUP BY genre
ORDER BY COUNT(genre) DESC
LIMIT 10;


-- Question 3
-- Graph 3a & 3b
DROP VIEW IF EXISTS maturity_ratings;
CREATE VIEW maturity_ratings AS
SELECT show_type, rating,
CASE 
	WHEN rating IN ('R', 'TV-MA', 'NC-17', 'NR', 'UR') THEN 'Adults'
	WHEN rating IN ('PG-13', 'TV-14') THEN 'Teens'
	WHEN rating IN ('TV-PG', 'PG', 'G', 'TV-G', 'TV-Y', 'TV-Y7', 'TV-Y7-FV') THEN 'Kids'
END maturity_rating
FROM netflix_titles;

-- Maturity rating ratio
SELECT maturity_rating, show_type, COUNT(maturity_rating),
SUM(COUNT(maturity_rating)) OVER(PARTITION BY maturity_rating) AS total_per_m_rating,
TO_CHAR(
	100 * ((SUM(COUNT(maturity_rating)) OVER(PARTITION BY maturity_rating)) / 
		   (SUM(COUNT(maturity_rating)) OVER ())) 
	, 'FM99.9') AS genre_percentage
FROM maturity_ratings	
GROUP BY show_type, maturity_rating
ORDER BY maturity_rating, show_type;


-- Question 4
-- Graph 4
DROP VIEW IF EXISTS country_split;
CREATE VIEW country_split AS
SELECT show_type, title, 
regexp_split_to_table(country, ', ') AS country
FROM netflix_titles;

-- Top 10 countries with the most content on Netflix sorted by most content
SELECT 
	country, 
	COUNT(country) 
FROM country_split
WHERE country != 'Data Not Available'
GROUP BY country
ORDER BY COUNT(country) DESC
LIMIT 10;

-- Top 5 countries with the most content on Netflix for Movie category
SELECT show_type, country, COUNT(country) 
FROM country_split
WHERE show_type = 'Movie' AND country != 'Data Not Available'
GROUP BY show_type, country
ORDER BY COUNT(country) DESC
LIMIT 5;

-- Top 5 countries with the most content on Netflix for TV Show category
SELECT show_type, country, COUNT(country) 
FROM country_split
WHERE show_type = 'TV Show' AND country != 'Data Not Available'
GROUP BY show_type, country
ORDER BY COUNT(country) DESC
LIMIT 5;


-- Question 5
-- Graph 5a, 5b and 5c
DROP VIEW IF EXISTS dir_split;
CREATE VIEW dir_split AS
SELECT 
	title, 
	country,
	regexp_split_to_table(directors, ', ') AS director
FROM netflix_titles;

DROP VIEW IF EXISTS cast_split;
CREATE VIEW cast_split AS
SELECT 
	title, 
	country,
	regexp_split_to_table(casts, ', ') AS casts
FROM netflix_titles;

-- Graph 5a, 5b and 5c_Top 10 directors with the most appearances
SELECT 
	director, 
	COUNT(director) AS appearance_count  
FROM dir_split
WHERE director != 'Data Not Available'
GROUP BY director
ORDER BY COUNT(director) DESC
LIMIT 10;

-- Graph 5a, 5b and 5c_Top 10 actors with the most appearances
SELECT 
	casts, 
	COUNT(casts) AS appearance_count 
FROM cast_split
WHERE casts != 'Data Not Available'
GROUP BY casts
ORDER BY COUNT(casts) DESC
LIMIT 10;

-- Graph 5d
CREATE VIEW dc_split2 AS
SELECT 
	d.director, 
	c.casts 
FROM dir_split d FULL JOIN cast_split c
ON d.title = c.title;

DROP VIEW IF EXISTS pair;
CREATE VIEW pair AS
SELECT CONCAT(director, ' - ', casts) AS dc_pair 
FROM dc_split2
WHERE (director != 'Data Not Available' AND director IS NOT NULL) AND 
	(casts != 'Data Not Available' AND casts IS NOT NULL);

-- Top 10 director-actor pair (work together on the same movie)
SELECT 
	dc_pair AS director_actor_pair, 
	COUNT(dc_pair) AS number_of_times_theyre_on_the_same_movie 
FROM pair
GROUP BY dc_pair
ORDER BY COUNT(dc_pair) DESC, dc_pair
LIMIT 8;