-- Check the transformed data
SELECT * FROM population LIMIT 10;

-- Query to calculate the percentage of population aged 60+ years
SELECT year, country, population_60_plus_percent
FROM population
WHERE year = 2020;
