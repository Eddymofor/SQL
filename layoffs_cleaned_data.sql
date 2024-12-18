--              DATA CLEANING      
select *
from layoffs;

create table layoffs_staging
like layoffs;

insert into layoffs_staging
select *
from layoffs;

-- Created layoffs_staging to avoid working on original dataset

-- To clean Data, we need to:

-- 1. Remove Duplicates
-- 2. Standardise the Data
-- 3. Null Values or Blank Vlues
-- 4. Remove Any columns

-- 1.1 Check for duplicates
-- create cte as num_row with row_number and partition by all columns in data
-- then select * from cte where num_row > 1. Results will be duplicates.

with duplicate_cte as
( select *,
row_number() over( partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as num_row
from layoffs_staging
)
select *
from duplicate_cte
where num_row > 1
;

-- check if duplicates are useful or not

select *
from layoffs_staging
where company = 'casper' ;

-- 1.2 Dropping duplicates
-- To drop duplicates, we create new table layoffs_staging2 with new column (row_num) and insert duplicate_cte values into this table
-- The result of values with row_num> 1 will be duplicates so we delete them.


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- 1.3 populate table with results from duplicate_cte 

insert into layoffs_staging2
 select *,
row_number() over( partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as num_row
from layoffs_staging
;

-- 1.4 check for duplicates
select *
from layoffs_staging2
where row_num > 1;

-- 1.5 deleting the duplicates
delete 
from layoffs_staging2
where row_num > 1;

-- Sanity check 
select *
from layoffs_staging2
where row_num > 1;


-- 2. Standardising data
--  This is just finding and fixing issues with our data

 -- 2.1 Rmovoing blank spaces in the company column.
 
 select company, trim(company)
 from layoffs_staging2;
 
 update layoffs_staging2
 set company = trim(company);
 
 -- 2.2 Ckeck for spelling errors/differences in the industry column
 select distinct industry
 from layoffs_staging2
 order by 1;
 
 -- 2.3 we notice Crypto, and Crypto currency are meant to be the same. 
 
 select *
from layoffs_staging2
where industry like 'crypto%' ;

-- 2.4 Update all Crypto and Crypto Currency in industry to Crypto
update layoffs_staging2
set industry = 'Crypto'
 where industry like 'crypto%';
 
 -- Sanity check 
  select distinct industry
 from layoffs_staging2
 order by 1;
 
 -- 2.5 Peform same check for Country.
select distinct country
from layoffs_staging2
order by 1 ;

-- 2.6 Found one mispelt United States with "." so we remove the "."

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
where country like 'United States%'
order by 1;

-- 2.7 update country column
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';
 
 -- Sanity check 
 select distinct country
from layoffs_staging2
order by 1 ;


-- 2.8 Date column is in text format which could affect any time series forecasts preparatioins
SELECT `date`
FROM layoffs_staging2;

update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter date table, modify column to date

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;


--  3. Removing or populating the null or blank values

-- 3.1 Find the null or blank values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- The values above might be useles to us (both numerative columns are null) so we decide if we wish to remove them later
-- We check for null values in the industry column 


select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb'
;

-- 3.2. Checking for possible data that can be populated in place of null or blank
-- We join layoffs_staging2 as t1 to itself as t2 (ON t1.company = t2.company AND t1.location = t2.location), Where (t1.industry is Null or = '' AND t2.industry is not null ANd != '')
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '');
-- 3.3. Populating the blank or null data by updating table layoffs_staging2 (t1) joining to layoffs_staging2 (t2) on t1.company = t2.company AND t1.location = t2.location, SETTING t1.industry = t2.industry, WHERE t1.industry is NULL OR = '' AND t2.industry is not null AND != '' 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '')
;

-- Sanity Check

select industry
from layoffs_staging2
where company = 'Airbnb';


-- 4 Remooving any unnecessary columns

-- 4.1. Check for missing values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- 4.2. Removing missing values

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Sanity check
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- 4.3. Remove the row_num column added to check for duplicates

alter table layoffs_staging2
drop column row_num;


-- 5. Save final data
create table layoffs_clean_data
like layoffs_staging2;

insert into layoffs_clean_data 
select *
from layoffs_clean_data ;


