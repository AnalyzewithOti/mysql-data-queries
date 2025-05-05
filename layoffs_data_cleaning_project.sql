-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- REMOVING DUPLICATES
select *
from layoffs
;

create table layoffs_staging
like layoffs
;

select *
from layoffs_staging
;

insert layoffs_staging
select *
from layoffs
;

select *, 
row_number() over(
partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoffs_staging
;

with duplicate_cte as
(
select *, 
row_number() over(
partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1
;

select *
from layoffs_staging
where company = 'casper'
;

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
;

insert into layoffs_staging2
select *, 
row_number() over(
partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoffs_staging
;

select *
from layoffs_staging2
where row_num > 1
;

SET SQL_SAFE_UPDATES = 0;
delete
from layoffs_staging2
where row_num > 1
;

select *
from layoffs_staging2
;


-- STANDARDIZING DATA
 select company, trim(company)
from layoffs_staging2
;

update layoffs_staging2
set company = trim(company)
;

select distinct industry
from layoffs_staging2
order by 1
;

select *
from layoffs_staging2
where industry like 'Crypto%'
;

update layoffs_staging2
set industry ='Crypto'
where industry like 'Crypto%'
;

select distinct country
from layoffs_staging2
order by 1
;

select *
from layoffs_staging2
where country like 'United States%'
;

update layoffs_staging2
set country = 'United States'
where country like 'United States%'
;

select `date`,
str_to_date 
( 
`date`, '%m/%d/%Y'
)
from layoffs_staging2
;

update layoffs_staging2
set `date` = str_to_date ( `date`, '%m/%d/%Y')
;

alter table layoffs_staging2
modify column `date` DATE
;


-- HANDLING NULL AND MISSING VALUE
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;

select distinct industry
from layoffs_staging2
where industry is null
or industry =''
;

update layoffs_staging2
set industry = null
where industry = ''
;

select *
from layoffs_staging2
where company = 'airbnb'
;

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company =t2.company 
    and t1.location = t2.location 
where t1.industry is null
and t2.industry is not null
;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company =t2.company 
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null
;
 

-- DROPPING A COLUMN
select *
from layoffs_staging2
;

alter table layoffs_staging2
drop column row_num
;












-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
-- first thing I did was to create a staging table. This is the one I worked in and cleaned the data. I wanted a table with the raw data in case something happened
-- now when I clean data I usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways










