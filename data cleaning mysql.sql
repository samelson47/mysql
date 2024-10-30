SELECT * 
FROM world_layoffs.layoffs;

-- create a staging table. 
-- we will work in and clean the data. just to keep the raw data so incase if amything goes wrong 

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Data cleaning 
-- 1. check for duplicates and remove 
-- 2. standardise data and fix errors
-- 3. Look at null values and see what can we do with it 
-- 4. remove any columns and rows that are not necessary 

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH Duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging 
)
select*
FROM Duplicate_cte
where row_num>1;

select*
from layoffs_staging
where company= 'casper';

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

SELECT *
FROM layoffs_staging
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
   `row_num`int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SET SQL_SAFE_UPDATES = 0;

select*
from layoffs_staging2 ;

insert into layoffs_staging2
select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) as row_num
from layoffs_staging;

-- removing duplicates

delete
from layoffs_staging2
where row_num>1;

-- standardizing data

select distinct(trim(company))
from layoffs_staging2;

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select*
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry='crypto'
where industry like 'crypto%';

select distinct industry
from layoffs_staging2
order by 1;

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select distinct country ,trim(trailing'.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country=trim(trailing'.' from country)
where country like 'united states%'
;

-- changing format of data col from text to date
 
 select`date`,
 str_to_date(`date`,'%m/%d/%Y')
 from layoffs_staging2;

update layoffs_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y');


-- taking care of of null values  and blank values

select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select distinct industry
from layoffs_staging2
where industry is null
or industry=' ';

select*
from layoffs_staging2
where company='airbnb';

select*
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
where (t1.industry is null or t1.industry=' ')
and t2.industry is not null;

update layoffs_staging2
set industry=null
where industry=' ';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry=' ')
and t2.industry is not null;

select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- removing unwanted columns or rows


 alter table layoffs_staging2
 drop column row_num;

select*
from layoffs_staging2