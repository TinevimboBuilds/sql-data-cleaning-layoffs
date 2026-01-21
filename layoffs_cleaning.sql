-- Data Cleaning Workflow

-- Step 1: Inspect raw data
SELECT * 
FROM layoffs;

-- Step 2: Create a staging table to protect raw data
CREATE TABLE layoffs_staging LIKE layoffs;

-- Step 3: Copy raw data into staging table
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- Step 4: Check for duplicates using ROW_NUMBER()
SELECT *, 
ROW_NUMBER() OVER(
    PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) AS row_num
FROM layoffs_staging;

-- Step 5: Identify duplicates with more detailed partitioning
WITH duplicate_cte AS (
    SELECT *, 
    ROW_NUMBER() OVER(
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
    FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- Step 6: Create a second staging table with row_num column
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Step 7: Insert data with row numbers into staging2
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- Step 8: Remove duplicate rows (keep only row_num = 1)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Step 9: Standardize company names (remove extra spaces)
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Step 10: Standardize industry values (normalize Crypto variations)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Step 11: Standardize country values (remove trailing dots)
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Step 12: Convert date column from text to proper DATE type
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 13: Handle nulls and blanks in industry
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Step 14: Fill missing industry values using other rows of same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Step 15: Remove rows with no layoff data (both total and percentage missing)
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Step 16: Drop helper column row_num (cleanup)
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final check: cleaned dataset
SELECT * 
FROM layoffs_staging2;
