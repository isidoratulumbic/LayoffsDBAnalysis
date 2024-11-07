-- Data cleaning with MySQL

SELECT *
FROM layoffs;

-- Kreiramo tabelu koja je ista kao i layoffs, ali nad kojom cemo vrsiti promene, jer bi u realnom svetu
-- bio dosta veliki problem kad bi menjali podatke nad originalnom bazom podataka, a i da ne bi, ovaj metod je preporucljiv, jer ako napravimo neku veliku gresku nad ovim podacima uvek imamo dostupnu original bazu
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Ubacujemo iste podatke u tabelu layyofs staging
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 1. Remove duplicates
-- Pošto u ovom data setu nemamo nikakav id, dodacemo redne brojeve, pa gde se pojavi veći od dva ili jednak dva znaci da je duplikat
-- Takođe ih grupišemo 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Sada od ovoga napravimo CTE, da nam izbaci sve koji imaju row_num veći ili jednak 2
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Proveravamo da li su zaista duplikati
SELECT *
FROM layoffs_staging
WHERE company = 'Spotify';

-- Sada, da bi obrisali same duplikate napravićemo novu tabelu sa istim podacima, ali sa dodatom kolonom row_num i nad njom izbrisati one gde je row_num=2
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 collate=utf8mb4_0900_ai_ci;
    
-- Ubacujemo podatke
insert into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Proveravamo unete podatke
SELECT *
FROM layoffs_staging2;

-- Brišemo kopije
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- SET SQL_SAFE_UPDATES = 0; Iskljucujemo safe updates jer nam Mysql brani da menjamo bazu, ali ovo je svakako kopija glavne tabele
SET SQL_SAFE_UPDATES = 0;
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standardizing data
-- Pronalaženje problema u bazi i popravljanje istih

-- Kod par naziva kompanija postoji razmak na prvom mestu
SELECT company, TRIM(company)
FROM layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Proveravamo sledeće kolone - industry
SELECT distinct industry
FROM layoffs_staging2
ORDER BY 1; -- da nam se pojave prvo null vrednosti

-- Sada imamo firmu istu a nazive crypto, crypto currency i criptocurrency, pa to moramo srediti
-- Proveravamo
SELECT *
FROM layoffs_staging2
where industry LIKE 'Crypto%';

-- Ispravljamo
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
 
 -- Negde kod države United States ima tacka na kraju, brišemo je
 UPDATE layoffs_staging2
 SET country = TRIM(TRAILING '.' FROM country)
 WHERE country LIKE 'United States%';
 
 -- Posto nam je datum tipa text menjamo u date
 UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

 -- Ovim smo samo prebacili u format datuma, ali nam je i dalje tekst
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null values or blank values

-- Pošto u industry polju imamo neke nedostajuće podatke, pogledaćemo da li u nekim drugim redovima gde je ista kompanija, tj isti naziv kompanije, postoji polje za industriju, pa ćemo to dodati kompaniji kojoj je polje industrija prazno
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2

-- Ovim proveravamo da li su kompanije u istoj zemlji, možda imaju iste nazive u različitim zemljama, ali im nije ista industrijska grana
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Ispravljamo
-- Prvo stavljamo gde su null vrednosti da bude prazna polje, da bi lakše obrisali
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Dopunjujemo nedostajuće vrednosti 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
SET t1.industry = t2. industry 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Ostala nam je kompanija Ballys Interactive ali za nju nemamo drugi red sa istom da uporedimo, pa ćemo je izrbiasti, jer svakako ima null vrednosti i za total_laid_off polje
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

DELETE FROM layoffs_staging2
WHERE company LIKE 'Bally%';


-- Takođe brišemo sve redove gde je total laid off i perc laid off null, jer bez njihovih vrednosti nemamo neku analizu, a nemamo ni neke dodatne kolone koje bi nam pomogle da ih dopunimo
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Remove any columns

-- Brisemo kolonu row_num, jer nam vise nije potrebna
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- BAZA SREĐENA:SLEDEĆI KORAK-->ANALIZA PODATAKA

-- Najviše ćemo analizirati polje total laid off, i percentage laid off 

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;
-- 1 je 100%, što je loše

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
order by total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
order by funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
group by company
order by 2 DESC;
-- Zaključujem da je ovo otpuštanje bilo negde između 2020. i 2023. godine, zbog Corona virusa

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Gledamo koje grane industrije je ovaj talas otpuštanja najviše zahvatio
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
group by industry
order by 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
group by country
order by 2 DESC;
-- Najviše je zahvatilo United States, pa Indiju

select YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
order by 1 desc;
-- 2023.

SELECT SUBSTRING(`date`, 1, 7) AS MONTH, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`, 1, 7) AS MONTH, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC 
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;
-- Rangiramo koje godine su otpustili najviše zaposlenih

WITH Company_Year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years is not null
)
select *
from Company_Year_Rank
WHERE Ranking <= 5;
-- Tražimo koliko su kompanije radnika otpustile po godinama
