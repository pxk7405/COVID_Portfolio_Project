SELECT TOP (1000) [iso_code]
      ,[continent]
      ,[location]
      ,[date]
      ,[population]
      ,[total_cases]
      ,[new_cases]
      ,[new_cases_smoothed]
      ,[total_deaths]
      ,[new_deaths]
      ,[new_deaths_smoothed]
      ,[total_cases_per_million]
      ,[new_cases_per_million]
      ,[new_cases_smoothed_per_million]
      ,[total_deaths_per_million]
      ,[new_deaths_per_million]
      ,[new_deaths_smoothed_per_million]
      ,[reproduction_rate]
      ,[icu_patients]
      ,[icu_patients_per_million]
      ,[hosp_patients]
      ,[hosp_patients_per_million]
      ,[weekly_icu_admissions]
      ,[weekly_icu_admissions_per_million]
      ,[weekly_hosp_admissions]
      ,[weekly_hosp_admissions_per_million]
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]

 --SELECT * From [COVID Portfolio Project].[dbo].[CovidVaccination$]
 --order by 3,4;

 SELECT * FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
 order by 3,4;

  SELECT location, date, total_cases,new_cases, total_deaths, population
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  order by 1,2;

  ---TOTAL CASES VS TOTAL DEATHS--

  SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_percentage
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  WHERE LOCATION LIKE '%states%'
  order by 1,2;

----another way---
  SELECT 
  location, 
  date, 
  total_cases, 
  total_deaths, 
  (CAST(total_deaths AS FLOAT) / NULLIF(CAST(total_cases AS FLOAT), 0)) * 100 AS death_percentage
FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
ORDER BY 1, 2;

--Total cases vs Population--

  SELECT location, date, population, total_cases, (total_cases/population)*100 as case_percentage
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  --WHERE continent IS NULL
  --WHERE LOCATION LIKE '%states%'
  order by 1,2;

--countries with Highest infection rate compared to population--
  SELECT location, population, max(total_cases) as HighestInfectionCount, max((total_cases/population))*100 as InfectedPopulation_percentage
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  --WHERE LOCATION LIKE '%states%'
  GROUP BY location, population
  order by InfectedPopulation_percentage DESC;

  SELECT location, max(CAST(total_deaths AS INT)) as HighestdDeathCount 
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  --WHERE LOCATION LIKE '%Asia%'
  where continent IS NOT NULL
  GROUP BY location
  order by HighestdDeathCount DESC;

 SELECT continent,location FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
 where continent is NOT null 

   SELECT location, max(CAST(total_deaths AS INT)) as HighestdDeathCount 
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  --WHERE LOCATION LIKE '%Asia%'
  where continent IS NULL
  GROUP BY location
  order by HighestdDeathCount DESC;

  SELECT 
  continent, 
  location,
  CASE 
    WHEN continent IS NULL THEN 'Summary/Region'
    ELSE 'Country'
  END AS RowType
FROM [COVID Portfolio Project].[dbo].[CovidDeaths$];

   SELECT continent, max(CAST(total_deaths AS INT)) as HighestdDeathCount 
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  --WHERE LOCATION LIKE '%Asia%'
  where continent IS NOT NULL
  GROUP BY continent
  order by HighestdDeathCount DESC;

  
  SELECT date, Sum(new_cases), total_deaths, (total_deaths/total_cases)*100 as death_percentage
  FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
  WHERE continent is not null 
  group by date
  order by 1,2;

  select date, SUM(total_cases), SUM(cast(total_deaths as int)), SUM(new_cases),SUM(CAST(new_deaths AS INT))
   FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
   group by date
   ORDER BY date;

   SELECT 
    date,
    SUM(total_cases) AS total_cases,
    SUM(cast(total_deaths as int)) AS total_deaths,
    (SUM(cast(total_deaths as int)) * 100.0 / SUM(total_cases)) AS death_percentage
FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
GROUP BY date
ORDER BY date;

   SELECT 
    date,
    SUM(new_cases) AS total_cases,
    SUM(cast(new_deaths as int)) AS total_deaths,
    (SUM(cast(new_deaths as int))  * 100.0 / SUM(new_cases)) AS death_percentage
FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
where continent is not null
GROUP BY date
ORDER BY date;

SELECT 
    date,
    SUM(new_cases) AS total_cases,
    SUM(CAST(new_deaths AS INT)) AS total_deaths,
    (SUM(CAST(new_deaths AS INT)) * 100.0 / NULLIF(SUM(new_cases), 0)) AS death_percentage
FROM [COVID Portfolio Project].[dbo].[CovidDeaths$]
GROUP BY date
ORDER BY date;

Select dea.location, dea.continent, dea.population, dea.date, vac.new_vaccinations
from [COVID Portfolio Project]..CovidDeaths$ AS dea
JOIN [COVID Portfolio Project]..CovidVaccination$ AS vac
ON dea.location = vac.location AND dea.date= vac.date
where dea.continent is not null
order by 1,4;

---using windows function
Select dea.location, dea.continent, dea.population, dea.date, vac.new_vaccinations,
SUM(CAST(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date)
as rollingpopulationvaccinated
from [COVID Portfolio Project]..CovidDeaths$ AS dea
JOIN [COVID Portfolio Project]..CovidVaccination$ AS vac
ON dea.location = vac.location AND dea.date= vac.date
where dea.continent is not null
;

---using CTE 
With popvsvac (location, continent, population,date, new_vaccination, rollingpopulationvaccinated)
as (
Select dea.location, dea.continent, dea.population, dea.date, vac.new_vaccinations,
SUM(CAST(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date)
as rollingpopulationvaccinated
from [COVID Portfolio Project]..CovidDeaths$ AS dea
JOIN [COVID Portfolio Project]..CovidVaccination$ AS vac
ON dea.location = vac.location AND dea.date= vac.date
where dea.continent is not null
)
select * , (rollingpopulationvaccinated/population)*100
from popvsvac


----temp table

DROP TABLE IF EXISTS #percentpeoplevaccinated
CREATE TABLE #percentpeoplevaccinated
(
location NVARCHAR(255),
continent NVARCHAR(255),
population numeric,
date datetime,
new_vaccination numeric,
rollingpopulationvaccinated numeric
)

INSERT INTO #percentpeoplevaccinated
Select dea.location, dea.continent, dea.population, dea.date, vac.new_vaccinations,
SUM(CAST(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date)
as rollingpopulationvaccinated
from [COVID Portfolio Project]..CovidDeaths$ AS dea
JOIN [COVID Portfolio Project]..CovidVaccination$ AS vac
ON dea.location = vac.location AND dea.date= vac.date
where dea.continent is not null

select * , (rollingpopulationvaccinated/population)*100
from #percentpeoplevaccinated

---creating view for store data for later visualization--
create view peoplevaccinatedpercentage as
Select dea.location, dea.continent, dea.population, dea.date, vac.new_vaccinations,
SUM(CAST(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location, dea.date)
as rollingpopulationvaccinated
from [COVID Portfolio Project]..CovidDeaths$ AS dea
JOIN [COVID Portfolio Project]..CovidVaccination$ AS vac
ON dea.location = vac.location AND dea.date= vac.date
where dea.continent is not null

select * from peoplevaccinatedpercentage;

