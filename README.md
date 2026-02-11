# PySpark Crime Analysis -- Los Angeles Crime Data (2020-Present)

Large-scale analysis of the **LA Crime dataset** (~1M records) using a two-phase pipeline: data cleaning and transformation with Pandas, followed by distributed querying and aggregation with **Apache PySpark**.

The project answers key public safety questions: which age groups are most affected by crime, what weapons are most frequently used, and which neighborhoods have the highest crime rates.

![Python](https://img.shields.io/badge/Python-3.x-3776AB)
![PySpark](https://img.shields.io/badge/PySpark-Distributed-E25A1C)
![Pandas](https://img.shields.io/badge/Pandas-Preprocessing-150458)
![Records](https://img.shields.io/badge/Records-1M+-blue)

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Dataset](#dataset)
- [Pipeline Architecture](#pipeline-architecture)
- [Phase 1 -- Data Cleaning](#phase-1--data-cleaning)
- [Phase 2 -- PySpark Analysis](#phase-2--pyspark-analysis)
- [Key Findings](#key-findings)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)

---

## Problem Statement

The City of Los Angeles publishes detailed crime incident data. With over **1 million records** spanning 2020 to present, the dataset is too large for simple spreadsheet analysis. This project builds a scalable pipeline to:

1. **Clean and standardize** raw LAPD data (fix types, handle missing values, remove anomalies)
2. **Analyze crime patterns** at scale using PySpark's distributed computing
3. **Answer business questions** about victim demographics, weapon usage, geographic hotspots, and temporal trends

---

## Dataset

| Property | Value |
|----------|-------|
| Source | [LA Open Data -- Crime Data from 2020 to Present](https://data.lacity.org/) |
| Records | ~1,004,991 |
| Original columns | 28 |
| Cleaned columns | 18 (after dimensionality reduction) |
| Key variables | Crime type, victim age/sex/descent, weapon, location, date/time, status |

---

## Pipeline Architecture

```
Raw CSV (28 columns, ~1M rows)
  |
  v
Phase 1: Pandas Cleaning (crimes_transfo.py / cleaned_crimes.ipynb)
  |-- Dimensionality reduction: 28 -> 16 columns
  |-- Date/time parsing: combined timestamp + derived hour/day_of_week
  |-- Missing value imputation:
  |     |-- vict_age: median imputation (MCAR) + age group binning
  |     |-- vict_sex/descent: "Unknown" (MNAR -- non-disclosure)
  |     |-- weapon_desc: "No weapon" (MNAR -- no weapon used)
  |     |-- premis_desc: "NO DESC" (MCAR -- data entry error)
  |     |-- status: mode imputation (1 missing record)
  |-- Snake_case column naming convention
  |-- Zero NaN remaining after cleaning
  |
  v
Cleaned CSV (18 columns, 1,004,991 rows, 0 NaN)
  |
  v
Phase 2: PySpark Analysis (crimeanalysis.py)
  |-- Load into Spark DataFrame
  |-- Filter, group, aggregate at scale
  |-- Answer key crime analysis questions
  |
  v
Results: crime patterns by age, weapon, area, time
```

---

## Phase 1 -- Data Cleaning

### Dimensionality Reduction

Removed redundant columns (`Crm Cd 2/3/4`, `Cross Street`, `LOCATION`, `DR_NO`) to reduce shuffle cost in PySpark. Kept only the 16 columns needed for analysis using the **QQOQCP framework** (What, When, Where, Who/Victim).

### Missing Value Strategy

Each missing value was treated according to its **mechanism**:

| Variable | Mechanism | Strategy | Justification |
|----------|-----------|----------|---------------|
| `vict_age` | MCAR | Median imputation (30.0) | Random missingness, median preserves distribution |
| `vict_sex` | MNAR | "Unknown" category | Victim chose not to disclose |
| `vict_descent` | MNAR | "Unknown" category | Sensitive info, not random |
| `weapon_desc` | MNAR | "No weapon" | Missing = no weapon was used |
| `premis_desc` | MCAR | "NO DESC" | <1% missing, simple data entry error |
| `status` | MCAR | Mode imputation | Single missing record |

### Feature Engineering

- **`timestamp_occ`**: Combined `date_occ` + `time_occ` into a single timestamp
- **`heure`**: Extracted hour (0-23) for temporal analysis
- **`jour_semaine`**: Extracted day name (Monday-Sunday)
- **`vict_age_group`**: Binned ages into Minor / Young Adult / Adult / Senior

---

## Phase 2 -- PySpark Analysis

The cleaned dataset is loaded into a Spark DataFrame for distributed querying.

### Questions Answered

**1. Which age groups are most affected by crime?**
- Filter records with valid victim age (`> 0`)
- Group by `vict_age_group`, count, and sort descending

**2. What weapons are most frequently used?**
- Filter out `"No weapon"` and NULL entries
- Group by `weapon_desc`, count top 10

**3. Which neighborhoods have the highest crime rates?**
- Group by `area_name`, count incidents
- Sort descending to identify hotspots

---

## Key Findings

- **Young Adults (18-35)** are the most victimized age group across all crime categories
- **Knives** and **firearms** are the most common weapons when a weapon is involved (majority of crimes involve no weapon)
- Certain LAPD areas consistently report higher crime volumes, useful for resource allocation decisions
- **Weekend evenings** show distinct crime spikes compared to weekday patterns

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Pandas | Data cleaning, transformation, feature engineering |
| NumPy | Numerical operations, NaN handling |
| PySpark | Distributed analysis and aggregation at scale |
| Google Colab | Notebook execution environment |

---

## Getting Started

### Prerequisites

- Python 3.x
- PySpark (`pip install pyspark`)
- Pandas, NumPy

### Run the Cleaning Pipeline

```bash
# Option 1: Run the notebook
jupyter notebook cleaned_crimes.ipynb

# Option 2: Run the Python script
python crimes_transfo.py
```

### Run the PySpark Analysis

```bash
# Requires the cleaned CSV output from Phase 1
python crimeanalysis.py
```

### Dataset

Download `Crime_Data_from_2020_to_Present.csv` from [LA Open Data](https://data.lacity.org/) and place it in the project root.

---

## Project Structure

```
pyspark_project/
├── cleaned_crimes.ipynb     # Phase 1: Interactive cleaning notebook with outputs
├── crimes_transfo.py        # Phase 1: Standalone cleaning script
├── crimeanalysis.py         # Phase 2: PySpark distributed analysis
└── README.md
```

| File | Description |
|------|-------------|
| `cleaned_crimes.ipynb` | Full cleaning pipeline with markdown explanations and cell outputs |
| `crimes_transfo.py` | Same pipeline as a standalone `.py` script for automation |
| `crimeanalysis.py` | PySpark queries: crime by age group, weapon type, and area |

---

## Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| 28 columns with many redundant fields | Applied QQOQCP framework to keep only 16 essential columns |
| Mixed date formats (`MM/DD/YYYY HH:MM:SS AM/PM`) | Parsed with explicit format string + combined date and time into single timestamp |
| MNAR missing values (weapon, sex, descent) | Domain-driven imputation: "No weapon", "Unknown" -- not random deletion |
| Negative and impossible ages (< 0, > 100) | Replaced with NaN then imputed with median (30.0) |
| 1M+ rows too large for single-machine aggregation | PySpark for distributed groupBy/count/orderBy operations |

---

**Built by [Wissal Akkaoui](https://github.com/wiswis01)**
