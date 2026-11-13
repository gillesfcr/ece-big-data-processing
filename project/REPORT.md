# ECE Big Data Processing - IMDb project report

Group gr-04: Arthur Perreaut, Gilles Foucart

## Table of contents

1. [Data preparation](#1-data-preparation)
2. [Data exploration](#2-data-exploration)
3. [Data enrichment](#3-data-enrichment)
4. [Streaming data processing](#4-streaming-data-processing)
5. [Open question](#5-open-question)

---

## 1. Data preparation

### 1.1 Percentage of null, missing or empty values per column

<!-- RESULT: table (table, column, % \N, % empty, % total missing) -->

### 1.2 Top five columns with the highest percentage of null values

<!-- RESULT: table -->

### 1.3 Other data quality issues

<!-- RESULT: one table per issue (description, rule, number of rows affected) -->

### 1.4 Handling strategy

<!-- RESULT: what was done for each issue, rows before/after -->

### Methodological explanation

#### Methodology and why it was chosen

<!-- TO WRITE -->

#### Alternatives considered and trade-offs

<!-- TO WRITE -->

---

## 2. Data exploration

### 2.1 Feature-length movies with an unusual runtime for their genre

<!-- RESULT -->

### 2.2 Top three most popular genres per year (1980-2025)

<!-- RESULT -->

### 2.3 Top three genres compared to the other genres of the year

<!-- RESULT -->

### 2.4 Directors with exactly one movie rated >= 8.0 and other movies averaging <= 6.0

<!-- RESULT -->

### 2.5 TV series whose worst episode is in the last season and best in the first two

<!-- RESULT -->

### 2.6 Actors/actresses successful in multiple languages

<!-- RESULT: two languages or more, then three languages or more -->

### 2.7 Actors/actresses whose popularity peaked early in their career

<!-- RESULT -->

### 2.8 Multi-talented film professionals

<!-- RESULT -->

### Methodological explanation

#### Methodology and why it was chosen

<!-- TO WRITE: definitions used (feature-length, popular, successful, ...) -->

#### Alternatives considered and trade-offs

<!-- TO WRITE -->

---

## 3. Data enrichment

### 3.1 Scope and source

<!-- RESULT: number of films, principals, source used -->

### 3.2 Enriched dataset

<!-- RESULT: sample of the output (with lineage column) -->

### 3.3 Coverage

<!-- RESULT: % of films with a synopsis, % of principals with a biography,
missing-information notes -->

### Methodological explanation

#### Methodology and why it was chosen

<!-- TO WRITE: rate limits, lineage, Spark processing, scalability -->

#### Alternatives considered and trade-offs

<!-- TO WRITE -->

---

## 4. Streaming data processing

### 4.1 Pipeline overview

<!-- RESULT: Wikipedia -> Kafka -> Structured Streaming -> output -->

### 4.2 Filtering strategy

<!-- RESULT -->

### 4.3 Results and streaming metrics

<!-- RESULT: test window, events received, events matched, sample of matched
events, batch durations / input rate -->

### 4.4 Late-arriving data

<!-- TO WRITE -->

### Methodological explanation

#### Methodology and why it was chosen

<!-- TO WRITE -->

#### Alternatives considered and trade-offs

<!-- TO WRITE -->

---

## 5. Open question

### 5.1 Question, motivation and additional data

<!-- TO WRITE -->

### 5.2 Results

<!-- RESULT -->

### 5.3 Key takeaways

<!-- TO WRITE -->

### Methodological explanation

#### Methodology and why it was chosen

<!-- TO WRITE -->

#### Alternatives considered and trade-offs

<!-- TO WRITE -->
