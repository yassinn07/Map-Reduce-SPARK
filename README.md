# 📊 Analyzing Wikimedia Page Views with Apache Spark

This project explores and analyzes hourly page view statistics from Wikimedia projects using Apache Spark. The dataset contains records of Wikimedia page hits between **0:00 and 1:00 AM on January 1st, 2016**, and is used to demonstrate distributed data processing using both the **MapReduce paradigm** and **Spark-native transformations/loops**.

---

## 📁 Dataset Description

The dataset can be downloaded from the Wikimedia page view statistics for Jan 1, 2016 (0–1 AM). Each line in the dataset contains whitespace-separated fields with the following schema:

| Field | Description |
|-------|-------------|
| Project Code | Project identifier (e.g., `en`, `de`, `es`, etc.) |
| Page Title   | Title of the page (e.g., `Main_Page`) |
| Page Hits    | Number of hits during that hour |
| Page Size    | Size of the page in bytes |

---

## 🛠️ Technologies Used

- Apache Spark
- PySpark or Scala (language-agnostic)
- HDFS or local file system
- UNIX timestamping for performance tracking

---

## ⚙️ Processing Approaches

Two implementations are developed:

1. **MapReduce-style transformations**
2. **Spark-native operations using DataFrame API or RDD loops**

Execution time for both methods is compared for performance benchmarking.

---

## ✅ Tasks Implemented

### 1. 🔎 Compute Page Size Statistics

- **Objective:** Calculate the **minimum**, **maximum**, and **average** of page sizes.
- **Methods:**
  - `RDD.reduce()` or `DataFrame.agg()` for performance comparison.

---

### 2. 🧠 Analyze Page Titles Starting with "The"

- **Objective:**
  - Count page titles that begin with the article `"The"` (case-sensitive or normalized).
  - Count how many of those titles **do not belong to the English project** (`project_code != "en"`).

---

### 3. 📚 Count Unique Terms in Page Titles

- **Objective:** 
  - Determine the number of **unique terms** in all page titles.
  - Terms are split on `"_"` instead of spaces.
  - Optional normalization:
    - Convert to lowercase
    - Remove non-alphanumeric characters

---

### 4. 🔁 Extract and Count Title Repetitions

- **Objective:** 
  - Extract each page title and count how many times it appears in the dataset.

---

### 5. 🔗 Merge Pages with Same Title

- **Objective:** 
  - Identify all pages that share the same title but may come from different projects.
  - Group them and output paired/aggregated data for each unique title.

---

## 📈 Performance Comparison

Each query is executed twice:

- **Using MapReduce (RDD transformation chains)**
- **Using Spark-native DataFrame operations or loops**

Execution time is recorded for both approaches to analyze performance trade-offs.

---

## 📝 Output

The project generates:

- Console outputs and logs for query results
- A comparative performance report (execution time per query/method)
- Optional: Saved result files (CSV, JSON, or Parquet) for each analysis

---

## 🔍 Insights and Learnings

This project demonstrates how to:

- Handle real-world, semi-structured text data
- Parse and normalize textual information
- Identify and avoid redundant computations
- Optimize distributed processing with Apache Spark
- Use both low-level RDD APIs and high-level Spark SQL/DataFrame APIs

---
