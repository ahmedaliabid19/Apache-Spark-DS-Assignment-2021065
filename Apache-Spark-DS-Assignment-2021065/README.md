
🎥 Netflix TV Shows & Movies Analysis with Spark & Scala
This project utilizes Apache Spark and Scala to conduct exploratory data analysis (EDA) on a dataset containing information about Netflix TV Shows and Movies.

---

## 📘 Table of Contents
- [Introduction](#introduction)
- [Core Spark Concepts](#core-spark-concepts)
- [Setup Instructions](#setup-instructions)
- [How to Run the Project](#how-to-run-the-project)
- [Results and Outputs](#results-and-outputs)

---

## 🚀 Introduction
Apache Spark is an open-source distributed computing system that provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. It’s widely used for big data processing and supports multiple programming languages like Scala, Python, and Java.

---

## 🔑 Core Spark Concepts

### RDD - Core of Spark
Resilient Distributed Dataset (RDD) is Spark's core abstraction for a fault-tolerant and distributed collection of elements that can be operated on in parallel.

### Execution in Spark
Spark employs a **Directed Acyclic Graph (DAG)** to plan jobs. It breaks operations into tasks and stages to optimize execution across distributed systems.

### Memory Management
Spark uses a combination of in-memory processing and disk storage to optimize the use of resources and speed up data processing.

### Fault Tolerance
Spark ensures fault tolerance by allowing RDDs to be rebuilt from lineage in case of node failures.

---

## Setup Instructions

### Prerequisites
1. Install **Docker** on your machine.
2. Clone this repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

---

## How to Run the Project

### 1. Build the Docker Image
Build the Docker image for the Spark Scala application:
```bash
docker build -t spark-scala-app .
```

### 2. Run the Docker Container
Start a Docker container using the built image:
```bash
docker run --rm -v $(pwd)/output:/app/output spark-scala-app
```



### 4. View Output
The results will be saved in the `output/` directory within the project directory

---

## Results and Outputs

1. **Schema Information**: Available in `output/schema.txt`.
2. **Sample Data**: First 10 records saved in `output/sample_data/`.
3. **Movies vs. TV Shows Count**: Results saved in `output/type_count/`.
4. **Top 10 Countries with Most Titles**: Results saved in `output/top_countries/`.
5. **Popular Genres**: Results saved in `output/popular_genres/`.
6. **Titles Released Per Year**: Results saved in `output/titles_per_year/`.
7. **Null Counts**: Null values for each column saved in `output/null_counts.txt`.

---

### Notes
- Ensure that the path to the `NetflixEDA.jar` file is correctly specified in the Spark job submission command.
- To map the output directory to your local machine, consider using Docker volumes or bindings.
