

<h1 align="center">THE RISE OF THE SINGLE SOURCE: A dbt Analytics Saga</h1>

![Logo](https://github.com/JBaptisteAll/dbt_tuto/blob/main/assets/logo_the_rise_of_the_single_source.png)
<p align="center">
  <img src="https://github.com/JBaptisteAll/dbt_tuto/blob/main/assets/logo_the_rise_of_the_single_source.png" alt="Texte alternatif" width="500">
</p>  

## 1. Executive Summary
This project demonstrates a production-grade ELT pipeline built with **dbt (data build tool)**.   
It transforms raw, fragmented aviation data into a strategic decision-making asset. The architecture focuses on multi-layer modeling, rigorous data validation, and automated documentation specifically designed to empower executive decision-making through high-level KPIs and automated classification.

---

## 2. Technical Stack & Infrastructure
* **Data Transformation**: dbt Cloud (Core logic in SQL/Jinja)
* **Data Warehouse**: PostgreSQL (Hosted on Neon.tech)
* **Orchestration**: dbt Cloud Jobs
* **Documentation**: dbt Docs (Automatic catalog & manifest generation)
* **Version Control**: GitHub (Version-controlled DAG and tests)
* **Modeling Philosophy**: Medallion-inspired (Staging -> Intermediate -> Marts)

---

## 3. Data Architecture & Lineage
The project follows a modular **Directed Acyclic Graph (DAG)** structure to ensure DRY (*Don't Repeat Yourself*) code and clear data provenance.  

### Data Lineage & DAG
The Directed Acyclic Graph (DAG) below illustrates the end-to-end flow, from raw ingestion to the final executive dashboard.

![Lineage](https://github.com/JBaptisteAll/dbt_tuto/blob/main/assets/dbt-dag.png)

### Modeling Layers

The project implements a **Medallion Architecture** to ensure data reliability and modularity.

### Data Transformation Layers
1.  **Staging (`models/staging/`)**: Atomic cleaning, schema standardization, and type casting from raw PostgreSQL sources.  
2.  **Intermediate (`models/intermediate/`)**: Business logic encapsulation.
    * **Geospatial**: Custom Haversine formula implementation using **dbt Seeds** for GPS coordinates.
    * **Metric Calculation**: Computes RPM (Revenue Passenger Miles) and ASM (Available Seat Miles) at the flight level.
3.  **Marts (`models/marts/`)**  
    The final consumer-facing datasets, organized by business domain:
    * **Operations**: Deep-dives into route efficiency, punctuality, and aircraft utilization.
    * **Marketing**: Focuses on passenger segmentation and loyalty. This layer analyzes booking behavior and traveler demographics to drive targeted campaigns.
    * **Executive**: A high-level strategy layer featuring the **CEO Dashboard** model.

---

## 4. Analytical Engineering Deep-Dive

### KPI Engineering: Load Factor
The core metric used to assess route viability is the **Load Factor**, calculated using Jinja-templated SQL to handle edge cases.


### The "Star Route" Classification (The CEO Mart)
The `mart_ceo_route_performance` model implements a relative performance algorithm. It compares a specific route's Load Factor against against airline benchmarks to classify performance dynamically:

- **💎 Star Route**: Performance > 10% above company average.
- **⚠️ Underperforming**: Performance < 0% below company average.
- **✅ Stable**: Performance aligned with airline standards.

---

## 5. Data Quality & Governance
Data integrity is maintained through **63 automated tests**, ensuring the "Single Source of Truth".

* **Generic Tests**: `unique`, `not_null`, and `relationships` (referential integrity).
* **Domain Validation**: `accepted_values` for status labels and logical consistency checks (e.g., flight duration validation).
* **Custom Singular Tests**: Validates geospatial coordinates and prevents logical errors in flight paths.
* **Documentation**: 100% of models are documented via YAML schemas, including column descriptions and metadata.

---

## 6. Project Structure
```text
├── models/
│   ├── staging/        # Source alignment & type casting
│   ├── intermediate/   # Heavy transformations & enrichment
│   └── marts/          # Consumer-facing datasets
│       ├── operations/ # Operational metrics
│       └── executive/  # Strategic insights (CEO layer)
├── seeds/              # Static reference data (Airport GPS)
├── tests/              # Custom SQL data validation
├── dbt_project.yml     # Global configurations
└── README.md           # Technical documentation
```

---

## 7. Deployment & Usage
**Local Development**  
To replicate this environment locally:
```
# Install dbt-postgres
pip install dbt-postgres

# Validate connection to Database
dbt debug

# Execute the full pipeline (Run + Test)
dbt build

# Generate and launch the interactive documentation site
dbt docs generate
dbt docs serve
```

--- 

## 8. Portfolio Integration
For a non-technical breakdown of the business outcomes and strategic insights derived from this data, please visit my [Portfolio Website](https://jbaptisteall.github.io/JeanBaptisteAllombert/index.html?utm_source=github&utm_medium=referral&utm_campaign=github).

Author: Jean-Baptiste Allombert  
Role: Data Engineer / Analytics Engineer

---

## 🔮 Final Notes: The Saga Concludes
This project is the final chapter of the three-part airline data saga. Following **Revenge of the Seat** (Database Design) and **Return of the Jet High** (SQL Analytics). 

By implementing The Rise of the Single Source, the transition from raw operational data to sophisticated Analytics Engineering is complete. The galaxy of data is now ordered, tested, and documented. 🚀