# Global NO₂ Monitoring & Anomaly Detection Platform

A scalable, end-to-end platform for detecting, analyzing, and visualizing global nitrogen dioxide (NO₂) anomalies using global daily satellite (TROPOMI)-based measurements on 0.1° × 0.1° grid of entire surface of habitable Earth.

---

## 🚀 Overview

This project ingests daily satellite NO₂ data, processes it into geospatial and country-level formats, computes rolling averages and anomalies, and exposes the data via an API and interactive frontend.

It combines modern **data engineering**, **cloud infrastructure**, and **data science** techniques to build a real-time environmental intelligence system.

---

## 🧠 Key Features

- **Daily Global NO₂ Ingestion** from ESA/TEMIS datasets
- **ETL Pipelines** for transforming raw data into:
  - 0.1° × 0.1° global grid Parquet files
  - Country-level daily CSVs
- **Rolling Average Computation** using 7-day windows
- **Anomaly Detection** based on deviations from rolling averages
- **Geospatial Panel Construction** for long-term trend and seasonality modeling
- **Cloud-Native Infrastructure** (built from scratch):
  - Batch jobs using **Google Cloud Batch** and **Docker**
  - Workflow orchestration via **GitHub Actions** and **Cloud Scheduler**
  - Data stored and served via **Google Cloud Storage** and **BigQuery**
- **API Access** via a productionized **FastAPI** backend deployed on **Cloud Run**
- **React Frontend** for user interaction, plotting, and data exploration
- **CI/CD Integration** for automated updates, testing, and deployment

---

## 🧰 Tech Stack

| Area              | Tools / Technologies                         |
|-------------------|----------------------------------------------|
| **Cloud Compute** | Google Cloud Run, Google Cloud Batch         |
| **Orchestration** | GitHub Actions, Cloud Scheduler              |
| **Data Storage**  | Google Cloud Storage, BigQuery               |
| **Containers**    | Docker, Artifact Registry                    |
| **Backend**       | Python, FastAPI, pandas, pyarrow             |
| **Frontend**      | React, Plotly, Tailwind                      |
| **Data Science**  | Rolling averages, anomaly detection, geospatial panel construction |

---

## 📈 Project Architecture

         ┌──────────────────────────┐
         │   Daily NO₂ Satellite   │
         │     Data Download       │
         └──────────┬──────────────┘
                    │
         ┌──────────▼──────────┐
         │  ETL & Format to    │
         │  Parquet / CSV      │
         └──────────┬──────────┘
                    │
         ┌──────────▼────────────┐
         │  Rolling Average Job  │
         │   (7-day smoothing)   │
         └──────────┬────────────┘
                    │
         ┌──────────▼────────────┐
         │ Anomaly Detection API │
         └──────────┬────────────┘
                    │
         ┌──────────▼────────────┐
         │   React Frontend UI   │
         └───────────────────────┘

## 📂 Repository Structure

├── sub/                  # Submodules for ingestion, rolling avg, etc.
│   ├── ingest/           # Download and format raw NO₂ data
│   ├── ra/               # Rolling average computation (Cloud Batch)
│   └── anomalies/        # Future anomaly detection models
├── api/                  # FastAPI backend
├── site-frontend/        # React frontend app
├── .github/workflows/    # GitHub Actions for CI/CD
├── Dockerfile            # Docker image for API or jobs
└── README.md

# ✅ What This Project Demonstrates
Cloud-native data workflows from ingestion to visualization

End-to-end ownership of a real-time production system

Scalable geospatial data processing using containers and batch jobs

Automation-first mindset with CI/CD, scheduling, and version control

GCP, Python, SQL, and JavaScript (React) — full-stack technical range

Ability to translate raw data into insight via anomaly detection and visualization

# 📎 Use Cases
Real-time environmental monitoring

Economic activity estimation from emissions

AI-based alert systems for pollution spikes

Geospatial data engineering at global scale

# 🔐 Status: August 7, 2025
Live daily updates running in production

Backfills completed for May 1, 2018 – present

Frontend and API under construction 

# 🧑‍💼 Contact
Stephen Morris
PhD Economist | Data Scientist | Cloud Engineer
[LinkedIn](https://www.linkedin.com/in/stephen-morris-b37931373/)
