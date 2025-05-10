# 📊 City of Fort Worth Development Permits Data Analysis

_An analysis of public permit data from the Ciy of Fort Worth using PostgreSQL and Power BI. 
A 3 part series about this project, including the results of my data analysis is available on my blog at https://medium.com/@sergioramos3.sr/list/exploring-cofw-a8720258688b_

## 🔍 Project Overview

This repository holds a data analysis project based on a public dataset. The data is analyzed using PostgreSQL and visualized in Power BI. Project includes a link to data source and code to replicate processing steps, as well as dashboard files.

## 🧱 Folder Structure

```graphql
project-root/
├── Code/                  # SQL queries used for data cleaning and analysis
├── Dashboard_Files/       # Power BI dashboard components
│   ├── PBIP/              # Power BI Project files
│   ├── PBIT/              # Power BI Template
│   └── PBIX/              # Power BI Desktop file
└── Research_Notes/        # Assumptions, findings, research sources, and exploratory notes
```

## 🚀 Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/0sergio-hash/City-of-Fort-Worth-Development-Permits-Data-Analysis.git
   ```

2. **Load the dataset**
   - Download data from COFW site.
   - SQL import and cleaning script is available in `/Code`to load into your PostgreSQL instance.

3. **Run the analysis**
   - SQL queries are modular and labeled inside `/Code`.

4. **Open the dashboard**
   - Use the `.pbix`, `.pbip`, or `.pbit` files inside `/Dashboard_Files` to open the Power BI report.
   - Power BI will prompt for data sources if not already loaded—point it to your PostgreSQL instance.

## 🧠 Project Goals

- [ ] Understand Economic Development in Fort Worth 
- [ ] Identify key trends in Development Permit data
- [ ] Build a clean and reusable Power BI dashboard

## 📚 Data Source

> https://data.fortworthtexas.gov/Development-Infrastructure/Development-Permits/quz7-xnsy/about_data

## 📌 Notes
- See `/Research_Notes` as well as notes within SQL scripts in `/Code` for additional context 

## ✅ Requirements

- Power BI Desktop (or Power BI service)
- PostgreSQL (or an understanding of the SQL used)
- Git (optional but helpful for version control)

