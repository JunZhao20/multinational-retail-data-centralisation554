# Multinational retail data centralization

A multinational company that sells various goods across the globe, currently their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, the organisation would like to make its sales data accessible from one centralised location.

## Aim

- To produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

- Query the database to get up-to-date metrics for the business

## Features

- Centralised location is stored and managed on postgreSQL
- Uses pandas module to clean and extract data from the various sources
- Cleaned data is stored using the feather format because:
  - Fast read and write operations.
  - Language-agnostic, can be used with multiple programming languages.
  - Columnar format, optimized for DataFrames.

## Installation

1.  Clone the repository to your preferred directory
    'https://github.com/JunZhao20/multinational-retail-data-centralisation554.git'
