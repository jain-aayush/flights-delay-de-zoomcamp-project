# DBT - Transformation

Steps : 

1. Create a DBT project and Add your GCP service-account-keys for authentication
2. Create and initialize a reposiroty
3. Fill in the macros and models. 
4. Commit the final changes
5. Create a Production Job by adding a dataset name and use the following commands
```
dbt build --select +flights_fact_dim --var 'is_test_run: false'
```

# Partitioning and Clustering

```
{{ 
    config(
        materialized='table',
        partition_by={
            'field' : 'Date',
            'data_type' : 'date',
            'granularity' : 'month'
        }, 
        cluster_by=['origin_airport_city', 'Origin']
    )
}}
```

Patitioned on Date column on monthly granularity as all temporal analysis were done based on monthly granularity. 
Clustering is done on origin_airport_city and Origin as spatial analysis were done based on these columns.