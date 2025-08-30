# DE_test

1. add .env file with:
    - OPENEXCHANGE_APP_ID
    - AIRFLOW__API_AUTH__JWT_SECRET
    - AIRFLOW__WEBSERVER__SECRET_KEY
    - POSTGRES_1_USER
    - POSTGRES_1_PASSWORD
    - POSTGRES_2_USER
    - POSTGRES_2_PASSWORD
2. run docker copmpose file
    https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env

    docker compose run airflow-cli airflow config list

    docker compose up airflow-init

    docker compose up -d

3. execute DB schema files
    1.[00_schema_stg.sql](DB/postgres-1/00_schema_stg.sql)
    2.[00_schema_dwh.sql](DB/postgres-2/00_schema_dwh.sql)
4. execute stored procedures files
    [sp_load_dim_curr_pair.sql](DB/postgres-2/proc/sp_load_dim_curr_pair.sql)
    [sp_load_orders_eur.sql](DB/postgres-2/proc/sp_load_orders_eur.sql)
5. test DAGs
    http://localhost:8080/

