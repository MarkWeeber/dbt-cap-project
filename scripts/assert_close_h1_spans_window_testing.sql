SELECT
    min("timestamp") AS first_hour,
    max("timestamp") AS last_hour,
    count(*) AS n,
    count(DISTINCT "timestamp") AS hours
FROM "dev"."DEV"."stg_close_h1";
