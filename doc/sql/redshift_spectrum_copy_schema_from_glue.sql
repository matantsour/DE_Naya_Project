-- in redshift query editor, link to Glue tables. need to have a cluster first.
CREATE EXTERNAL SCHEMA publishing_schema
FROM DATA CATALOG
DATABASE 'adzuna-jobs'
IAM_ROLE 'arn:aws:iam::275198336494:role/redshift-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;