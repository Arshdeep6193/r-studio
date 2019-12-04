install.packages("devtools")
install.packages("sparklyr")
install.packages("sparkbq")
install.packages("dplyr")

devtools::install_github("miraisolutions/sparkbq", ref = "develop")
devtools::install_github("rstudio/sparklyr")
packageVersion("sparklyr")

library(sparklyr)
library(sparkbq)
library(dplyr)

config <- spark_config()
spark_install()
sc <- spark_connect(master = "local[*]", config = config)

# Set Google BigQuery default settings
bigquery_defaults(
  billingProjectId = "optimal-sun-254511",
  gcsBucket = "gs://dataflow-staging-europe-west1-1011476581437/temp",
  datasetLocation = "EU",
  type = "direct"
)
default_bigquery_type()
default_billing_project_id()
default_dataset_location()
default_gcs_bucket()
default_service_account_key_file()


# Reading the public shakespeare data table
# https://cloud.google.com/bigquery/public-data/
# https://cloud.google.com/bigquery/sample-tables
hamlet <- 
  spark_read_bigquery(
    sc,
    name = "shakespeare",
    projectId = "bigquery-public-data",
    datasetId = "samples",
    tableId = "shakespeare") %>%
  filter(corpus == "hamlet") %>% # NOTE: predicate pushdown to BigQuery!
  collect()

Casino <- 
  spark_read_bigquery(
    sc,
    name = "Casino",
    projectId = "optimal-sun-254511",
    datasetId = "BWAnalytics",
    tableId = "Casino") %>%
  filter(name == "whow-production") # NOTE: predicate pushdown to BigQuery!

# Retrieve results into a local tibble
Casino %>% collect()


PlayTactic <- 
  spark_read_bigquery(
    sc,
    name = "PlayTactic",
    projectId = "optimal-sun-254511",
    datasetId = "test",
    tableId = "playtactics") %>%
  filter(id == "256") %>% # NOTE: predicate pushdown to BigQuery!
  collect()

head(PlayTactic,4)
print(PlayTactic)

PlayTactic_tbl <- dplyr::copy_to(sc, PlayTactic)
print(PlayTactic_tbl)

help(spark_write_bigquery)

spark_mtcars <- dplyr::copy_to(sc, mtcars, "spark_mtcars", overwrite = TRUE)
spark_write_bigquery(
  data = spark_mtcars,
  datasetId = "test",
  tableId = "mtcars",
  mode = "overwrite")


spark_write_bigquery(
  data = PlayTactic_tbl,
  datasetId = "test",
  tableId = "playtacticss",
  mode = "overwrite")


View(PlayTactic)



