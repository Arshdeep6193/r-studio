install.packages("sparklyr")
#install.packages("SparkR")
#install.packages("sparkbq")

library(sparklyr)
#library(sparkbq)
library(dplyr)
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
sc <- spark_connect(master="yarn-client", config=config)

gscycle <- 
  spark_read_csv(
    sc=sc
    ,path ="gs://dataflow_temp_store/bigquery_table/GSCycle.csv"
    ,infer_schema = TRUE
    ,header = TRUE
    ,memory = FALSE)  %>%
  filter(gameRoundId=="1037416964")%>%
  collect()

#or you can use sql query in it 
gscycle <- 
  spark_read_csv(
    sc=sc
    ,path ="gs://dataflow_temp_store/bigquery_table/GSCycle.csv"
    ,infer_schema = TRUE
    ,header = TRUE
    ,memory = FALSE
    ,)  %>%
  
  collect()

gsccyclelocal <- copy_to(sc,gscycle,overwrite = TRUE)

gsccyclelocal %>%  
  arrange(id) %>%
  summarize(
    first_val = first_value(bookingId),
    last_val = last_value(bookingId)
  ) 


FreeSpinCycle3 <- 
  spark_read_csv(
    sc=sc
    ,path ="gs://dataflow_temp_store/bigquery_table/freespincycle/freespincycle*.csv"
    ,memory = FALSE)  %>%
  filter(gameRoundId %in% c(1046205126,821549912,512875249)) %>%
  collect()

freespinlocal1 <- copy_to(sc, FreeSpinCycle3,overwrite = TRUE)


book1 <- 
  spark_read_csv(
    sc=sc
    ,path ="gs://dataflow_temp_store/bigquery_table/booking/booking*.csv"
    ,infer_schema = TRUE
    ,header = TRUE
    ,memory = FALSE)  %>%
  filter(sessionId=="3233937")%>%
  collect()


book2 <- 
  spark_read_parquet(
    sc=sc
    ,path ="gs://dataflow_temp_store/bigquery_table/booking_pq/000*"
    ,repartition = 0
    ,memory = FALSE)  %>%
  filter(sessionId=="3233937")%>%
  collect()

freespinlocal1 %>%
  arrange(id) %>% show()

group_by(gameRoundId) %>%
  summarize(
    first_val = first_value(counter),
    last_val = last_value(counter)
  )

freespinlocal1 %>%
  arrange(id) %>%
  mutate(count1 = lag(counter, 2))

freespinlocal1 %>%
  arrange(id) %>%
  mutate(rank = rank(desc(noOfFreeGames)))

spark_disconnect(sc)