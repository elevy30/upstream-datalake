# upstream-datalake

Posiable Improvements:
## module
  - split all layer to diferent package
  - use dbt in the gold layer
  - have common module that will handel all the common utils
## API to stream:
  - write data to kafka from the API
  - kafka will enable partitions & scale out
## Reading:
  - read data with spark streaming from kafka 
  - read as a stream, the stream will handel the offset and insure "at least once"
  - read as a stream enable commit & version control 
  - read in partition from kafa
## Writing:
  - write mode: in the task I used "overwrite" for simplicity but should be "append" mode
  - use delta/iceberg format - will enable versions, updates, transaction 
  - use partitions in gold by (hash(vim) % #partitions)
  - if working in databricks use liquid
  - 
 
