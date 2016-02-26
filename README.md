# Data Munging with Hadoop

#### As part of the US government’s efforts to make the healthcare system more transparent and accountable, the Centers for Medicare and Medicaid Services (CMS) released data surrounding payments rendered and procedures provided by physicians to Medicare beneficiaries. The data contains information such as
- The NPI (National Provider Identifier) of the physician rendering services
- The physician’s specialty
- The code of the procedure rendered
- The place of service
- Aggregate payment information for the rendering physician

The examples in this eBook are based on the CMS data from 2013
You can download the datasets from https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Physician-and-Other-Supplier2013.html

The files are in Excel format, convert to csv before importing into Hive

## Data Quality

#### Example: Pig UDF for HCPCS Codes
Procedures rendered in the course of treating a patient are coded, often using a coding standard called Current Procedural Terminology (CPT). The CMS encodes procedures in a generalization of this scheme called HCPCS (Healthcare Common Procedure Coding System), which we use in our example.
Healthcare workers often enter these codes manually, so they are prone to errors, and data quality checks are required. A simple way to validate an HCPCS code is by using a regular expression. This is a simple example of a cell-based data quality rule.
Our example dataset has listed the HCPCS code of the procedure rendered to a Medicare beneficiary by every provider who submitted for reimbursement from the government. The following implements, using Pig, a simple validity check for HCPCS codes and generates a report of the erroneous codes and the count per code:

#### prepare tables
```
CREATE DATABASE medicare_part_b;
DROP TABLE IF EXISTS medicare_part_b.medicare_part_b_2013_text;
CREATE TABLE medicare_part_b.medicare_part_b_2013_text (
  npi string,
  nppes_provider_last_org_name string,
  nppes_provider_first_name string,
  nppes_provider_mi string,
  nppes_credentials string,
  nppes_provider_gender string,
  nppes_entity_code string,
  nppes_provider_street1 string,
  nppes_provider_street2 string,
  nppes_provider_city string,
  nppes_provider_zip string,
  nppes_provider_state string,
  nppes_provider_country string,
  provider_type string,
  medicare_participation_indicator string,
  places_of_service string,
  hcpcs_code string,
  hcpcs_desc string,
  hcpcs_drug_indicator string,
  line_srvc_cnt int,
  bene_unique_cnt int,
  bene_day_srvc_cnt int,
  average_Medicare_all_owed_amt string,
  average_submitted_chrg_amt string,
  stdev_submitted_chrg_amt string,
  average_Medicare_payment_amt string,
  stdev_Medicare_payment_amt string)
COMMENT 'Data Munging Medicare Part B Dataset 2013'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\,'
STORED AS TEXTFILE;
```

#### upload sample data
```
sudo -u hdfs hdfs dfs -put Medicare_Provider_Util_Payment_PUF_a_CY2013.csv /tmp/
sudo -u hdfs hdfs dfs -mkdir /user/admin
sudo -u hdfs hdfs dfs -chown -R admin:hdfs /user/admin
sudo -u hdfs hdfs dfs -chown -R admin:hdfs /tmp/Med*

LOAD  DATA  INPATH  '/tmp/Medicare_Provider_Util_Payment_PUF_a_CY2013.csv'
	OVERWRITE INTO TABLE medicare_part_b.medicare_part_b_2013_text;
```

#### Confirm the data is readable

```
SELECT * FROM medicare_part_b.medicare_part_b_2013_text;
```	

#### Create ORC table from text
```
DROP TABLE IF EXISTS medicare_part_b.medicare_part_b_2013_raw;	
CREATE TABLE medicare_part_b.medicare_part_b_2013_raw 
COMMENT 'Data Munging Medicare Part B Dataset 2013'
STORED AS ORC tblproperties ("orc.compress"="ZLIB") 
AS
SELECT * FROM medicare_part_b.medicare_part_b_2013_text
where npi != '';
```

#### Now in PIG
enter the grunt shell with the following command and then paste the code below

```
pig -x tez -useHCatalog

ROWS = load 'medicare_part_b.medicare_part_b_2013_raw' using org.apache.hive.hcatalog.pig.HCatLoader();
HCPCS_CODES = foreach ROWS generate hcpcs_code, REGEX_EXTRACT(hcpcs_code,'(^[A-Z0-9]\\d{3,3}[A-Z0-9]$)',1) as match;
INVALID_CODES = filter HCPCS_CODES by match is null;
INVALID_CODE_G = group INVALID_CODES by hcpcs_code;
INVALID_CODE_CNT = foreach INVALID_CODE_G generate group as hcpcs_code, COUNT(INVALID_CODES) as count;
rmf medicare_part_b/bad_codes;
STORE INVALID_CODE_CNT into 'medicare_part_b/bad_codes' using PigStorage(',');
```

We use a complex regular expression to identify valid HCPCS codes as follows:
- They start with either a capital letter or a number.
- This is followed by three digits.
- The code ends in either a capital letter or a number.
Pig’s REGEX_EXTRACT returns NULL if the regular expression match is not met, so our hcpcs_code field in relation ROWS would therefore be NULL if the HCPCS code in the original file is invalid.
The report generated can and should be used, ideally, to correct the syntactic mistakes or, at a minimum, to get a firm grasp on the expected set of bad data and types of mistakes that are most common. Our dataset, thankfully, is quite clean, though there are some empty fields.

#### inspect the results
```
hdfs dfs -ls medicare_part_b/bad_codes
hdfs dfs -cat medicare_part_b/bad_codes/part-v001-o000-r-00000 | less
```

## Feature Matrix
### Sampling

Example: Sampling with Pig, Hive, and Spark
Consider our example Medicare dataset; it’s often advantageous to take only a sample of the whole dataset for processing. Of course, there are many types of sampling and a variety of ways to accomplish this in the Hadoop ecosystem.
With Pig, there are a variety of options for sampling. The built-in SAMPLE operator provides simple probabilistic sampling:

```
DEFINE HCatLoader org.apache.hive.hcatalog.pig.HCatLoader();
ROWS = load 'medicare_part_b.medicare_part_b_2013_raw' using HCatLoader();
SAMPLE_ROWS = sample ROWS 0.2;
rmf medicare_part_b/ex2_simple_sample;
STORE SAMPLE_ROWS into 'medicare_part_b/ex2_simple_sample' using PigStorage(',');
```

### DataFu Sampling Example (DOES NOT WORK)
```
DEFINE HCatLoader org.apache.hive.hcatalog.pig.HCatLoader();
DEFINE SampleByKey datafu.pig.sampling.SampleByKey('0.2');
ROWS = load 'medicare_part_b.medicare_part_b_2013_raw' using HCatLoader();
SAMPLE_BY_PROVIDERS = filter ROWS by SampleByKey(npi);
rmf medicare_part_b/ex2_by_npi_sample;
STORE SAMPLE_BY_PROVIDERS into 'medicare_part_b/ex2_by_npi_sample' using PigStorage(',');
```

### Sampling in Hive with TABLESAMPLE
to sample 10,000 rows, randomly from the medicare_part_b_2013_raw table, whereas
```
SELECT * FROM medicare_part_b.medicare_part_b_2013_raw TABLESAMPLE(10000 ROWS)
```

to sample 20% of the original table (DOES NOT WORK)
```
SELECT * FROM medicare_part_b.medicare_part_b_2013_raw TABLESAMPLE(20 percent)
```

### Sampling in Spark
Spark has a sampling feature in its RDD and Data Frame API. For example, the following code shows how to sample a Spark data frame using the pyspark API:

start pyspark REPL with
```
/usr/hdp/current/spark-client/bin/pyspark
```

```
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
hc = HiveContext(sc)

# load medicare dataset as a Spark dataframe
rows = hc.sql("select * from medicare_part_b.medicare_part_b_2013_raw")

#Create a new Spark dataframe with 20% sample rows, without replacement
sample = rows.sample(False, 0.2)
sample.take(5)
```


