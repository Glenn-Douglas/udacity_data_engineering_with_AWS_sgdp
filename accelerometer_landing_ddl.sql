CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` float COMMENT 'from deserializer', 
  `y` float COMMENT 'from deserializer', 
  `z` float COMMENT 'from deserializer')
COMMENT 'landing zone for the accelerometer data, pulled from storage in s3.'
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-sgdp-wh/accelerometer/landing'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1780240621')