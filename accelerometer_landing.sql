CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_db`.`accelerometer_landing` (
  `user` string COMMENT 'user first and last name',
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float
) COMMENT "landing zone for the accelerometer data, pulled from storage in s3."
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-sgdp-wh/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');