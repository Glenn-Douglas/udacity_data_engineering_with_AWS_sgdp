CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_db`.`step_trainer_landing` (
  `sensorReadingTime` bigint COMMENT 'time when read from the sensor',
  `serialNumber` string COMMENT 'unique serial number of STEDI device',
  `distanceFromObject` int COMMENT 'distance'
) COMMENT "landing zone for the step_trainer data, pulled from storage in s3."
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-sgdp-wh/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');