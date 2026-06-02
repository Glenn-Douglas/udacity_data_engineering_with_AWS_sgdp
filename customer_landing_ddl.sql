CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_db`.`customer_landing` (
  `customerName` string COMMENT 'first and last name of customer',
  `email` string COMMENT 'email provided by the customer',
  `phone` string COMMENT 'phone number provided by customer',
  `birthDay` string COMMENT 'customer date of birth',
  `serialNumber` string COMMENT 'STEDI device serial number',
  `registrationDate` bigint COMMENT 'date the customer registered the device',
  `lastUpdateDate` bigint COMMENT 'date of most recent update',
  `shareWithResearchAsOfDate` bigint COMMENT 'date opted in to share data with research',
  `shareWithPublicAsOfDate` bigint COMMENT 'date opted in to share data publicly',
  `shareWithFriendsAsOfDate` bigint COMMENT 'date opted in to share data with friends'
) COMMENT "landing zone for the STEDI customer data, pulled from storage in s3."
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-sgdp-wh/customer/landing/'
TBLPROPERTIES ('classification' = 'json');