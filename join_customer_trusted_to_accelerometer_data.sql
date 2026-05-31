SELECT 
   ct.*
  ,al.timestamp
  ,al.x
  ,al.y 
  ,al.z 
FROM customer_trusted AS ct
INNER JOIN accelerometer_landing AS al
ON ct.email = al.user
WHERE ct.sharewithresearchasofdate IS NOT NULL