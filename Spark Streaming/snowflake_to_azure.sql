create storage integration s3_retail_bucket type = external_stage storage_provider = s3 storage_aws_role_arn = 'arn:aws:iam::680832645642:role/snowflake-role';

SHOW INTEGRATIONS LIKE 's3_retail_bucket';

DESC INTEGRATION s3_retail_bucket;

CREATE STAGE retail_gold URL = 's3://retail-bucket-s3/gold/ubs/orders/' STORAGE_INTEGRATION = S3_RETAIL_BUCKET DIRECTORY = (ENABLE = true);

use database ORDERS;
use schema DEV;

CREATE 
OR REPLACE EXTERNAL TABLE delta_manifest_table(
  filename VARCHAR AS split_part(VALUE : c1, '/', -1)
) WITH LOCATION = @retail_gold / _symlink_format_manifest / FILE_FORMAT = (TYPE = CSV) PATTERN = '.*[/]manifest' AUTO_REFRESH = true;


SELECT 
  * 
FROM 
  delta_manifest_table 
LIMIT 
  10;
  
CREATE 
OR REPLACE EXTERNAL TABLE ubs_orders(
  order_id STRING AS (VALUE : order_id :: STRING), 
  order_date DATE AS (VALUE : order_date :: DATE), 
  product_name STRING AS (VALUE : product_name :: STRING), 
  quantity INT AS (VALUE : quantity :: INT), 
  unit_price DOUBLE AS (VALUE : unit_price :: DOUBLE), 
  total_price DOUBLE AS (VALUE : total_price :: DOUBLE), 
  item_id STRING AS (VALUE : item_id :: STRING), 
  parquet_filename VARCHAR AS split_part(metadata$filename, '/', -1)
) WITH LOCATION = @retail_gold / FILE_FORMAT = (TYPE = PARQUET) PATTERN = '.*[/]part-[^/]*[.]parquet' AUTO_REFRESH = true;

ALTER EXTERNAL TABLE IF EXISTS ubs_orders REFRESH;



SELECT 
  * 
FROM 
  ubs_orders;

  
SELECT 
  count(*)
FROM 
  ubs_orders;

CREATE MATERIALIZED VIEW ubs_orders_view
  AS  SELECT order_id,order_date,product_name,quantity,unit_price,total_price,item_id FROM ubs_orders;

SELECT * FROM ubs_orders_view;


CREATE OR REPLACE STAGE relax_azure_storage
  URL='azure://relaxunfi.blob.core.windows.net/relax'
  CREDENTIALS=(AZURE_SAS_TOKEN='?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-09-26T18:27:35Z&st=2023-09-26T10:27:35Z&spr=https,http&sig=QaoVGmg2SCSdI6GWXGT1EcpA36RD4M68wWfo21sShxY%3D')
  FILE_FORMAT = (TYPE = CSV);


  COPY INTO @relax_azure_storage from ubs_orders_view;
  
