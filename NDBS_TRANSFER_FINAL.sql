--**************begin of the legacy food script*********************--
--******************************************************************--

--1 Getting Ready
--1.1 target database
--the target database name at JIFSAN
USE usda_foods_ndb_no_transfer_test_4_183;       

--1.2 --add "Ingredients" to the Food Group table 
--***********only run this block of code if the "Ingredients" Food Group does NOT exist in the Food Group table
--**********************************************************************************************
INSERT INTO food_group VALUES (29, 2400, 'Ingredients', GETDATE());     --today's date
UPDATE  [food_group]
SET [last_updated] = CAST(GETDATE() as date)  
WHERE [id] = '29';
--**********************************************************************************************
----------------end of 1-----------------------





--2 adding NDB_NO foods to food table

--2.1 beginning of reindexing the NDB_NO foods

--2.1.1 create a view to listing all the NDB_NO foods we are transfering (they are also compiled foods)
--also identify their old id
create view compiled_food_update2 as   
SELECT 
  FI.IDENTIFIER
  FROM [ndbs].[dbo].[FOOD_ITEM] FI
  where NDB_NO IS NOT NULL ;        -- foods has NBD_NO



--2.1.2 insert the NDB_NO foods to the food_reindex table with its old id and assign the new id to them
--13115 records
INSERT INTO food_reindex 
select 
NEXT VALUE FOR food_seq OVER (ORDER BY cfu.IDENTIFIER) AS new_id,      --seq affected 
cfu.IDENTIFIER AS old_id
from compiled_food_update2 cfu;   
---------------end of 2.1---------------------------------------------------------------------------


--2.2 beginning of transfering NBD_NO foods to the food table

--2.2.1 create a view for the NDB_BO foods with fileds that line up with the fileds in the food table
CREATE VIEW new_food_transfer_compiled AS
SELECT 
    FI.IDENTIFIER,
	CONVERT (VARCHAR(255), FI.EDITED_NAME) AS [description], 
    CONVERT (VARCHAR(255), FI.SOURCE_DESCRIPTION) AS source_description,
	FI.ADDMOD_DATE,
	CONCAT([FOOD_GROUP_ID], '00') AS FOOD_GROUP_CODE,                        --the NDBS_FOOD_GROUP_ID formatted like "13", but the respective field(food_code) in food_group table formatted like "1300"
	                                                                         --thus we need to concat "00" at the end of each NDBS_FOOD_GROUP_ID to ensure a successful join
	FI.STATUS,
	FI.PERSON_IDENTIFIER,
	FR.new_id,
	FI.SCIENTIFIC_NAME
FROM [ndbs].[DBO].[FOOD_ITEM] FI
JOIN food_reindex fr ON fr.old_id = FI.IDENTIFIER
where  FI.NDB_NO is not null ;          -- only the foods has NBD_NO

--2.2.2 insert into the food table 
--the fields in the new_food_transfer_compiled VIEW is already lined up with the fileds in the food table, now we transfer the records from the VIEW to the food table
--13115 records
INSERT INTO food (id, description,  food_group_id, last_updated, status, last_updated_by,scientific_name)
SELECT 
CONVERT (BIGINT, new_id) AS id, 
IIF (NF.description IS NULL, NF.source_description, NF.description) as description ,                                   --use FOOD_ITEM.SOURCE_DESCRIPTION if FOOD_ITEM.DESCRIPTION is null
CONVERT (BIGINT, FG.id) as food_group_id, 
CONVERT (DATETIME, NF.ADDMOD_DATE )as last_updated, 
CONVERT(VARCHAR(20) ,STATUS) as [status], 
CONVERT(VARCHAR(50) , PERSON_IDENTIFIER) as [last_updated_by],
NF.SCIENTIFIC_NAME as scientific_name
FROM new_food_transfer_compiled as NF
JOIN food_group FG
ON FG.code = NF.FOOD_GROUP_CODE;                                           
---------------end of 2.2-------------------------------------------------------------------







--3 beginning of transfering NBD_NO foods column names to the food_attribute table----------------------------------------

--3.1 insert the food column names and other associated food information to the food_attribute table
--2540 records
Insert into food_attribute(id, food_id, food_attribute_type_id, value, last_updated_by, last_updated)    
select 
 NEXT VALUE FOR food_attribute_seq OVER (ORDER BY f.id) AS id,       --seq affected 
 fr.new_id as food_id,
 '1000' as food_attribute_type_id,                                   --we assumed the food_attribute_type_id (fk) is "1000" for all records, reference food_attribute table
CN.NAME as value,
'IMPORTED' as last_updated_by                                        --we assumed te last_updated_by is "IMPORTED" for all records
, CAST(GETDATE() AS DATETIME )as  last_updated                       --today's date
FROM [ndbs].[DBO].[FOOD_ITEM] FI
JOIN food_reindex fr ON fr.old_id = FI.IDENTIFIER
JOIN [ndbs].[dbo].[COMMON_NAME] CN
ON FI.IDENTIFIER = CN.F_I_IDENTIFIER
JOIN food f on f.id = fr.new_id
where FI.NDB_NO is not null ;                                         -- foods has NBD_NO
------------end of 3 -----------------------------------------------







--4 beginning of transfering the final_food table ------------------------------------------------------------------------------

--4.1 insert into NDB_NO foods with its NBD_number and its new Identifier from reindexing into the final_food table  
--13115 records
INSERT INTO final_food (food_id, NDB_number)  
SELECT 
fr.new_id AS food_id,
FI.NDB_NO AS NDB_number
FROM [ndbs].[DBO].[FOOD_ITEM] FI
JOIN food_reindex fr ON fr.old_id = FI.IDENTIFIER
where  FI.NDB_NO is not null;         -- foods has NBD_NO
------------end of 4------------------------------------------------------------------------------------







--5 beginning of transfering the food conversion_factor tables--------------------------------------------------------


--5.1 create a table that considers all the 5 five factors (for 3 target tables) with atuo-incremented id
CREATE table nutrient_conversion (
ncid BIGINT  NOT NULL IDENTITY (1, 1) PRIMARY KEY ,
IDENTIFIER int ,
PRO_CAL_FACTOR varchar (255),
FAT_CAL_FACTOR varchar(255),
CARBOHYDRATE_CAL_FACTOR varchar(255),
LIPID_CONVERSION_FACTOR varchar(255),
N_TO_PROT_CONV_FACTOR varchar(255),
new_id int)

--5.2 insert N_TO_PROT_CONV_FACTOR to the newly created nutrient_conversion table and asign records with auto-incremented id
insert into nutrient_conversion  (IDENTIFIER, N_TO_PROT_CONV_FACTOR,new_id )
select 
IDENTIFIER,
N_TO_PROT_CONV_FACTOR,
fr.new_id
from 
[ndbs].[DBO].[FOOD_ITEM] FI 
join food_reindex fr on fr.old_id = FI.IDENTIFIER
where FI.NDB_NO is not null and FI.N_TO_PROT_CONV_FACTOR is not null;     --Food has NDB_NO and N_TO_PROT_CONV_FACTOR is not null


--5.3 insert N_TO_PROT_CONV_FACTOR into final_food_nutrient_conversion_factor table.
--**********************must run this block of code all together (declaring a local variable to store the id value of nutrient_conversion_factor_seq)
--*****************************************************************************************************************************************************
--9051 rows
DECLARE @ncf_value int ;
SET @ncf_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'nutrient_conversion_factor_seq');  
insert into final_food_nutrient_conversion_factor (id, final_food_id, last_updated,last_updated_by)
SELECT NEXT VALUE FOR nutrient_conversion_factor_seq OVER (ORDER BY nc.ncid ),
nc.new_id,
GETDATE(),                                                                                             --today's date (last_updated)
'IMPORTED'                                                                                             --IMPORTED (last_updated by)
from nutrient_conversion nc ;
insert into final_food_protein_conversion_factor 
SELECT ffncf.id, N_TO_PROT_CONV_FACTOR
FROM  nutrient_conversion nc
join  final_food_nutrient_conversion_factor  ffncf on ffncf.final_food_id = nc.new_id
where ffncf.id >= @ncf_value and ffncf.id < (@ncf_value + 9051);         --increase the current nutrient_conversion_factor_seq by 9051 and only insert this range of records
--**************************************************************************************************************************************************




--5.4 insert LIPID_CONVERSION_FACTOR to the newly created nutrient_conversion table and asign records with auto-incremented id
insert into nutrient_conversion  (IDENTIFIER,[LIPID_CONVERSION_FACTOR],new_id )  --5973
select 
IDENTIFIER,
[LIPID_CONVERSION_FACTOR],
fr.new_id
from   
[ndbs].[DBO].[FOOD_ITEM] FI 
join food_reindex fr on fr.old_id = FI.IDENTIFIER
where FI.NDB_NO is not null and LIPID_CONVERSION_FACTOR is not null;     --Food has NDB_NO and LIPID_CONVERSION_FACTOR is not null

--5.5 insert LIPID_CONVERSION_FACTOR into final_food_nutrient_conversion_factor table.
--**********************must run this block of code all together (declaring a local variable to store the id value of nutrient_conversion_factor_seq)
--*****************************************************************************************************************************************************
--5973 rows
DECLARE @ncf_value int ;   
SET @ncf_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'nutrient_conversion_factor_seq');  
insert into final_food_nutrient_conversion_factor (id, final_food_id, last_updated,last_updated_by)
SELECT 
NEXT VALUE FOR nutrient_conversion_factor_seq OVER (ORDER BY nc.ncid ),
nc.new_id,
GETDATE(),                                                                                       --today's date (last_updated)
'IMPORTED'                                                                                        --IMPORTED (last_updated by)
from nutrient_conversion nc
where nc.LIPID_CONVERSION_FACTOR is not null ;
insert into final_food_fat_conversion_factor 
SELECT ffncf.id, LIPID_CONVERSION_FACTOR
FROM  nutrient_conversion nc
join  final_food_nutrient_conversion_factor  ffncf on ffncf.final_food_id = nc.new_id
where (ffncf.id >= @ncf_value and ffncf.id <= @ncf_value +5973 ) and nc.LIPID_CONVERSION_FACTOR is not null ;


--16023 id 

----------------------------------------------------------------------------------------------------------------


insert into nutrient_conversion  (IDENTIFIER,  [PRO_CAL_FACTOR]  ,new_id )    --5961 rows
select 
IDENTIFIER,
[PRO_CAL_FACTOR],
fr.new_id
from 
[ndbs].[DBO].[FOOD_ITEM] FI 
join food_reindex fr on fr.old_id = FI.IDENTIFIER
where FI.NDB_NO is not null and [PRO_CAL_FACTOR] is not null;    



DECLARE @ncf_value int ;   
SET @ncf_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'nutrient_conversion_factor_seq'); 
insert into final_food_nutrient_conversion_factor (id, final_food_id, last_updated,last_updated_by)
SELECT 
NEXT VALUE FOR nutrient_conversion_factor_seq OVER (ORDER BY nc.ncid ),
nc.new_id,
GETDATE(),
'IMPORTED'
from nutrient_conversion nc
where nc.[PRO_CAL_FACTOR]  is not null ;


insert into final_food_calorie_conversion_factor (final_food_nutrient_conversion_factor_id, protein_value)
SELECT ffncf.id, [PRO_CAL_FACTOR]
FROM  nutrient_conversion nc
join  final_food_nutrient_conversion_factor  ffncf on ffncf.final_food_id = nc.new_id
where (ffncf.id >= @ncf_value  and ffncf.id <= (@ncf_value+5961) ) and nc.[PRO_CAL_FACTOR] is not null; 

--21984 id

------------------------------------------------------------------------

insert into nutrient_conversion  (IDENTIFIER,  [FAT_CAL_FACTOR]  ,new_id )    --6067 rows
select 
IDENTIFIER,
[FAT_CAL_FACTOR],
fr.new_id
from 
[ndbs].[DBO].[FOOD_ITEM] FI 
join food_reindex fr on fr.old_id = FI.IDENTIFIER
where FI.NDB_NO is not null and [FAT_CAL_FACTOR] is not null;    





insert into final_food_nutrient_conversion_factor (id, final_food_id, last_updated,last_updated_by)   --6067 rows
SELECT 
NEXT VALUE FOR nutrient_conversion_factor_seq OVER (ORDER BY nc.ncid ),
nc.new_id,
GETDATE(),
'IMPORTED'
from nutrient_conversion nc
where nc.[FAT_CAL_FACTOR]  is not null ;


DECLARE @ncf_value int ;   
SET @ncf_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'nutrient_conversion_factor_seq');  
insert into final_food_calorie_conversion_factor (final_food_nutrient_conversion_factor_id, fat_value)
SELECT ffncf.id, [FAT_CAL_FACTOR]
FROM  nutrient_conversion nc
join  final_food_nutrient_conversion_factor  ffncf on ffncf.final_food_id = nc.new_id
where (ffncf.id > (@ncf_value - 6067) and ffncf.id <= @ncf_value)  and nc.[FAT_CAL_FACTOR] is not null; 


--28051 id --6067 rows inseted

--------------------------------------------------------
insert into nutrient_conversion  (IDENTIFIER,  [CARBOHYDRATE_CAL_FACTOR]  ,new_id )    --5965 rows
select 
IDENTIFIER,
CARBOHYDRATE_CAL_FACTOR,
fr.new_id
from 
[ndbs].[DBO].[FOOD_ITEM] FI 
join food_reindex fr on fr.old_id = FI.IDENTIFIER
where FI.NDB_NO is not null and [CARBOHYDRATE_CAL_FACTOR] is not null;    


insert into final_food_nutrient_conversion_factor (id, final_food_id, last_updated,last_updated_by)   --5965 rows
SELECT 
NEXT VALUE FOR nutrient_conversion_factor_seq OVER (ORDER BY nc.ncid ),
nc.new_id,
GETDATE(),
'IMPORTED'
from nutrient_conversion nc
where nc.[CARBOHYDRATE_CAL_FACTOR]  is not null ;


DECLARE @ncf_value int ;   
SET @ncf_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'nutrient_conversion_factor_seq');  
insert into final_food_calorie_conversion_factor (final_food_nutrient_conversion_factor_id, carbohydrate_value)
SELECT ffncf.id, [CARBOHYDRATE_CAL_FACTOR]
FROM  nutrient_conversion nc
join  final_food_nutrient_conversion_factor  ffncf on ffncf.final_food_id = nc.new_id
where (ffncf.id > (@ncf_value - 5965) and ffncf.id <= @ncf_value)  and nc.[CARBOHYDRATE_CAL_FACTOR] is not null; 
--34016 id


drop table nutrient_conversion ;
----------------------------------------------------------------------


-----transfer nutrient ----

CREATE TABLE food_nutrient_reindex
(new_id bigint not null primary key,
old_id bigint)


--958271 rows  
create view nutrient_value_transfer as 
SELECT 
 NV.F_I_IDENTIFIER,
 NV.N_NUMBER AS NUTRIENT_NUMBER, 
n.nutrient_nbr,
 NV.IDENTIFIER AS NUTRIENT_VALUE_IDENTIFIER,
 NV.VALUE
 FROM  [NDBS].[dbo].[NUTRIENT_VALUE] NV
 JOIN [NDBS].[dbo].FOOD_ITEM  FI ON NV.F_I_IDENTIFIER = FI.IDENTIFIER
 JOIN nutrient n ON n.nutrient_nbr = NV.N_NUMBER    --------lost 136 records
 where 
 NV.VALUE is NOT null  
 and FI.NDB_NO IS NOT NULL;



 --958271 rows

INSERT INTO food_nutrient_reindex (new_id , old_id)
SELECT
  NEXT VALUE FOR food_nutrient_seq OVER (ORDER BY NVT.NUTRIENT_VALUE_IDENTIFIER) AS new_id, 
  NVT.NUTRIENT_VALUE_IDENTIFIER as old_id
  FROM nutrient_value_transfer NVT


 
DECLARE @fnr_value int ;   
SET @fnr_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'food_nutrient_seq');  
INSERT INTO food_nutrient   
(id, food_id, nutrient_id, data_points, standard_error,derivation_id, last_updated, value, min, max, degrees_of_freedom, median)
SELECT
   fnr.new_id as id,
   fr.new_id as food_id,
   n.id  as nutrient_id,                            
   NV.NUMBER_OF_SAMPLES as data_points,
   NV.STD_DEV as standard_dev, 
   nd.id as derivation_id,
   IIF(NV.[ADDMOD_DATE] IS NULL, '1998-03-01'  ,NV.[ADDMOD_DATE] ) as  last_updated,    ---- SET AS 30 years ago
   NV.VALUE,
   NV.MIN_VALUE as min,  
   NV.MAX_VALUE as max,
   NV.DEG_FREE,
   NV.MEDIAN_QUANTITY      
   FROM [NDBS].[dbo].[NUTRIENT_VALUE] NV
     JOIN nutrient n                                        
     on n.nutrient_nbr = NV.N_NUMBER
          LEFT  JOIN food_nutrient_derivation nd                       
          on NV.DERIV_CODE = nd.code
		     JOIN food_reindex fr ON fr.old_id = NV.F_I_IDENTIFIER
			 JOIN NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = fr.old_id
	  JOIN food_nutrient_reindex fnr 
	  on fnr.old_id = NV.IDENTIFIER	
	  WHERE  (fnr.new_id > (@fnr_value -958271) and fnr.new_id <= @fnr_value ) and FI.NDB_NO is not null;

	  
----------------  
DECLARE @fnr_value int ;   
SET @fnr_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'food_nutrient_seq');    
 Insert into lab_analysis_sub_sample_result (food_nutrient_id, nutrient_name,  unit, original_value)
 select  
 fn.id as food_nutrient_id,
 convert(varchar(50), n.name )as nutrient_name,
 
 n.UNIT_NAME AS unit,
 nv.INIT_VALUE

 from 
 food_nutrient fn
 join  nutrient n
 on n.id = fn.nutrient_id
 join food_reindex fr
 on fr.new_id = fn.food_id
 JOIN food_nutrient_reindex fnr  on fnr.new_id = fn.id
 JOIN NDBS.DBO.NUTRIENT_VALUE NV on nv.IDENTIFIER = fnr.old_id
 JOIN NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = NV.F_I_IDENTIFIER
 WHERE  (fnr.new_id > (@fnr_value -958271) and fnr.new_id <= @fnr_value ) AND FI.NDB_NO IS NOT NULL;


 --------------------------


 --18993
INSERT INTO food_component_reindex(new_id, old_id)
SELECT 

NEXT VALUE FOR food_component_seq OVER (ORDER BY  FC.[IDENTIFIER]) AS new_id,
FC.[IDENTIFIER]   as old_id   
 FROM [NDBS].[dbo].[FOOD_COMPONENT] FC
 JOIN [NDBS].[dbo].[FOOD_ITEM] FI
 ON FI.IDENTIFIER = FC.F_I_IDENTIFIER
 WHERE FI.NDB_NO is not null;



 --18993
DECLARE @fcr_value int ;   
SET     @fcr_value = (SELECT convert(int, current_value) FROM sys.sequences WHERE name = 'food_component_seq');  
  INSERT INTO food_component 
      ([id] ,[food_id] ,[name] ,[pct_weight] ,[is_refuse] ,[gram_weight] ,[is_rejected] ,[last_updated] ,[data_points] ,[standard_error]
,[reject_comments])
SELECT fcr.new_id as id
      ,fr.new_id as food_id
	     
	  ,IIF (FC.[NAME] IS NULL, ' ', FC.[NAME])  as name                                        ---------------- space
      ,FC.[PERCENT_WEIGHT] as pct_weight
	  ,IIF(FC.[REFUSE_IND] IS NULL, 'N', FC.[REFUSE_IND]) as is_refuse                                 -----------
      ,FC.[GRAM_WEIGHT] as gram_weight
	  ,IIF(FC.[REJECT_FLAG] IS NULL, 'N',FC.[REJECT_FLAG])  as is_rejected  ,                          ----------------
CASE WHEN FC.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NOT NULL THEN CAST (FI.[CREATION_DATE] AS DATETIME)   
WHEN FC.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NULL THEN '1998-03-01'                                             ------30 years ago
ELSE CAST (FC.[ADDMOD_DATE] AS DATETIME) END AS [last_updated],
	  IIF ( FC.[NUM_SAMPLES] is null, '-1' ,  FC.[NUM_SAMPLES] )as data_points                                                -----------   -1
	  ,FC.[STD_DEV] as std_deviation
	  ,FC.[INCLUDE_FLAG_IND] as reject_comments
   FROM [NDBS].[dbo].[FOOD_COMPONENT] FC
   JOIN food_reindex fr
   ON fr.old_id = FC.F_I_IDENTIFIER
   JOIN [NDBS].[dbo].[FOOD_ITEM] FI
   ON FI.IDENTIFIER = FC.F_I_IDENTIFIER
   JOIN food_component_reindex fcr
   ON fcr.old_id = FC.IDENTIFIER
   WHERE FI.NDB_NO IS NOT NULL AND (fCr.new_id > (@fCr_value -19992) and fCr.new_id <= @fCr_value ) ;

   ------------------------------------------------------------


   ---FOOD MEASURE 

   ----------------------*check existing sequences
SELECT name, object_id, create_date,modify_date, start_value, current_value FROM sys.sequences WHERE name = 'food_nutrient_seq' or name = 'food_seq'
or name = 'project_seq' or name = 'food_measure_seq' or name = 'food_measure_dimension_seq' or name = 'food_component_seq'
or name = 'lab_analysis_sub_sample_seq' or name = 'lab_analysis_sub_sample_result_seq';


---27190 records 
select M.IDENTIFIER
from ndbs.dbo.MEASURE M 
join ndbs.dbo.FOOD_ITEM fi on FI.IDENTIFIER= M.F_I_IDENTIFIER
where fi.NDB_NO is not null;




CREATE VIEW  measure_unit_modify_3_1_1 AS
SELECT 
[IDENTIFIER]  
,CASE WHEN [MEASURE].[UNIT_NAME] LIKE '%,' THEN LEFT([UNIT_NAME], LEN([UNIT_NAME])-1) 
WHEN [MEASURE].[UNIT_NAME] LIKE '%.' THEN LEFT([UNIT_NAME], LEN([UNIT_NAME])-1)       
ELSE [MEASURE].[UNIT_NAME]  END AS [UNIT_NAME]
      
    ,[SURVEY_CODE]
      ,[AMOUNT]
      ,[SHAPE_NAME]
      ,[DISS_TEXT]
      ,[SOURCE_CODE]
      ,[TEXT]
      ,[WEIGHT]
      ,[WEIGHT_UNIT]
      ,[GRAM_WEIGHT]
      ,[F_I_IDENTIFIER]
      ,[NUTRIENT_BASIS]
      ,[RANK]
      ,[ADDMOD_DATE]
      ,[NUM_SAMPLES]
      ,[STD_DEV]
      ,[REJECT_FLAG]
      ,[NON_GM_WT_FLAG]
FROM [NDBS].[dbo].[MEASURE];



--correct unit names with space at the beginning
CREATE VIEW measure_unit_modify_3_1_2   AS
 SELECT    
      [IDENTIFIER]  
      ,LTRIM(RTRIM([UNIT_NAME])) AS [UNIT_NAME]
      ,[SURVEY_CODE]
      ,[AMOUNT]
      ,[SHAPE_NAME]
      ,[DISS_TEXT]
      ,[SOURCE_CODE]
      ,[TEXT]
      ,[WEIGHT]
      ,[WEIGHT_UNIT]
      ,[GRAM_WEIGHT]
      ,[F_I_IDENTIFIER]
      ,[NUTRIENT_BASIS]
      ,[RANK]
      ,[ADDMOD_DATE]
      ,[NUM_SAMPLES]
      ,[STD_DEV]
      ,[REJECT_FLAG]
      ,[NON_GM_WT_FLAG]
FROM measure_unit_modify_3_1_1 ;

--add one record into measure_unit for the unmatched record. dont add if it already exists
INSERT INTO measure_unit (id, name, abbreviation) VALUES ('9999','undetermined' ,'undetermined');


--get all unit names in the measure unit table as standard names
CREATE VIEW  king_of_unit_name_3_1 AS 
SELECT [id]
      ,[name]  AS unit_name
FROM [measure_unit]
UNION
SELECT [id]
      ,[abbreviation] AS unit_name
FROM [measure_unit];




--27190 ROWS 
CREATE VIEW match_unit_name_3_1_1 AS
SELECT 
        mum2.[UNIT_NAME] AS [OLD_UNIT_NAME]
	   ,IIF (koun.[unit_name] is NULL, 'undetermined', koun.[unit_name])  AS [new_unit_name]
	   ,IIF (koun.id is NULL, '9999', koun.id) as new_unit_id
	   ,mum2.[IDENTIFIER] AS [OLD_MEASURE_T_IDENTIFIER]   
       ,mum2.[F_I_IDENTIFIER] AS [OLD_FOOD_IDENTIFIER]
 
 FROM measure_unit_modify_3_1_2 mum2
  left join king_of_unit_name_3_1 koun  ON koun.[unit_name] = mum2.[UNIT_NAME]
 JOIN NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = mum2.F_I_IDENTIFIER
 WHERE FI.NDB_NO IS NOT NULL;


   CREATE VIEW match_unit_name_3_1_2 AS  

  SELECT  mun1.[OLD_UNIT_NAME]
         , mun1.[new_unit_name]
		 , mun1.[OLD_MEASURE_T_IDENTIFIER]   
		 , mun1.[OLD_FOOD_IDENTIFIER]
		 , fr.new_id
		 , koun.id as unit_id
		 from match_unit_name_3_1_1 mun1
		 join king_of_unit_name_3_1 koun on koun.unit_name = mun1.new_unit_name
		 Join food_reindex fr on fr.old_id = mun1.[OLD_FOOD_IDENTIFIER] ;




create view measure_reindex_3_1 as 
SELECT 
 
M.IDENTIFIER as old_id
FROM NDBS.DBO.MEASURE M
JOIN match_unit_name_3_1_2 mun2 ON mun2.OLD_MEASURE_T_IDENTIFIER = M.IDENTIFIER
JOIN NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = M.F_I_IDENTIFIER---27190

EXCEPT
select 
m.IDENTIFIER as old_id
FROM NDBS.DBO.MEASURE M
JOIN match_unit_name_3_1_2 mun2 ON mun2.OLD_MEASURE_T_IDENTIFIER = M.IDENTIFIER
JOIN NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = m.F_I_IDENTIFIER
where   m.[GRAM_WEIGHT] IS NULL;  --526




--insert new records into measure_reindex table
--26664 rows
INSERT INTO measure_reindex  (new_id, old_id)
select 
NEXT VALUE FOR food_measure_seq OVER (ORDER BY old_id) AS new_id,
old_id
from  measure_reindex_3_1;




--insert records into food_measure  -26663
		INSERT INTO food_measure 
    (   [id],[food_id],[value],[last_updated],[gram_weight],[data_points],[standard_error], [modifier],[is_rejected],reject_comments,[measure_unit_id],[rank])
 
 SELECT 
  convert(BIGINT, mr.new_id) as id,
  CONVERT (BIGINT, mun2.new_id) as food_id
 ,CONVERT (decimal(19,8) ,M.AMOUNT) AS value
, CASE WHEN M.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NOT NULL THEN CAST (FI.[CREATION_DATE] AS DATETIME)    --when ADDMOD_DATE is null, then use CREATION_DATE,
  WHEN M.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NULL THEN '1998-03-01 00:00:00.000'                     --when ADDMOD_DATE is null, CREATION_DATE is also NULL, use '2222-12-12 12:12:12.000 ELSE CAST (M.[ADDMOD_DATE] AS DATETIME) END AS [last_updated]
  ELSE CAST (M.[ADDMOD_DATE] AS DATETIME) END AS [last_updated]
	    ,CONVERT (decimal(19,8), M.[GRAM_WEIGHT]) AS  gram_weight    	  
		,CONVERT (INT, M.[NUM_SAMPLES]) AS data_points
		,convert (decimal (19, 8), M.[STD_DEV]) AS  standard_error
		,convert (varchar (50), M.[SHAPE_NAME]) AS modifer  
        ,IIF(M.[REJECT_FLAG] is null, 'N',M.[REJECT_FLAG]) AS is_rejected,           -------
	     convert (varchar (255), MC.COMMENT_TEXT) AS reject_comments,
		 mun2.unit_id as [measure_unit_id]
		,M.[RANK] AS [rank]

FROM NDBS.DBO.MEASURE M
JOIN match_unit_name_3_1_2 mun2 ON mun2.OLD_MEASURE_T_IDENTIFIER = M.IDENTIFIER
JOIN measure_reindex mr ON mr.old_id = M.IDENTIFIER
JOIN NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = M.F_I_IDENTIFIER
left JOIN NDBS.DBO.MEASURE_COMMENT MC ON MC.IDENTIFIER = M.IDENTIFIER
where m.IDENTIFIER <> '177259' and FI.NDB_NO is not null;
;   



--------

declare @m_new_id INT;
SET @m_new_id = (SELECT new_id FROM measure_reindex m where m.old_id  = '177259');

declare @food_id int;
set @food_id = (SELECT f.id from food f JOIN food_reindex fr on fr.new_id = f.id join NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = fr.old_id JOIN NDBS.DBO.MEASURE M ON M.F_I_IDENTIFIER = FI.IDENTIFIER WHERE m.IDENTIFIER = '177259')

INSERT INTO food_measure 
( [id],[food_id],[value],[last_updated],[gram_weight],[data_points],[is_rejected],reject_comments,[measure_unit_id],[rank])
 VALUES (@m_new_id, @food_id, '1' , '2010-12-10 00:00:00.000', '31' , '1', 'N', 'was 2009 changed weight to 31 per kell spreadsheet 2009', '1000', '1')

 ---------


 --------------------------
 SELECT 
mr.new_id as id,
mun2.new_id as food_id
 ,M.AMOUNT AS value
, CASE WHEN M.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NOT NULL THEN CAST (FI.[CREATION_DATE] AS DATETIME)    --when ADDMOD_DATE is null, then use CREATION_DATE,
  WHEN M.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NULL THEN '1998-3-1'                     --when ADDMOD_DATE is null, CREATION_DATE is also NULL, use '2222-12-12 12:12:12.000 ELSE CAST (M.[ADDMOD_DATE] AS DATETIME) END AS [last_updated]
  ELSE CAST (M.[ADDMOD_DATE] AS DATETIME) END AS [last_updated]
	    ,M.[GRAM_WEIGHT]  gram_weight    	  
		,M.[NUM_SAMPLES] AS data_points
		,M.[STD_DEV] AS  [standard_error]
		,M.[SHAPE_NAME] AS modifer  
        ,IIF(M.[REJECT_FLAG] is null, 'N',M.[REJECT_FLAG]) AS is_rejected,           -------
	  MC.COMMENT_TEXT AS reject_comments,
		 mun2.unit_id as [measure_unit_id]
		,M.[RANK] AS [rank]
FROM NDBS.DBO.MEASURE M
JOIN match_unit_name_3_1_2 mun2 ON mun2.OLD_MEASURE_T_IDENTIFIER = M.IDENTIFIER
JOIN NDBS.DBO.FOOD_ITEM FI ON FI.IDENTIFIER = M.F_I_IDENTIFIER
JOIN measure_reindex mr ON mr.old_id = M.IDENTIFIER
left JOIN NDBS.DBO.MEASURE_COMMENT MC ON MC.IDENTIFIER = M.IDENTIFIER
where m.IDENTIFIER = '177259';
;
--------------------------------------figure out the record.  

--936
create view  dimension_reindex_3_1 as 
SELECT 
D.[IDENTIFIER] AS old_id
  FROM [NDBS].[dbo].[DIMENSION] D
  JOIN [NDBS].[dbo].[MEASURE] M 
  ON M.IDENTIFIER = D.M_IDENTIFIER
  JOIN [NDBS].[dbo].[FOOD_ITEM] FI 
  ON FI.IDENTIFIER = M.F_I_IDENTIFIER 
  WHERE FI.NDB_NO IS NOT NULL AND D.VALUE IS NOT NULL;


--936   --id 1935    
insert into dimension_reindex  (new_id, old_id)
select
NEXT VALUE FOR food_measure_dimension_seq OVER (ORDER BY  old_id) AS new_id,
old_id from dimension_reindex_3_1;





--- 934  , MEASURE GRAM WEIGHT LOSE 2 RECORDS, EXPECTED
  insert into food_measure_dimension (id, food_measure_id, unit, value, type, last_updated)
  SELECT dr.new_id,
        mr.new_id,
   D.[UNIT]
   ,D.[VALUE] 
   ,IIF(D.[TYPE] IS NULL, 'unknown' , D.[TYPE]),


   CASE WHEN M.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NOT NULL THEN CAST (FI.[CREATION_DATE] AS DATETIME)    --when ADDMOD_DATE is null, then use CREATION_DATE,
  WHEN M.[ADDMOD_DATE] IS NULL AND FI.[CREATION_DATE] is NULL THEN '1998-03-01 00:00:00.000'                     --when ADDMOD_DATE is null, CREATION_DATE is also NULL, use '2222-12-12 12:12:12.000 ELSE CAST (M.[ADDMOD_DATE] AS DATETIME) END AS [last_updated]
  ELSE CAST (M.[ADDMOD_DATE] AS DATETIME) END AS [last_updated]


 
  FROM [NDBS].[dbo].[DIMENSION] D
  JOIN [NDBS].[dbo].[MEASURE] M 
  ON M.IDENTIFIER = D.M_IDENTIFIER
  JOIN [NDBS].[dbo].[FOOD_ITEM] FI 
  ON FI.IDENTIFIER = M.F_I_IDENTIFIER 
  JOIN measure_reindex mr on mr.old_id = M.IDENTIFIER
  JOIN dimension_reindex dr on dr.old_id = D.IDENTIFIER

  WHERE FI.NDB_NO IS NOT NULL AND D.VALUE IS NOT NULL AND M.GRAM_WEIGHT IS NOT NULL
  
  
  ----and m.GRAM_WEIGHT is null   --- causing losing two records















  
SELECT  M.IDENTIFIER AS  M_IDENTIFIER,  count(mc.IDENTIFIER)   AS  COUNT_MC_IDENTIFIER

FROM NDBS.DBO.FOOD_ITEM FI
JOIN  NDBS.DBO.MEASURE M ON FI.IDENTIFIER = M.F_I_IDENTIFIER   --27190

JOIN [NDBS].[dbo].[MEASURE_COMMENT] MC ON MC.IDENTIFIER = M.IDENTIFIER

 where FI.NDB_NO IS NOT NULL 
GROUP BY  M.IDENTIFIER   having count(mc.IDENTIFIER) >1 
order by M_IDENTIFIER
