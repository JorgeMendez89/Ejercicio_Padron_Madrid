-- Practica formativa Bosonit


-- A partir de los datos (CSV) de Padrón de Madrid:
-- https://datos.madrid.es/egob/catalogo/200076-1-padron.csv
-- llevar a cabo lo siguiente:


-- 1. Creación de tablas en formato texto.

-- 1.1. Crear Base de datos "datos_padron"
--Drop database if exists datos_padron;
--create database datos_padron;
--Use datos padron;

-- 1.2. Crear la tabla de datos padron_txt con todos los campos del fichero CSV 
-- y cargar los datos mediante el comando LOAD DATA LOCAL INPATH. 
-- La tabla tendrá formatotexto y tendrá como delimitador de campo el caracter ';' 
-- y los campos que en el documento original están encerrados en comillas dobles '"'
-- no deben estar envueltos en estos caracteres en la tabla de Hive 
-- (es importante indicar esto utilizando el serde de OpenCSV, si no la importación 
-- de las variables que hemos indicado como numéricas fracasará ya que al estar envueltos 
-- en comillas los toma como strings) 
--y se deberá omitir la cabecera del fichero de datos al crear la tabla.

DROP TABLE padron_raw;

create table padron_raw(
COD_DISTRITO int,
DESC_DISTRITO string,
COD_DIST_BARRIO int,
DESC_BARRIO string,
COD_BARRIO int,
COD_DIST_SECCION int,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar"=';', "quoteChar"='"')  
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count" = "1");

load data local inpath '/home/cloudera/padron/padron.csv' 
into table padron_raw;

show create table padron_raw;
-- totalSize = 22 594 627, 
select * from padron_raw;
select * from padron_raw where desc_barrio like "O%"; -- no da problema con las ñ

-- crear una tabla nueva casteada ya que no estaba mostrando las clases adecuadas de las columnas
drop table padron_txt;

create table padron_txt as
select
cast(COD_DISTRITO as int) COD_DISTRITO,
cast (DESC_DISTRITO as string) DESC_DISTRITO,
cast (COD_DIST_BARRIO as int) COD_DIST_BARRIO,
cast (DESC_BARRIO as string) DESC_BARRIO,
cast (COD_BARRIO as int) COD_BARRIO,
cast (COD_DIST_SECCION as int) COD_DIST_SECCION,
cast (COD_SECCION as int) COD_SECCION,
cast (COD_EDAD_INT as int) COD_EDAD_INT,
cast (EspanolesHombres as int) EspanolesHombres,
cast (EspanolesMujeres as int) EspanolesMujeres,
cast (ExtranjerosHombres as int) ExtranjerosHombres,
cast (ExtranjerosMujeres as int) ExtranjerosMujeres
from padron_raw;

show create table padron_txt;
-- numRows = 237825' 
-- rawDataSize = 16972157' 
-- totalSize = 17 209 982'

select * from padron_txt;

-- 1.3. Hacer trim sobre los datos para eliminar los espacios innecesarios guardando la
-- tabla resultado como padron_txt_2. (Este apartado se puede hacer creando la tabla
-- con una sentencia CTAS.)
drop table padron_txt_2;

CREATE TABLE padron_txt_2 as
select 
COD_DISTRITO COD_DISTRITO,
trim(DESC_DISTRITO) DESC_DISTRITO,
COD_DIST_BARRIO COD_DIST_BARRIO,
trim(DESC_BARRIO) DESC_BARRIO,
COD_BARRIO COD_BARRIO,
COD_DIST_SECCION COD_DIST_SECCION,
COD_SECCION COD_SECCION,
COD_EDAD_INT COD_EDAD_INT,
EspanolesHombres EspanolesHombres,
EspanolesMujeres EspanolesMujeres,
ExtranjerosHombres ExtranjerosHombres,
ExtranjerosMujeres ExtranjerosMujeres
from padron_txt;

select * from padron_txt_2;
show create table padron_txt_2;
-- numRows = 237825 
-- rawDataSize = 12465434 
-- totalSize = 12 703 259 

-- 1.4. Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOAD DATA.

-- Local es para usar los archivos fuera de hdfs, al quitar local busca dentro de hdfs

-- 1.5. En este momento te habrás dado cuenta de un aspecto importante, los datos nulos
-- de nuestras tablas vienen representados por un espacio vacío y no por un identificador 
-- de nulos comprensible para la tabla. 
-- Esto puede ser un problema para el tratamiento posterior de los datos. 
-- Podrías solucionar esto creando una nueva tabla utilizando sentencias case when que sustituyan 
-- espacios en blanco por 0. 
-- Para esto primero comprobaremos que solo hay espacios en blanco en las variables numéricas 
-- correspondientes a las últimas 4 variables de nuestra tabla (podemos hacerlo con alguna sentencia 
-- de HiveQL) y luego aplicaremos las sentencias case when para sustituir por 0 los espacios en blanco. 
-- (Pista: es útil darse cuenta de que un espacio vacío es un campo con longitud 0). 
-- Haz esto solo para la tabla padron_txt. (sobre padron raw que es la tabla cargada con todos los datos string)

drop table padron_txt_3;

CREATE TABLE padron_txt_3 as
select 
cast(case when (length(COD_DISTRITO) = 0) Then "0"  else (trim(COD_DISTRITO)) end as int)COD_DISTRITO,
cast(case when (length(DESC_DISTRITO) = 0) Then "0"  else (trim(DESC_DISTRITO)) end as string)DESC_DISTRITO,
cast(case when (length(COD_DIST_BARRIO) = 0) Then "0"  else (trim(COD_DIST_BARRIO)) end as int)COD_DIST_BARRIO,
cast(case when (length(DESC_BARRIO) = 0) Then "0"  else (trim(DESC_BARRIO)) end as string)DESC_BARRIO,
cast(case when (length(COD_BARRIO) = 0) Then "0"  else (trim(COD_BARRIO)) end as string)COD_BARRIO,
cast(case when (length(COD_DIST_SECCION) = 0) Then "0"  else (trim(COD_DIST_SECCION)) end as int)COD_DIST_SECCION,
cast(case when (length(COD_SECCION) = 0) Then "0"  else (trim(COD_SECCION)) end as int)COD_SECCION,
cast(case when (length(COD_EDAD_INT) = 0) Then "0"  else (trim(COD_EDAD_INT)) end as int)COD_EDAD_INT,
cast(case when (length(EspanolesHombres) = 0) Then "0"  else (trim(EspanolesHombres)) end as int)EspanolesHombres,
cast(case when (length(EspanolesMujeres) = 0) Then "0"  else (trim(EspanolesMujeres)) end as int)EspanolesMujeres,
cast(case when (length(ExtranjerosHombres) = 0) Then "0"  else (trim(ExtranjerosHombres)) end as int)ExtranjerosHombres,
cast(case when (length(ExtranjerosMujeres) = 0) Then "0"  else (trim(ExtranjerosMujeres)) end as int)ExtranjerosMujeres
FROM padron_raw;

select * from padron_txt_3;
show create table padron_txt_3;
-- numRows = 237825 
-- rawDataSize = 11709190 
-- totalSize = 12 422 665 

-- Una manera tremendamente potente de solucionar todos los problemas previos
-- (tanto las comillas como los campos vacíos que no son catalogados como null y los
-- espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona
-- OpenCSV.
-- Para ello utilizamos :
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
-- WITH SERDEPROPERTIES ('input.regex'='XXXXXXX')
-- Donde XXXXXX representa una expresión regular que debes completar y que
-- identifique el formato exacto con el que debemos interpretar cada una de las filas de
-- nuestro CSV de entrada. Para ello puede ser útil el portal "regex101". Utiliza este método
-- para crear de nuevo la tabla padron_txt_2.
-- Una vez finalizados todos estos apartados deberíamos tener una tabla padron_txt que
-- conserve los espacios innecesarios, no tenga comillas envolviendo los campos y los campos
-- nulos sean tratados como valor 0 y otra tabla padron_txt_2 sin espacios innecesarios, sin
-- comillas envolviendo los campos y con los campos nulos como valor 0. Idealmente esta
-- tabla ha sido creada con las regex de OpenCSV.

-- "(\d+)";"(.*?)\s*";"(\d+)";"(.*?)\s*";"(\d+)";"(\d+)";"(\d+)";"(\d+)";"(\d+)";"(\d+)";"(\d+)";"(\d+)"

drop table padron_txt_reg;

create table padron_txt_reg(
COD_DISTRITO int,
DESC_DISTRITO string,
COD_DIST_BARRIO int,
DESC_BARRIO string,
COD_BARRIO int,
COD_DIST_SECCION int,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex'='"(\\d*)"\;"(.*?)\\s*"\;"(\\d*)"\;"(.*?)\s*"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"',"serialization.encoding"="UTF-8")
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count" = "1");

load data local inpath '/home/cloudera/padron/padron.csv' 
into table padron_txt_reg;

select * from padron_txt_reg;
show create table padron_txt_reg;
-- 'totalSize'='22594627'

-- cambiar codificacion de el archivo csv a utf8 para que pueda reconocer las ñ

select * from padron_txt_reg where Upper(desc_barrio) LIKE "O%";


-- 2- Investigamos el formato columnar parquet

-- 2.1 ¿Qué es CTAS?
-- crear una tabla a partir de otra, haciendo select de las columnas de las colmnas que se quieran tomar
-- para esta nueva tabla

-- 2.2Crear tabla Hive padron_parquet (cuyos datos serán almacenados en el formato
-- columnar parquet) a través de la tabla padron_txt mediante un CTAS.

DROP TABLE padron_parquet;

create table padron_parquet
STORED AS PARQUET
as select
cast(COD_DISTRITO as int) COD_DISTRITO,
cast(DESC_DISTRITO as string) DESC_DISTRITO,
cast(COD_DIST_BARRIO as int) COD_DIST_BARRIO,
cast(DESC_BARRIO as string) DESC_BARRI,
cast(COD_BARRIO as int) COD_BARRIO,
cast(COD_DIST_SECCION as int) COD_DIST_SECCION,
cast(COD_SECCION as int) COD_SECCION,
cast(COD_EDAD_INT as int) COD_EDAD_INT,
cast(EspanolesHombres as int) EspanolesHombres,
cast(EspanolesMujeres as int) EspanolesMujeres,
cast(ExtranjerosHombres as int) ExtranjerosHombres,
cast(ExtranjerosMujeres as int) ExtranjerosMujeres
from padron_raw;

select * from  padron_parquet;
show create table padron_parquet;
-- numRows = 237825 
-- rawDataSize = 2853900 
-- totalSize = 876 046 

-- 2.3 Crear tabla Hive padron_parquet_2 a través de la tabla padron_txt_2 mediante un
-- CTAS. En este punto deberíamos tener 4 tablas, 2 en txt (padron_txt y
-- padron_txt_2, la primera con espacios innecesarios y la segunda sin espacios
-- innecesarios) y otras dos tablas en formato parquet (padron_parquet y
-- padron_parquet_2, la primera con espacios y la segunda sin ellos).

drop table padron_parquet_2;

create table padron_parquet_2
STORED AS PARQUET
as
select *
from padron_txt_2;

select * from  padron_parquet_2;
show create table padron_parquet_2;
-- numRows = 237825  
-- rawDataSize = 2853900  
-- totalSize = 874 007  

-- crear parquet de la tabla que menos ha pesado como txt
drop table padron_parquet_3;

create table padron_parquet_3
STORED AS PARQUET
as
select *
from padron_txt_3;

select * from  padron_parquet_3;
show create table padron_parquet_3;
-- numRows = 237825 
-- rawDataSize = 2616075 
-- totalSize = 937 485 


-- 2.4 Opcionalmente también se pueden crear las tablas directamente desde 0 (en lugar
-- de mediante CTAS) en formato parquet igual que lo hicimos para el formato txt
-- incluyendo la sentencia STORED AS PARQUET. Es importante para comparaciones
-- posteriores que la tabla padron_parquet conserve los espacios innecesarios y la
-- tabla padron_parquet_2 no los tenga. Dejo a tu elección cómo hacerlo.

drop table padron_parquet_reg;

create table padron_parquet_reg(
COD_DISTRITO int,
DESC_DISTRITO string,
COD_DIST_BARRIO int,
DESC_BARRIO string,
COD_BARRIO int,
COD_DIST_SECCION int,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex'='"(\\d*)"\;"(.*?)\\s*"\;"(\\d*)"\;"(.*?)\s*"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"\;"(\\d*)"',"serialization.encoding"="UTF-8")
STORED AS PARQUET
TBLPROPERTIES("skip.header.line.count" = "1");

-- ale ejecutar este llenado de la tabla, se hace sin problema, pero al ser lazy, cuando se quiere hacer un select
-- muestra que no se puede llenar una tabla parquet con un archivo que no sea parquet, en este caso csv
load data local inpath '/home/cloudera/padron/padron.csv' 
into table padron_parquet_reg;

-- asi que la alternativa es el metodo utilizado anteriormente, con una tabla creada primero como txt y luego pasada a f
-- formato parquet
-- select * from  padron_parquet_reg;


-- 2.5. Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar
-- con este tipo de formatos.

-- El formato Parquet es un formato open-source de almacenamiento en columnas para Hadoop.
-- Fue creado para poder disponer de un formato libre de compresión y codificación eficiente.
-- El formato de Parquet está compuesto por tres piezas:

-- Row group: es un conjunto de filas en formato columnar, con un tamaño entre 50Mb a 1Gb.
-- Column chunk: son los datos de una columna en un grupo. Se puede leer de manera independiente para mejorar las lecturas.
-- Page: Es donde finalmente se almacenan los datos debe ser lo suficiente grande para que la compresión sea eficiente. 

-- En entornos YARN es necesario indicar cuánta memoria puede utilizar un nodo para asignar recursos con el parámetro.
-- Los tipos de compresión recomendados con este formato son:
-- snappy (valor predeterminado)
-- gzip

-- 2.6. Comparar el tamaño de los ficheros de los datos de las tablas padron_txt (txt),
-- padron_txt_2 (txt pero no incluye los espacios innecesarios), padron_parquet y
-- padron_parquet_2 (alojados en hdfs cuya ruta se puede obtener de la propiedad
-- location de cada tabla por ejemplo haciendo "show create table").

--padron_raw (txt con serde sin cast - colomunas como string)
-- totalSize = 22 594 627, 

--padron_txt (ctas de padron_raw con cast para los tipos de columnas)
-- numRows = 237 825
-- rawDataSize = 16 972 157
-- totalSize = 17 209 982

--table padron_txt_2 (ctas de padron_txt con trim en columnas string)
-- numRows = 237 825 
-- rawDataSize = 12 465 434 
-- totalSize = 12 703 259 

--table padron_txt_3 (ctas de padron_raw poniendo 0 a los valores vacios y cast a topdas las columnas)
-- numRows = 237 825 
-- rawDataSize = 11 709 190 
-- totalSize = 12 422 665

--table padron_txt_reg (desde 0 con expresiones regulares - columnas como string)
-- totalSize = 22 594 627

--table padron_parquet (ctas de padron_raw con cast a las columnas)
-- numRows = 237 825 
-- rawDataSize = 2 853 900 
-- totalSize = 876 046 

--table padron_parquet_2 (ctas de padron_txt_2)
-- numRows = 237 825  
-- rawDataSize = 2 853 900  
-- totalSize = 874 007 

--table padron_parquet_3 (ctas de padron_txt_3)
-- numRows = 237 825 
-- rawDataSize = 2 616 075 
-- totalSize = 937 485 


-- 3. Juguemos con Impala.
-- 3.6. Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2
-- (No deberían incluir espacios innecesarios). ¿Alguna conclusión?
select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_txt_3
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_parquet_3
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- el tiempo de respuesta es mucho mayor en hive que en impala, ademas de que hive no reconoce la ñ en un nombre de columna.

-- 4. Sobre tablas particionadas.
-- 4.1. Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y DESC_BARRIO 
-- cuyos datos estén en formato parquet.

DROP TABLE padron_particionada;

create table padron_particionada(
COD_DISTRITO int,
COD_DIST_BARRIO int,
COD_BARRIO int,
COD_DIST_SECCION int,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int
)
PARTITIONED BY (DESC_DISTRITO string, DESC_BARRIO string)
STORED AS PARQUET;

 

-- 4.2. Insertar datos (en cada partición) dinámicamente (con Hive) 
-- en la tabla recién creada a partir de un select de la tabla padron_parquet_2.


-- Antes de insertar los datos se aplica la configuracion de partion en hive
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;


-- ahora se ejecuta el llenado de la tabla
FROM padron_parquet_3
INSERT OVERWRITE TABLE padron_particionada
PARTITION(desc_distrito, desc_barrio)
SELECT cod_distrito, cod_dist_barrio, cod_barrio, cod_dist_seccion,
cod_seccion, cod_edad_int, espanoleshombres, espanolesmujeres, extranjeroshombres, 
extranjerosmujeres, desc_distrito, desc_barrio;

describe padron_particionada;
 
--4.5. Llevar a cabo la consulta en Hive en las tablas padron_parquet y padron_partitionado. ¿Alguna conclusión?
select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_parquet_2
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_particionada
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- tardan mas o menos lo mismo, pero mucho mas lento que impala 

-- 4.7. Hacer consultas de agregación (Max, Min, Avg, Count) tal cual el ejemplo anterior con las 3 tablas 
-- (padron_txt_2, padron_parquet_2 y padron_particionado) y comparar rendimientos tanto en Hive como en Impala 
-- y sacar conclusiones.

-- padron_txt_3
select max(espanoleshombres), max(espanolesmujeres), max(extranjeroshombres), max(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_txt_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select min(espanoleshombres), min(espanolesmujeres), min(extranjeroshombres), min(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_txt_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select avg(espanoleshombres), avg(espanolesmujeres),avg(extranjeroshombres), avg(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_txt_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select count(espanoleshombres), count(espanolesmujeres),count(extranjeroshombres), count(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_txt_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

--padron_parquet_3
select max(espanoleshombres), max(espanolesmujeres), max(extranjeroshombres), max(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_parquet_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select min(espanoleshombres), min(espanolesmujeres), min(extranjeroshombres), min(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_parquet_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select avg(espanoleshombres), avg(espanolesmujeres),avg(extranjeroshombres), avg(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_parquet_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select count(espanoleshombres), count(espanolesmujeres),count(extranjeroshombres), count(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_parquet_3
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- padron_particionada
select max(espanoleshombres), max(espanolesmujeres), max(extranjeroshombres), max(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_particionada
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select min(espanoleshombres), min(espanolesmujeres), min(extranjeroshombres), min(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_particionada
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select avg(espanoleshombres), avg(espanolesmujeres),avg(extranjeroshombres), avg(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_particionada
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select count(espanoleshombres), count(espanolesmujeres),count(extranjeroshombres), count(extranjerosmujeres), desc_distrito distrito, desc_barrio barrio
from padron_particionada
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- mucho mas lento que IMpala tal como en los casos anteriores. Ademas de que al no ponerle nombre a las columnas
-- de las funciones agregadas, le coloca _c0, _c1 etc... 