-- 3. Juguemos con Impala.
-- 3.1. ¿Qué es Impala?
/* 
Es un motor SQL de elevado performance pensada para operar sobre grandes volúmenes de datos.
El motor es MPP: procesamiento masivo en paralelo
Con latencias de Milisegundos.

Impala corre sobre clusters Hadoop.
Puede ejecutar Queries sobre HDFS o Hbase.
Lee y escribe datos sobre ficheros con tipos de datos típicos de Hadoop.
Originalmente desarrollada por Cloudera.
Hoy en día es un proyecto perteneciente al ASF (Apache Software Foundation)
Es 100% open source.
Todavía está en proceso de incubación.
*/

-- 3.2. ¿En qué se diferencia de Hive?
/*
Impala ejecuta las queriesdirectamente sobre el clusteren lugar de ejecutar MapReducepara procesar.
Es en torno a unas 5 veces más rápido que Hiveo Pig, aunque  a menudo puede ser hasta 20 veces más rápido.
Actualmente hay una serie de funcionalidades que Impala no soporta pero Hive sí
- Ficheros con tipos de datos a medida.
- El tipo de datos DATE.
- Funciones XML y JSON.
- Algunas funciones de agregación como: “covar_pop, covar_samp, corr, percentile, percentile_approx, histogram_numeric, collect_set”
- Sampling(que es el ejecutar queries sobre un subset de una tabla en lugar de sobre toda la tabla).
- Vistas laterales (sobre una columna de una tabla, fila a fila, se le aplica una función (que crea un resultado en forma de 
vista en ejecución) y sobre ese resultado se aplica otra función, que es lo que se muestra.
- Multilples cláusulas DISTINCT por query.
- UDFs(soportadas a partir de impala  1.2).

*/

--3.3. Comando INVALIDATE METADATA, ¿en qué consiste?
/*
Marca los metadatos de una o todas las tablas como obsoletos. 
Requerido después de que se crea una tabla a través del shell de Hive, 
antes de que la tabla esté disponible para consultas de Impala. 
La próxima vez que el nodo actual de Impala realiza una consulta en una tabla 
cuyos metadatos están invalidados, Impala vuelve a cargar los metadatos asociados 
antes de que continúe la consulta. 
Esta es una operación relativamente costosa en comparación con la actualización incremental 
de metadatos realizada por la instrucción REFRESH, por lo que en el escenario común de agregar 
nuevos archivos de datos a una tabla existente, prefiera REFRESH en lugar de INVALIDATE METADATA. 
*/

-- 3.4. Hacer invalidate metadata en Impala de la base de datos datos_padron.
use datos_padron;
INVALIDATE METADATA;

-- 3.5. Calcular el total de EspanolesHombres, espanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres 
-- agrupado por DESC_DISTRITO y DESC_BARRIO.

select sum(espanoleshombres) total_hombres_españoles, sum(espanolesmujeres) total_mujeres_españolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_txt_3
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- 3.6. Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2
-- (No deberían incluir espacios innecesarios). ¿Alguna conclusión?

-- el tiempo de respuesta es mucho mayor en hive que en impala, ademas de que hive no reconoce la ñ en un nombre de columna


-- 3.7. Llevar a cabo la misma consulta sobre las mismas tablas en Impala. ¿Algunaconclusión?
select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_txt_3
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_parquet_3
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- La velocidad de respuesta es mejor en impala para las dos consultas, por lo tanto se traduce como mejor rendimiento.

-- 4.3. Hacer invalidate metadata en Impala de la base de datos padron_particionado.

invalidate metadata datos_padron.padron_particionada;

-- 4.4.Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por 
-- DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.

select * from padron_particionada;

select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_particionada
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- 4.6. Llevar a cabo la consulta en Impala en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?
select sum(espanoleshombres) total_hombres_espanoles, sum(espanolesmujeres) total_mujeres_espanolas, sum(extranjeroshombres) total_hombres_extranjeros, sum(extranjerosmujeres) total_mujeres_extranjeras, desc_distrito distrito, desc_barrio barrio
from padron_parquet_2
where desc_distrito in ("CENTRO", "LATINA", "CHAMARTIN", "TETUAN", "VICALVARO", "BARAJAS")
group by desc_distrito, desc_barrio
order by desc_distrito, desc_barrio;

-- el tiempo de ejecucion no tiene comparacion con HIVE, siendo Impala mucho mas reapido para las dos consultas

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

-- tablas parquet y particionada mas rapidas que las txt, sin embargo tendria que hacer otras priuebas para saber
-- cual de estas ultimas es mas rapida.