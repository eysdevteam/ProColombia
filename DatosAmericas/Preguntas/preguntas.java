//Importar librerÃƒÂ­as
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext,Row}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{DenseVector,Vector}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.feature.{Normalizer,StandardScaler}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.PCA
import java.util.Calendar
import org.apache.hadoop.fs._;



val df11 = spark.read.json("/home/centos/DatosAmericas/Analistas/Section1/seccion1.json")
val df12 = spark.read.json("/home/centos/DatosAmericas/Analistas/Section2/seccion2.json")
val df13 = spark.read.json("/home/centos/DatosAmericas/Analistas/Section3/seccion3.json")


val df21 = spark.read.json("/home/centos/DatosAmericas/Jefes/Section1/seccion1.json")
val df22 = spark.read.json("/home/centos/DatosAmericas/Jefes/Section2/seccion2.json")
val df23 = spark.read.json("/home/centos/DatosAmericas/Jefes/Section3/seccion3.json")


val df31 = spark.read.json("/home/centos/DatosAmericas/Directores/Section1/seccion1.json")
val df32 = spark.read.json("/home/centos/DatosAmericas/Directores/Section2/seccion2.json")
val df33 = spark.read.json("/home/centos/DatosAmericas/Directores/Section3/seccion3.json")


val df41 = spark.read.json("/home/centos/DatosAmericas/Gerencias/Section1/seccion1.json")
val df42 = spark.read.json("/home/centos/DatosAmericas/Gerencias/Section2/seccion2.json")
val df43 = spark.read.json("/home/centos/DatosAmericas/Gerencias/Section3/seccion3.json")

val df1 = df11.union(df21).union(df31).union(df41)
val df2 = df12.union(df22).union(df32).union(df42)
val df3 = df13.union(df23).union(df33).union(df43)



//Calcular el percentil de la seccion1 por cada una de de las preguntas
val per11  = df1.stat.approxQuantile("A1",Array(0.60),0)
val per12  = df1.stat.approxQuantile("B2",Array(0.60),0)
val per13  = df1.stat.approxQuantile("C3",Array(0.60),0)
val per14  = df1.stat.approxQuantile("D4",Array(0.60),0)
val per15  = df1.stat.approxQuantile("E5",Array(0.60),0)
val per16  = df1.stat.approxQuantile("F6",Array(0.60),0)
val per17  = df1.stat.approxQuantile("G7",Array(0.60),0)
val per18  = df1.stat.approxQuantile("H8",Array(0.60),0)
val per19  = df1.stat.approxQuantile("I9",Array(0.60),0)
val per110 = df1.stat.approxQuantile("J10",Array(0.60),0)
val per111 = df1.stat.approxQuantile("K11",Array(0.60),0)
val per112 = df1.stat.approxQuantile("L12",Array(0.60),0)
val per113 = df1.stat.approxQuantile("M13",Array(0.60),0)
val per114 = df1.stat.approxQuantile("N14",Array(0.60),0)
val per115 = df1.stat.approxQuantile("O15",Array(0.60),0)
val per116 = df1.stat.approxQuantile("P16",Array(0.60),0)

//Calcular el percentil de la seccion2 por cada una de de las preguntas
val per21  = df2.stat.approxQuantile("A1",Array(0.60),0)
val per22  = df2.stat.approxQuantile("B2",Array(0.60),0)
val per23  = df2.stat.approxQuantile("C3",Array(0.60),0)
val per24  = df2.stat.approxQuantile("D4",Array(0.60),0)
val per25  = df2.stat.approxQuantile("E5",Array(0.60),0)
val per26  = df2.stat.approxQuantile("F6",Array(0.60),0)

//Calcular el percentil de la seccion3 por cada una de de las preguntas
val per31  = df3.stat.approxQuantile("A1",Array(0.60),0)
val per32  = df3.stat.approxQuantile("B2",Array(0.60),0)
val per33  = df3.stat.approxQuantile("C3",Array(0.60),0)
val per34  = df3.stat.approxQuantile("D4",Array(0.60),0)
val per35  = df3.stat.approxQuantile("E5",Array(0.60),0)
val per36  = df3.stat.approxQuantile("F6",Array(0.60),0)
val per37  = df3.stat.approxQuantile("G7",Array(0.60),0)
val per38  = df3.stat.approxQuantile("H8",Array(0.60),0)
val per39  = df3.stat.approxQuantile("I9",Array(0.60),0)
val per310  = df3.stat.approxQuantile("J10",Array(0.60),0)
val per311  = df3.stat.approxQuantile("K11",Array(0.60),0)
val per312  = df3.stat.approxQuantile("L12",Array(0.60),0)

//Almacenar en un solo Array el percentil de cada pregunta de la sección 1 
val distance1 = new ArrayBuffer[Double]()
distance1 += per11(0)
distance1 += per12(0)
distance1 += per13(0)
distance1 += per14(0)
distance1 += per15(0)
distance1 += per16(0)
distance1 += per17(0)
distance1 += per18(0)
distance1 += per19(0)
distance1 += per110(0)
distance1 += per111(0)
distance1 += per112(0)
distance1 += per113(0)
distance1 += per114(0)
distance1 += per115(0)
distance1 += per116(0)


//Almacenar en un solo Array el percentil de cada pregunta de la sección 2 
val distance2 = new ArrayBuffer[Double]()
distance2 += per21(0)
distance2 += per22(0)
distance2 += per23(0)
distance2 += per24(0)
distance2 += per25(0)
distance2 += per26(0)

//Almacenar en un solo Array el percentil de cada pregunta de la sección 3
val distance3 = new ArrayBuffer[Double]()
distance3 += per31(0)
distance3 += per32(0)
distance3 += per33(0)
distance3 += per34(0)
distance3 += per35(0)
distance3 += per36(0)
distance3 += per37(0)
distance3 += per38(0)
distance3 += per39(0)
distance3 += per310(0)
distance3 += per311(0)
distance3 += per312(0)




//Convertir en un DataFrame el array que contiene los percentiles de cada pregunta de la seccion1
val df11 = distance1.map(x => x.toDouble).toDF("section1")
//Convertir en un DataFrame el array que contiene los percentiles de cada pregunta de la seccion2
val df22 = distance2.map(x => x.toDouble).toDF("section2")
//Convertir en un DataFrame el array que contiene los percentiles de cada pregunta de la seccion3
val df33 = distance3.map(x => x.toDouble).toDF("section3")



//Calcular el percentil de la seccion1 tomando los percentiles de cada una de las preguntas de la  misma seccion1
val PerSection1 = df11.stat.approxQuantile("section1",Array(0.60),0).toBuffer
//Calcular el percentil de la seccion2 tomando los percentiles de cada una de las preguntas de la  misma seccion2
val PerSection2 = df22.stat.approxQuantile("section2",Array(0.60),0).toBuffer
//Calcular el percentil de la seccion2 tomando los percentiles de cada una de las preguntas de la  misma seccion3
val PerSection3 = df33.stat.approxQuantile("section3",Array(0.60),0).toBuffer


val name1 = ArrayBuffer[String]("Seccion1").toDF("seccion").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val name2 = ArrayBuffer[String]("Seccion2").toDF("seccion").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val name3 = ArrayBuffer[String]("Seccion3").toDF("seccion").withColumn("id", monotonicallyIncreasingId+1.toFloat)

val con = ArrayBuffer[Int](5).toDF("contC").withColumn("id", monotonicallyIncreasingId+1.toFloat)

val dfPersection1 = PerSection1.map(x => x.toInt).toDF("valor").withColumn("id", monotonicallyIncreasingId+1.toFloat) 
val dfPersection2 = PerSection2.map(x => x.toInt).toDF("valor").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val dfPersection3 = PerSection3.map(x => x.toInt).toDF("valor").withColumn("id", monotonicallyIncreasingId+1.toFloat)

val dfPS1 = name1.join(dfPersection1,"id").join(con,"id")
val dfPS2 = name2.join(dfPersection2,"id").join(con,"id")
val dfPS3 = name3.join(dfPersection3,"id").join(con,"id")


val dfPS = dfPS1.union(dfPS2).union(dfPS3)

val k = new ArrayBuffer[Int]()
//Numero de personas que han diligenciado
	if (df1.count == df2.count)
	{
		k += df1.count.toInt 		
	}
//seccion 1
val dfd1 = distance1.toDF.withColumn("id", monotonicallyIncreasingId+1.toInt)
//seccion 2
val dfd2 = distance2.toDF.withColumn("id", monotonicallyIncreasingId+1.toInt)
//seccion 3
val dfd3 = distance3.toDF.withColumn("id", monotonicallyIncreasingId+1.toInt)


//nombres preguntas x cada seccion
val dfname1 = spark.read.format("csv").option("encoding","ISO-8859-1").load("/home/centos/DatosAmericas/Preguntas/names1.txt").withColumnRenamed("_c0","name")
val dfname11 = dfname1.withColumn("id", monotonicallyIncreasingId+1.toInt)


val dfname2 = spark.read.format("csv").option("encoding","ISO-8859-1").load("/home/centos/DatosAmericas/Preguntas/names2.txt").withColumnRenamed("_c0","name")
val dfname12 = dfname2.withColumn("id", monotonicallyIncreasingId+1.toInt)


val dfname3 = spark.read.format("csv").option("encoding","ISO-8859-1").load("/home/centos/DatosAmericas/Preguntas/names3.txt").withColumnRenamed("_c0","name")
val dfname13 = dfname1.withColumn("id", monotonicallyIncreasingId+1.toInt)


//Definir estrutuctura de datos x secci�n y por pregunta (ejm 15 seccion 1 pregunta 5)
val dfcat11 = dfd1.filter($"value"===1).join(dfname11,"id").drop("id")
val dfcat12 = dfd1.filter($"value"===2).join(dfname11,"id").drop("id")
val dfcat13 = dfd1.filter($"value"===3).join(dfname11,"id").drop("id")
val dfcat14 = dfd1.filter($"value"===4).join(dfname11,"id").drop("id")
val dfcat15 = dfd1.filter($"value"===5).join(dfname11,"id").drop("id")

val dfcat21 = dfd2.filter($"value"===1).join(dfname12,"id").drop("id")
val dfcat22 = dfd2.filter($"value"===2).join(dfname12,"id").drop("id")
val dfcat23 = dfd2.filter($"value"===3).join(dfname12,"id").drop("id")
val dfcat24 = dfd2.filter($"value"===4).join(dfname12,"id").drop("id")
val dfcat25 = dfd2.filter($"value"===5).join(dfname12,"id").drop("id")

val dfcat31 = dfd3.filter($"value"===1).join(dfname13,"id").drop("id")
val dfcat32 = dfd3.filter($"value"===2).join(dfname13,"id").drop("id")
val dfcat33 = dfd3.filter($"value"===3).join(dfname13,"id").drop("id")
val dfcat34 = dfd3.filter($"value"===4).join(dfname13,"id").drop("id")
val dfcat35 = dfd3.filter($"value"===5).join(dfname13,"id").drop("id")

//Estructura si existen valores con esa categor�a
val dfes111 = dfcat11.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat11.count)).drop("value").withColumn("seccion",lit("Sección 1"))
val dfes112 = dfcat12.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat12.count)).drop("value").withColumn("seccion",lit("Sección 1"))
val dfes113 = dfcat13.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat13.count)).drop("value").withColumn("seccion",lit("Sección 1"))
val dfes114 = dfcat14.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat14.count)).drop("value").withColumn("seccion",lit("Sección 1"))
val dfes115 = dfcat15.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat15.count)).drop("value").withColumn("seccion",lit("Sección 1"))


val dfes121 = dfcat21.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat21.count)).drop("value").withColumn("seccion",lit("Sección 2"))
val dfes122 = dfcat22.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat22.count)).drop("value").withColumn("seccion",lit("Sección 2"))
val dfes123 = dfcat23.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat23.count)).drop("value").withColumn("seccion",lit("Sección 2"))
val dfes124 = dfcat24.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat24.count)).drop("value").withColumn("seccion",lit("Sección 2"))
val dfes125 = dfcat25.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat25.count)).drop("value").withColumn("seccion",lit("Sección 2"))

val dfes131 = dfcat31.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat31.count)).drop("value").withColumn("seccion",lit("Sección 3"))
val dfes132 = dfcat32.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat32.count)).drop("value").withColumn("seccion",lit("Sección 3"))
val dfes133 = dfcat33.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat33.count)).drop("value").withColumn("seccion",lit("Sección 3"))
val dfes134 = dfcat34.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat34.count)).drop("value").withColumn("seccion",lit("Sección 3"))
val dfes135 = dfcat35.groupBy("value").agg(collect_list($"name")).withColumn("valor",lit(dfcat35.count)).drop("value").withColumn("seccion",lit("Sección 3"))



//Estructura si no existen valores con esa categoria
val dfes211 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 1"))
val dfes212 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 1"))
val dfes213 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 1"))
val dfes214 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 1"))
val dfes215 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 1"))

val dfes221 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 2"))
val dfes222 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 2"))
val dfes223 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 2"))
val dfes224 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 2"))
val dfes225 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 2"))

val dfes231 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 3"))
val dfes232 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 3"))
val dfes233 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 3"))
val dfes234 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 3"))
val dfes235 = dfd1.where($"id"===1).withColumn("name",lit("")).agg(collect_list($"name")).withColumn("valor",lit(0)).withColumn("seccion",lit("Sección 3"))


//Generar estrucutyra final, verificando la condici�n
val dfes11 = if(dfd1.filter($"value"===1).count==0) dfes211 else dfes111
val dfes12 = if(dfd1.filter($"value"===2).count==0) dfes212 else dfes112
val dfes13 = if(dfd1.filter($"value"===3).count==0) dfes213 else dfes113
val dfes14 = if(dfd1.filter($"value"===4).count==0) dfes214 else dfes114
val dfes15 = if(dfd1.filter($"value"===5).count==0) dfes215 else dfes115


val dfes21 = if(dfd2.filter($"value"===1).count==0) dfes221 else dfes121
val dfes22 = if(dfd2.filter($"value"===2).count==0) dfes222 else dfes122
val dfes23 = if(dfd2.filter($"value"===3).count==0) dfes223 else dfes123
val dfes24 = if(dfd2.filter($"value"===4).count==0) dfes224 else dfes124
val dfes25 = if(dfd2.filter($"value"===5).count==0) dfes225 else dfes125

val dfes31 = if(dfd3.filter($"value"===1).count==0) dfes231 else dfes131
val dfes32 = if(dfd3.filter($"value"===2).count==0) dfes232 else dfes132
val dfes33 = if(dfd3.filter($"value"===3).count==0) dfes233 else dfes133
val dfes34 = if(dfd3.filter($"value"===4).count==0) dfes234 else dfes134
val dfes35 = if(dfd3.filter($"value"===5).count==0) dfes235 else dfes135




val dfes011 = dfes11.union(dfes12).union(dfes13).union(dfes14).union(dfes15)
val dfes012 = dfes21.union(dfes22).union(dfes23).union(dfes24).union(dfes25)
val dfes013 = dfes31.union(dfes32).union(dfes33).union(dfes34).union(dfes35)


val dfes021 = dfes011.drop("collect_list(name)").groupBy("seccion").agg(collect_list("valor")).withColumn("contC",lit(5))
val dfes022 = dfes012.drop("collect_list(name)").groupBy("seccion").agg(collect_list("valor")).withColumn("contC",lit(5))
val dfes023 = dfes013.drop("collect_list(name)").groupBy("seccion").agg(collect_list("valor")).withColumn("contC",lit(5))



//Export data bullet graph
dfes011.coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file1/data.json"));
 

dfes012.coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file1/data.json"));


dfes013.coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file1/data.json"));


dfes021.coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion1/file2/data.json"));

dfes022.coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion2/file2/data.json"));


dfes023.coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Preguntas/seccion3/file2/data.json"));

//
System.exit(0) 
