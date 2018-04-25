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

//Export data bullet graph
dfPS.drop("id").coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/General/bullet/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/General/bullet/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/General/bullet/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/General/bullet/bullet.json"));
 

//Export data donuts graph

val dfK = k.toDF("Colaboradores").withColumn("total",lit(134))
dfK.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "FALSE").save("/var/www/html/AmericasBps/DashboardABps/web/General/donut/")
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/General/donut/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/General/donut/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/General/donut/donut.json"));


System.exit(0) 
