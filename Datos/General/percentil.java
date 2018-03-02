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


//Export data bullet graph

val df1 = spark.read.json("/var/www/html/DashboardProc/web/Vicepr/bullet/bullet.json")
val df2 = spark.read.json("/var/www/html/DashboardProc/web/Jefes/bullet/bullet.json")
val df3 = spark.read.json("/var/www/html/DashboardProc/web/Nacionales/bullet/bullet.json") 
val df4 = spark.read.json("/var/www/html/DashboardProc/web/Internacionales/bullet/bullet.json")

val vicep = df1.select("valor").toDF.collect
val jefes = df2.select("valor").toDF.collect
val nales = df3.select("valor").toDF.collect
val inale = df4.select("valor").toDF.collect



//Almacenar en un solo Array el percentil de cada pregunta de la secciÃ³n 1 
val section1 = new ArrayBuffer[Int]()
section1 += vicep(0).toString.replace("[","").replace("]","").toInt
section1 += jefes(0).toString.replace("[","").replace("]","").toInt
section1 += nales(0).toString.replace("[","").replace("]","").toInt
section1 += inale(0).toString.replace("[","").replace("]","").toInt

val section2 = new ArrayBuffer[Int]()
section2 += vicep(1).toString.replace("[","").replace("]","").toInt
section2 += jefes(1).toString.replace("[","").replace("]","").toInt
section2 += nales(1).toString.replace("[","").replace("]","").toInt
section2 += inale(1).toString.replace("[","").replace("]","").toInt

val section3 = new ArrayBuffer[Int]()
section3 += vicep(2).toString.replace("[","").replace("]","").toInt
section3 += jefes(2).toString.replace("[","").replace("]","").toInt
section3 += nales(2).toString.replace("[","").replace("]","").toInt
section3 += inale(2).toString.replace("[","").replace("]","").toInt

val section4 = new ArrayBuffer[Int]()
section4 += vicep(3).toString.replace("[","").replace("]","").toInt
section4 += jefes(3).toString.replace("[","").replace("]","").toInt
section4 += nales(3).toString.replace("[","").replace("]","").toInt
section4 += inale(3).toString.replace("[","").replace("]","").toInt


//Convertir en un DataFrame el array que contiene los percentiles de cada pregunta de la seccion1
val df11 = section1.map(x => x.toInt).toDF("section1")

val df12 = section2.map(x => x.toInt).toDF("section2")

val df13 = section3.map(x => x.toInt).toDF("section3")

val df14 = section4.map(x => x.toInt).toDF("section4")



//Calcular el percentil de la seccion1 tomando los percentiles de cada una de las preguntas de la  misma seccion1
val PerSection1 = df11.stat.approxQuantile("section1",Array(0.75),0).toBuffer
//Calcular el percentil de la seccion2 tomando los percentiles de cada una de las preguntas de la  misma seccion2
val PerSection2 = df12.stat.approxQuantile("section2",Array(0.75),0).toBuffer
//Calcular el percentil de la seccion2 tomando los percentiles de cada una de las preguntas de la  misma seccion3
val PerSection3 = df13.stat.approxQuantile("section3",Array(0.75),0).toBuffer
//Calcular el percentil de la seccion2 tomando los percentiles de cada una de las preguntas de la  misma seccion2
val PerSection4 = df14.stat.approxQuantile("section4",Array(0.75),0).toBuffer


val name1 = ArrayBuffer[String]("Seccion1").toDF("seccion").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val name2 = ArrayBuffer[String]("Seccion2").toDF("seccion").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val name3 = ArrayBuffer[String]("Seccion3").toDF("seccion").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val name4 = ArrayBuffer[String]("Seccion4").toDF("seccion").withColumn("id", monotonicallyIncreasingId+1.toFloat)

val con = ArrayBuffer[Int](5).toDF("contC").withColumn("id", monotonicallyIncreasingId+1.toFloat)


val dfPersection1 = PerSection1.map(x => x.toInt).toDF("valor").withColumn("id", monotonicallyIncreasingId+1.toFloat) 
val dfPersection2 = PerSection2.map(x => x.toInt).toDF("valor").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val dfPersection3 = PerSection3.map(x => x.toInt).toDF("valor").withColumn("id", monotonicallyIncreasingId+1.toFloat)
val dfPersection4 = PerSection4.map(x => x.toInt).toDF("valor").withColumn("id", monotonicallyIncreasingId+1.toFloat)

val dfPS1 = name1.join(dfPersection1,"id").join(con,"id")
val dfPS2 = name2.join(dfPersection2,"id").join(con,"id")
val dfPS3 = name3.join(dfPersection3,"id").join(con,"id")
val dfPS4 = name4.join(dfPersection4,"id").join(con,"id")


val dfPS = dfPS1.union(dfPS2).union(dfPS3).union(dfPS4)

//Export data bullet graph
dfPS.drop("id").coalesce(1).write.mode("overwrite").json("/var/www/html/DashboardProc/web/General/bullet/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/DashboardProc/web/General/bullet/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/DashboardProc/web/General/bullet/"+file), new Path("/var/www/html/DashboardProc/web/General/bullet/bullet.json"));
 


//Export data donuts graph

val df5  = spark.read.format("csv").option("header","false").load("/var/www/html/DashboardProc/web/Vicepr/donut/donut.json")
val df51 = df5.select("_c0").collect.toBuffer
val df52 = df51(0).mkString.toInt

val df6  = spark.read.format("csv").option("header","false").load("/var/www/html/DashboardProc/web/Jefes/donut/donut.json")
val df61 = df6.select("_c0").collect.toBuffer
val df62 = df61(0).mkString.toInt

val df7  = spark.read.format("csv").option("header","false").load("/var/www/html/DashboardProc/web/Nacionales/donut/donut.json")
val df71 = df7.select("_c0").collect.toBuffer
val df72 = df71(0).mkString.toInt

val df8  = spark.read.format("csv").option("header","false").load("/var/www/html/DashboardProc/web/Internacionales/donut/donut.json")
val df81 = df8.select("_c0").collect.toBuffer
val df82 = df81(0).mkString.toInt

val df9 = df52+df62+df72+df82
val df91 = new ArrayBuffer[Int]()
df91 += df9

val dfK = df91.toDF("Colaboradores").withColumn("total",lit(140))
dfK.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "FALSE").save("/var/www/html/DashboardProc/web/General/donut/")
val file = fs.globStatus(new Path("/var/www/html/DashboardProc/web/General/donut/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/DashboardProc/web/General/donut/"+file), new Path("/var/www/html/DashboardProc/web/General/donut/donut.json"));

System.exit(0)

