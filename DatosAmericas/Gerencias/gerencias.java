//Importar librerÃƒÆ’Ã‚Â­as
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



//Leer datos y ajustar para su procesamiento
case class Obs1(  q1: String, q2: String, 
	q3: String, q4: String, q5: String, q6: String, q7: String, q8: String, q9: String, q10: String, q11: String, q12: String, q13: String, q14: String, q15: String, q16: String)

case class Obs2( q1: String, q2: String, 
	q3: String, q4: String, q5: String, q6: String)
 
case class Obs3( q1: String, q2: String, 
	q3: String, q4: String, q5: String, q6: String, q7: String, q8: String, q9: String, q10: String, q11: String, q12: String)
 

case class Obs5( k1: String, k2: String)


def parseObs1(line: Array[String]): Obs1 = {
    Obs1( line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14), line(15)  )     }

def parseObs2(line: Array[String]): Obs2 = {
    Obs2( line(0), line(1), line(2), line(3), line(4), line(5) ) }
    
def parseObs3(line: Array[String]): Obs3 = {
    Obs3( line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11) ) }

def parseObs5(line: Array[String]): Obs5 = {
    Obs5( line(0), line(1) ) }

def parseRDD1(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(_.split(",")).map(_.drop(0)).map(_.map(_.toString))}

def parseRDD2(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(_.split(",")).map(_.drop(0)).map(_.map(_.toString))}	
    
def parseRDD3(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(_.split(",")).map(_.drop(0)).map(_.map(_.toString))}
    
def parseRDD5(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(_.split(",")).map(_.drop(0)).map(_.map(_.toString))}
 
val rdd1 = sc.textFile("/home/centos/DatosAmericas/Gerencias/seccion1.txt")
val rdd2 = sc.textFile("/home/centos/DatosAmericas/Gerencias/seccion2.txt")
val rdd3 = sc.textFile("/home/centos/DatosAmericas/Gerencias/seccion3.txt")
val rdd5 = sc.textFile("/home/centos/DatosAmericas/Gerencias/otro.txt")


val Obs1RDD = parseRDD1(rdd1).map(parseObs1)
val Obs2RDD = parseRDD2(rdd2).map(parseObs2)
val Obs3RDD = parseRDD3(rdd3).map(parseObs3)
val Obs5RDD = parseRDD5(rdd5).map(parseObs5)


val df10 = Obs1RDD.toDF().cache()
val df220 = Obs2RDD.toDF().cache()
val df310 = Obs3RDD.toDF().cache()
val df50 = Obs5RDD.toDF().cache()
val df5 = df50.withColumn("k1",'k1.cast("Int"))


df10.registerTempTable("obs1")
df220.registerTempTable("obs2")
df310.registerTempTable("obs3")
df5.registerTempTable("obs5")

val df11 =  df10.join(df5).where(df10("q1")===df5("k2")).drop("q1","k2").withColumnRenamed("k1","A1")
val df12 =  df11.join(df5).where(df11("q2")===df5("k2")).drop("q2","k2").withColumnRenamed("k1","B2")
val df13 =  df12.join(df5).where(df12("q3")===df5("k2")).drop("q3","k2").withColumnRenamed("k1","C3")
val df14 =  df13.join(df5).where(df13("q4")===df5("k2")).drop("q4","k2").withColumnRenamed("k1","D4")
val df15 =  df14.join(df5).where(df14("q5")===df5("k2")).drop("q5","k2").withColumnRenamed("k1","E5")
val df16 =  df15.join(df5).where(df15("q6")===df5("k2")).drop("q6","k2").withColumnRenamed("k1","F6")
val df17 =  df16.join(df5).where(df16("q7")===df5("k2")).drop("q7","k2").withColumnRenamed("k1","G7")
val df18 =  df17.join(df5).where(df17("q8")===df5("k2")).drop("q8","k2").withColumnRenamed("k1","H8")
val df19 =  df18.join(df5).where(df18("q9")===df5("k2")).drop("q9","k2").withColumnRenamed("k1","I9")
val df20 =  df19.join(df5).where(df19("q10")===df5("k2")).drop("q10","k2").withColumnRenamed("k1","J10")
val df21 =  df20.join(df5).where(df20("q11")===df5("k2")).drop("q11","k2").withColumnRenamed("k1","K11")
val df22 =  df21.join(df5).where(df21("q12")===df5("k2")).drop("q12","k2").withColumnRenamed("k1","L12")
val df23 =  df22.join(df5).where(df22("q13")===df5("k2")).drop("q13","k2").withColumnRenamed("k1","M13")
val df24 =  df23.join(df5).where(df23("q14")===df5("k2")).drop("q14","k2").withColumnRenamed("k1","N14")
val df25 =  df24.join(df5).where(df24("q15")===df5("k2")).drop("q15","k2").withColumnRenamed("k1","O15")
val df26 =  df25.join(df5).where(df25("q16")===df5("k2")).drop("q16","k2").withColumnRenamed("k1","P16")


val df221 =  df220.join(df5).where(df220("q1")===df5("k2")).drop("q1","k2").withColumnRenamed("k1","A1")
val df222 =  df221.join(df5).where(df221("q2")===df5("k2")).drop("q2","k2").withColumnRenamed("k1","B2")
val df223 =  df222.join(df5).where(df222("q3")===df5("k2")).drop("q3","k2").withColumnRenamed("k1","C3")
val df224 =  df223.join(df5).where(df223("q4")===df5("k2")).drop("q4","k2").withColumnRenamed("k1","D4")
val df225 =  df224.join(df5).where(df224("q5")===df5("k2")).drop("q5","k2").withColumnRenamed("k1","E5")
val df226 =  df225.join(df5).where(df225("q6")===df5("k2")).drop("q6","k2").withColumnRenamed("k1","F6")


val df311 =  df310.join(df5).where(df310("q1")===df5("k2")).drop("q1","k2").withColumnRenamed("k1","A1")
val df312 =  df311.join(df5).where(df311("q2")===df5("k2")).drop("q2","k2").withColumnRenamed("k1","B2")
val df313 =  df312.join(df5).where(df312("q3")===df5("k2")).drop("q3","k2").withColumnRenamed("k1","C3")
val df314 =  df313.join(df5).where(df313("q4")===df5("k2")).drop("q4","k2").withColumnRenamed("k1","D4")
val df315 =  df314.join(df5).where(df314("q5")===df5("k2")).drop("q5","k2").withColumnRenamed("k1","E5")
val df316 =  df315.join(df5).where(df315("q6")===df5("k2")).drop("q6","k2").withColumnRenamed("k1","F6")
val df317 =  df316.join(df5).where(df316("q7")===df5("k2")).drop("q7","k2").withColumnRenamed("k1","G7")
val df318 =  df317.join(df5).where(df317("q8")===df5("k2")).drop("q8","k2").withColumnRenamed("k1","H8")
val df319 =  df318.join(df5).where(df318("q9")===df5("k2")).drop("q9","k2").withColumnRenamed("k1","I9")
val df320 =  df319.join(df5).where(df319("q10")===df5("k2")).drop("q10","k2").withColumnRenamed("k1","J10")
val df321 =  df320.join(df5).where(df320("q11")===df5("k2")).drop("q11","k2").withColumnRenamed("k1","K11")
val df322 =  df321.join(df5).where(df321("q12")===df5("k2")).drop("q12","k2").withColumnRenamed("k1","L12")


val df1 = df26
val df2 = df226
val df3 = df322


df1.coalesce(1).write.mode("overwrite").json("/home/centos/DatosAmericas/Gerencias/Section1/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/home/centos/DatosAmericas/Gerencias/Section1/part*"))(0).getPath().getName();
fs.rename(new Path("/home/centos/DatosAmericas/Gerencias/Section1/"+file), new Path("/home/centos/DatosAmericas/Gerencias/Section1/seccion1.json"));

df2.coalesce(1).write.mode("overwrite").json("/home/centos/DatosAmericas/Gerencias/Section2/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/home/centos/DatosAmericas/Gerencias/Section2/part*"))(0).getPath().getName();
fs.rename(new Path("/home/centos/DatosAmericas/Gerencias/Section2/"+file), new Path("/home/centos/DatosAmericas/Gerencias/Section2/seccion2.json"));

df3.coalesce(1).write.mode("overwrite").json("/home/centos/DatosAmericas/Gerencias/Section3/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/home/centos/DatosAmericas/Gerencias/Section3/part*"))(0).getPath().getName();
fs.rename(new Path("/home/centos/DatosAmericas/Gerencias/Section3/"+file), new Path("/home/centos/DatosAmericas/Gerencias/Section3/seccion3.json"));


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


//Almacenar en un solo Array el percentil de cada pregunta de la secciÃ³n 1 
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


//Almacenar en un solo Array el percentil de cada pregunta de la secciÃ³n 2 
val distance2 = new ArrayBuffer[Double]()
distance2 += per21(0)
distance2 += per22(0)
distance2 += per23(0)
distance2 += per24(0)
distance2 += per25(0)
distance2 += per26(0)

//Almacenar en un solo Array el percentil de cada pregunta de la secciÃ³n 3
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
dfPS.drop("id").coalesce(1).write.mode("overwrite").json("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/")
val fs = FileSystem.get(sc.hadoopConfiguration);
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/bullet/bullet.json"));
 

//Export data donuts graph

val dfK = k.toDF("Colaboradores").withColumn("total",lit(6))
dfK.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "FALSE").save("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/donut/")
val file = fs.globStatus(new Path("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/donut/part*"))(0).getPath().getName();
fs.rename(new Path("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/donut/"+file), new Path("/var/www/html/AmericasBps/DashboardABps/web/Gerencias/donut/donut.json"));


//
System.exit(0) 

