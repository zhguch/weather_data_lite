package main.scala

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.explode
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.unsafe.memory.MemoryBlock


import java.io.File
import java.util.Calendar
import java.text.SimpleDateFormat
import java.io.FileWriter



object main {
 
   //val warehouseLocation = "${system:user.dir}spark-warehouse"
   val warehouseLocation = new File("spark-warehouse").getAbsolutePath
   val spark_delta_temp_path = new File("spark_delta_temp").getAbsolutePath
   val spark_delta_path = new File("spark_delta").getAbsolutePath
   val spark_target_temp_path = new File("spark_target_temp").getAbsolutePath
   val spark_target_path = new File("spark_target").getAbsolutePath
   val spark_sources_path = new File("sources").getAbsolutePath
   val spark_log_path = new File("log").getAbsolutePath
   var target_file_name = "AU_Weather_Data_Lite.csv.gz"
   var target_file_name_path = spark_target_path+"/"+target_file_name
   var world_city_name = "current.city.list.json.gz"
   var world_city_name_path = spark_sources_path+"/"+world_city_name
   var sample_delta_file_name = "AU_Weather_Data_Lite_Delta_Sample.csv"
   var sample_delta_file_name_path = spark_sources_path+"/"+sample_delta_file_name

   var system_start_timestamp = get_current_timestamp.system_timestamp
   var delta_file_name = system_start_timestamp +"_AU_Weather_Data_Lite_Delta.csv.gz"   
   var spark_log_file ="weather_data_lite_sla.log"
   var spark_log_file_path = spark_log_path+"/"+spark_log_file
   
   val spark = SparkSession
  .builder()
  .appName("Weather_Data_Lite")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .master("local")
  .getOrCreate()

  
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._


  def main(args: Array[String]) {
  
  try {
      

  println(target_file_name_path+"\n"+spark_target_temp_path+"\n"+spark_target_path+"\n"+target_file_name+"\n")
 
  
  //get_JVM_memory_size
  get_city_list 
  get_city_id_list
  get_source_weather_Info
  verify_target_file (target_file_name_path)
  
  read_target_weather_csv
  
  ////////////////read_delta_weather_csv
  
  upsert_target_csv

  Write_to_sla_log
  spark.stop()

      
  } catch {
      case ioe: java.io.IOException =>  // handle this
      case ste: java.net.SocketTimeoutException => // handle this
  }
  
 }
   
 object get_current_timestamp {

  val format_Date = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
  val system_timestamp = format_Date.format(Calendar.getInstance().getTime)

 }
   
   
 object get_city_list {
      val df_city_list = spark.read.option("wholeFile", true)
                              .option("multiLine", true)
                              .option("mode", "PERMISSIVE")
                              .json(world_city_name_path)
                              .drop("request")                        
                              .withColumn("id", $"id".cast("Int").cast("string"))                         
                              .select("id","name","country")    
      //df_city_list.show()
                                           
      df_city_list.createOrReplaceTempView("CITY")
      //val sql_df_city_list = spark.sql("SELECT * FROM CITY WHERE country='AU'")
      //sql_df_city_list.show()       
 }
 
 
 object get_city_id_list {
      // get the IATA code from web  
      val rdd_IATA_Info = spark.sparkContext.parallelize(Seq(get_IATA_json.response))
      //rdd_IATA_Info.take(1).foreach(println) 
      
      val df_IATA_Info = spark.read.json(rdd_IATA_Info).drop("request")
       
      val df_IATA_Info_Content = df_IATA_Info.withColumn("response", explode($"response"))
                                             .select("response.code", "response.name","response.country_code")
      
      df_IATA_Info_Content.createOrReplaceTempView("IATA_INFO")
      
      val sql_query = new Spark_Sql_List
      val sql_df_IATA_Info = spark.sql(sql_query.distinct_au_city)
      
      val city_id_list = sql_df_IATA_Info.select("id").map(r => r.getString(0)).collect.toList
      
      //for (city_id <- city_id_list) println(city_id)
   
 }

object get_source_weather_Info {
      // get the weatherfrom web  
      
      val rdd_weather_Info = spark.sparkContext.parallelize(Seq(get_weather_josn.result))
           
      val df_weather_Info = spark.read.json(rdd_weather_Info)
                                 .drop("clouds","sys","cod","base")

      //df_weather_Info.show()
      val df_weather_Info_Content = df_weather_Info.withColumn("weather", explode($"weather"))
                                                   .withColumn("coord", concat_ws(",", $"coord.lat".cast("string"), $"coord.lon".cast("string")))
                                                   .withColumn("celsius_temp", bround($"main.temp"-273.15,2))
                                                   .withColumn("celsius_temp_min", bround($"main.temp_min"-273.15,2))
                                                   .withColumn("celsius_temp_max", bround($"main.temp_max"-273.15,2))
                                                   .withColumn("wind_deg", bround($"wind.deg",2))
                                                   .select("id", "coord","name", "weather.main","weather.description",  "celsius_temp", "celsius_temp_min", "celsius_temp_max", "main.pressure", "main.humidity","wind.speed","wind_deg","dt" )
      
      val sql_query = new Spark_Sql_List
      //df_weather_Info_Content.show()
    
      val columnsRenamed = sql_query.column_list.split(",").toSeq
         
      //df_weather_Info_Content.toDF(columnsRenamed: _*).show()
      df_weather_Info_Content.toDF(columnsRenamed: _*).createOrReplaceTempView("SOURCE_WEATHER_INFO")
            
      
      val sql_df_weather_Info = spark.sql(sql_query.sql_df_source_weather)
      val delta_input_count=sql_df_weather_Info.count()
      sql_df_weather_Info.createOrReplaceTempView("SOURCE_WEATHER_INFO_DELTA")
      sql_df_weather_Info.repartition(1)
                         .write      
                         .option("delimiter", "|")
                         .option("header", true)
                         .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                         .mode("overwrite")
                         .csv(spark_delta_temp_path)
     
      
      write_target_csv(spark_delta_temp_path,
                       spark_delta_path, 
                       delta_file_name )
} 


def write_target_csv (input_path: String, 
                      output_path: String,
                      output_file_name: String )  {
      val hadoop_fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val Spark_Output_file = hadoop_fs.globStatus(new Path(input_path+"/part*"))(0)
                   .getPath()
                   .getName()
                   
      //println(Spark_Output_file)
      verify_path(output_path)           
      hadoop_fs.copyToLocalFile(new Path(input_path +"/" + Spark_Output_file), new Path(output_path + "/"+ output_file_name ))
}


object read_target_weather_csv {

      val target_weather_Info = spark.read
                              .option("wholeFile", true)
                              .option("delimiter", "|")
                              .option("header", true)
                              .option("nullValue", "null")
                              .csv(target_file_name_path)
                              
      target_weather_Info.createOrReplaceTempView("TARGET_WEATHER_INFO")

      var target_row_count=target_weather_Info.count()                                          
      //val sql_df_target_weather_Info = spark.sql("SELECT * FROM TARGET_WEATHER_INFO")
      //sql_df_target_weather_Info.show()        
  
}

//TEST
object read_delta_weather_csv {

      val delta_weather_Info = spark.read
                              .option("wholeFile", true)
                              .option("delimiter", "|")
                              .option("header", true)
                              .option("nullValue", "null")
                              .csv(sample_delta_file_name_path)
                              
      delta_weather_Info.createOrReplaceTempView("SOURCE_WEATHER_INFO_DELTA")

      //target_weather_Info.show()                                          
      //val sql_df_delta_weather_Info = spark.sql("SELECT * FROM SOURCE_WEATHER_INFO_DELTA")
      //sql_df_target_weather_Info.show()        
  
}
//TEST



object upsert_target_csv {
      val sql_query = new Spark_Sql_List
      val sql_df_target_weather_Info = spark.sql(sql_query.sql_df_upsert_target_weather)

      val updated_target_row_count=sql_df_target_weather_Info.count()
      sql_df_target_weather_Info.repartition(1).write
                         .option("delimiter", "|")
                         .option("header", true)
                         .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                         .mode("overwrite")
                         .csv(spark_target_temp_path)

      write_target_csv(spark_target_temp_path,
                       spark_target_path, 
                       target_file_name)
  
}

def verify_path (path_name: String) {
  var path = new File(path_name);

  if (path.exists()) {
    println("Required directory path exists \n")
  }
  else
  {
    path.mkdir();
    println("Create directory path \n")
  }
  
}


def verify_target_file (path_name: String) {
  var path = new File(path_name);

  if (path.exists()) {
    println("Target file "+path_name+" exists \n")
  }
  else
  {
    println("Target file "+path_name+" does not exist \n")
    println("Save the delta file and target file\n")
      write_target_csv(spark_delta_temp_path,
                       spark_target_path, 
                       target_file_name)
    Write_to_sla_log
    spark.stop()
    System.exit(0)
  }
  
}


def get_JVM_memory_size {
  
      var JVM_Heap_Size = Runtime.getRuntime().totalMemory();

        //Print the jvm heap size.
        System.out.println("JVM Heap Size = " + JVM_Heap_Size);
}


object Write_to_sla_log {
  
    val writer_log = new FileWriter(spark_log_file_path,true)
    var system_end_timestamp = get_current_timestamp.system_timestamp  
    writer_log.append(system_start_timestamp +"|"
                    + system_end_timestamp+"|"
                    + delta_file_name +"|"
                    + get_source_weather_Info.delta_input_count+"|"
                    + read_target_weather_csv.target_row_count+"|"
                    + upsert_target_csv.updated_target_row_count+"|"
                    +"FIN\n")
    writer_log.close()

  }

}

