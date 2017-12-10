package test.scala


import org.scalatest.Matchers
import org.scalatest.FunSuite
import main.scala.main.get_city_id_list
import main.scala.main.read_delta_weather_csv
import main.scala.main.read_target_weather_csv
import org.apache.spark._
import org.apache.spark.sql._
import java.io.File
import main.scala.Spark_Sql_List


class Input_Output_Test extends FunSuite with Matchers{
   val warehouseLocation = new File("spark-warehouse").getAbsolutePath
   val spark = SparkSession
  .builder()
  .appName("Weather_Data_Lite")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .master("local")
  .getOrCreate() 
  
   val df_city_list = main.scala.main.get_city_list.df_city_list 
       df_city_list.createOrReplaceTempView("CITY")
   val sql_df_city_list = spark.sql("SELECT * FROM CITY WHERE country='AU'")
   
   val df_IATA_Info_Content=main.scala.main.get_city_id_list.df_IATA_Info_Content
       df_IATA_Info_Content.createOrReplaceTempView("IATA_INFO")
      
   val sql_query = new Spark_Sql_List
   val sql_df_IATA_Info = spark.sql(sql_query.distinct_au_city)
   
   val target_weather_Info =read_target_weather_csv.target_weather_Info
       target_weather_Info.createOrReplaceTempView("TARGET_WEATHER_INFO")
   val sql_df_target_weather_Info = spark.sql("SELECT * FROM TARGET_WEATHER_INFO")
   
   val delta_weather_Info = read_delta_weather_csv.delta_weather_Info
       delta_weather_Info.createOrReplaceTempView("SOURCE_WEATHER_INFO_DELTA")
   val sql_df_delta_weather_Info = spark.sql("SELECT * FROM SOURCE_WEATHER_INFO_DELTA")
     
   val sql_df_target_weather_Info_final = spark.sql(sql_query.sql_df_upsert_target_weather)
   
   test("All City List") {
      df_city_list.count() should be(22635)
   }

   test("Au City List") {
      sql_df_city_list.count() should be(129)
   }
   
   test("Au City List With IATA Code") {
      sql_df_IATA_Info.count() should be(57)
   }   

   test("Weather Upsert Count") {
      sql_df_target_weather_Info.count()+sql_df_delta_weather_Info.count()  should be(sql_df_target_weather_Info_final.count())
   }   

   
    //spark.stop()
}



