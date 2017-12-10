package main.scala

import  org.apache.spark.sql.types._


class Spark_Sql_List {
 
 
  var column_list= "City_Id,Coordinates,City,Conditions,Conditions_Description,Temperature,Min_Temperature,Max_Temperature,Pressure,Humidity,Wind_Speed,Wind_Degrees,Source_Date_Time"
  
  var distinct_au_city ="""
                          SELECT distinct a.id, 
                                 a.name,
                                 i.code
                          FROM CITY a
                          INNER JOIN IATA_INFO i 
                          ON a.name = i.name
                          WHERE a.country='AU'
                          AND i.country_code='AU'   
                       """
   
 
  var sql_df_source_weather= """
                          SELECT i.code as Station, 
                                 w.Coordinates, 
                                 DATE_FORMAT(current_timestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'") as Create_Timestamp,
                                 w.City, 
                                 w.Conditions,
                                 w.Conditions_Description,
                                 CASE WHEN w.Temperature > 0 THEN CONCAT('+','',w.Temperature)
                                      WHEN w.Temperature < 0 THEN CONCAT('-','',w.Temperature)
                                      ELSE w.Temperature END as Temperature,
                                 CASE WHEN w.Min_Temperature > 0 THEN CONCAT('+','',w.Min_Temperature)
                                      WHEN w.Min_Temperature < 0 THEN CONCAT('-','',w.Min_Temperature)
                                      ELSE w.Min_Temperature END as Min_Temperature,
                                 CASE WHEN w.Max_Temperature > 0 THEN CONCAT('+','',w.Max_Temperature)
                                      WHEN w.Max_Temperature < 0 THEN CONCAT('-','',w.Max_Temperature)
                                      ELSE w.Max_Temperature END as Max_Temperature,
                                 w.Pressure,
                                 w.Humidity,
                                 w.Wind_Speed,
                                 w.Wind_Degrees,
                                 w.Source_Date_Time,
                                 w.City_Id,
                                 DATE_FORMAT("1900-01-01 00:00:00", "yyyy-MM-dd'T'HH:mm:ss'Z'") as Update_Timestamp,
                                 0 as Record_Delete_Flag
                          FROM SOURCE_WEATHER_INFO w 
                          INNER JOIN IATA_INFO i on w.City = i.name 
                          AND i.country_code='AU'
                       """    
  
  
  
 var sql_df_upsert_target_weather= """
                         SELECT 
                               t.Station, 
                               t.Coordinates, 
                               t.Create_Timestamp,
                               t.City, 
                               t.Conditions,
                               t.Conditions_Description,
                               t.Temperature,
                               t.Min_Temperature,
                               t.Max_Temperature,
                               t.Pressure,
                               t.Humidity,
                               t.Wind_Speed,
                               t.Wind_Degrees,
                               t.Source_Date_Time,
                               t.City_Id,
                               t.Update_Timestamp,
                               t.Record_Delete_Flag
                         FROM TARGET_WEATHER_INFO t
                         LEFT JOIN SOURCE_WEATHER_INFO_DELTA s 
                         ON  t.City_Id = s.City_Id 
                         AND  t.Source_Date_Time = s.Source_Date_Time
                         WHERE  S.City_Id is NULL or S.Source_Date_Time is NULL            
                         
                         UNION ALL                        
                         
                         SELECT 
                               s.Station, 
                               s.Coordinates, 
                               s.Create_Timestamp,
                               s.City, 
                               s.Conditions,
                               s.Conditions_Description,
                               s.Temperature,
                               s.Min_Temperature,
                               s.Max_Temperature,
                               s.Pressure,
                               s.Humidity,
                               s.Wind_Speed,
                               s.Wind_Degrees,
                               s.Source_Date_Time,
                               s.City_Id,
                               s.Update_Timestamp,
                               s.Record_Delete_Flag
                         FROM SOURCE_WEATHER_INFO_DELTA s
                         LEFT JOIN TARGET_WEATHER_INFO t
                         ON  t.City_Id = s.City_Id
                         AND  t.Source_Date_Time = s.Source_Date_Time
                         WHERE  t.City_Id is NULL or t.Source_Date_Time is NULL
                         
                         UNION ALL
                                               
                         SELECT
                               t.Station, 
                               t.Coordinates, 
                               t.Create_Timestamp,
                               t.City, 
                               t.Conditions,
                               t.Conditions_Description,
                               t.Temperature,
                               t.Min_Temperature,
                               t.Max_Temperature,
                               t.Pressure,
                               t.Humidity,
                               t.Wind_Speed,
                               t.Wind_Degrees,
                               t.Source_Date_Time,
                               t.City_Id,
                               t.Update_Timestamp,
                               t.Record_Delete_Flag
                         FROM TARGET_WEATHER_INFO t
                         INNER JOIN SOURCE_WEATHER_INFO_DELTA s                           
                         ON  t.City_Id = s.City_Id
                         AND t.Source_Date_Time = s.Source_Date_Time
                         WHERE t.Station = s.Station
                               AND
                               t.Coordinates = s.Coordinates
                               AND
                               t.City = s.City
                               AND
                               t.Conditions = s.Conditions
                               AND
                               t.Conditions_Description = s.Conditions_Description
                               AND
                               t.Temperature = s.Temperature
                               AND
                               t.Min_Temperature = s.Min_Temperature
                               AND
                               t.Max_Temperature = s.Max_Temperature
                               AND
                               t.Pressure = s.Pressure
                               AND
                               t.Humidity = s.Humidity
                               AND
                               t.Wind_Speed = s.Wind_Speed
                               AND
                               t.Wind_Degrees = s.Wind_Degrees

                         UNION ALL
                                               
                         SELECT
                               t.Station,
                               t.Coordinates,
                               t.Create_Timestamp,
                               t.City,
                               t.Conditions,
                               t.Conditions_Description,
                               t.Temperature,
                               t.Min_Temperature,
                               t.Max_Temperature,
                               t.Pressure,
                               t.Humidity,
                               t.Wind_Speed,
                               t.Wind_Degrees,
                               t.Source_Date_Time,
                               t.City_Id,
                               DATE_FORMAT(current_timestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'") as Update_Timestamp,
                               1 as Record_Delete_Flag
                         FROM TARGET_WEATHER_INFO t
                         INNER JOIN SOURCE_WEATHER_INFO_DELTA s                           
                         ON  t.City_Id = s.City_Id
                         AND t.Source_Date_Time = s.Source_Date_Time
                         WHERE t.Station <> s.Station
                               or
                               t.Coordinates <> s.Coordinates
                               or
                               t.City <> s.City
                               or
                               t.Conditions <> s.Conditions
                               or
                               t.Conditions_Description <> s.Conditions_Description
                               or
                               t.Temperature <> s.Temperature
                               or
                               t.Min_Temperature <> s.Min_Temperature
                               or
                               t.Max_Temperature <> s.Max_Temperature
                               or
                               t.Pressure <> s.Pressure
                               or
                               t.Humidity <> s.Humidity
                               or
                               t.Wind_Speed <> s.Wind_Speed
                               or
                               t.Wind_Degrees <> s.Wind_Degrees

                         UNION ALL                        
                         
                         SELECT 
                               s.Station, 
                               s.Coordinates, 
                               s.Create_Timestamp,
                               s.City, 
                               s.Conditions,
                               s.Conditions_Description,
                               s.Temperature,
                               s.Min_Temperature,
                               s.Max_Temperature,
                               s.Pressure,
                               s.Humidity,
                               s.Wind_Speed,
                               s.Wind_Degrees,
                               s.Source_Date_Time,
                               s.City_Id,
                               s.Update_Timestamp,
                               s.Record_Delete_Flag
                         FROM SOURCE_WEATHER_INFO_DELTA s
                         INNER JOIN TARGET_WEATHER_INFO t
                         ON  t.City_Id = s.City_Id
                         AND t.Source_Date_Time = s.Source_Date_Time
                         WHERE t.Station <> s.Station
                               or
                               t.Coordinates <> s.Coordinates
                               or
                               t.City <> s.City
                               or
                               t.Conditions <> s.Conditions
                               or
                               t.Conditions_Description <> s.Conditions_Description
                               or
                               t.Temperature <> s.Temperature
                               or
                               t.Min_Temperature <> s.Min_Temperature
                               or
                               t.Max_Temperature <> s.Max_Temperature
                               or
                               t.Pressure <> s.Pressure
                               or
                               t.Humidity <> s.Humidity
                               or
                               t.Wind_Speed <> s.Wind_Speed
                               or
                               t.Wind_Degrees <> s.Wind_Degrees                                                                                                                                                                                                                                                                                                                                                                                                                                              
                      """  


 var sql_df_upsert_target_weather_stat= """                         
                         SELECT COUNT(*) FROM(
                         SELECT 
                                t.*
                         FROM TARGET_WEATHER_INFO t
                         LEFT JOIN SOURCE_WEATHER_INFO_DELTA s 
                         ON  t.City_Id = s.City_Id 
                         AND  t.Source_Date_Time = s.Source_Date_Time
                         WHERE  S.City_Id is NULL or S.Source_Date_Time is NULL)                   
                         
                         UNION ALL                        
                         
                         SELECT COUNT(*) FROM(
                         SELECT 
                                s.*
                         FROM SOURCE_WEATHER_INFO_DELTA s
                         LEFT JOIN TARGET_WEATHER_INFO t
                         ON  t.City_Id = s.City_Id
                         AND  t.Source_Date_Time = s.Source_Date_Time
                         WHERE  t.City_Id is NULL or t.Source_Date_Time is NULL)

                         SELECT COUNT(*) FROM(
                         SELECT
                               t.*
                         FROM TARGET_WEATHER_INFO t
                         INNER JOIN SOURCE_WEATHER_INFO_DELTA s                           
                         ON  t.City_Id = s.City_Id
                         AND t.Source_Date_Time = s.Source_Date_Time
                         WHERE t.Station = s.Station
                               AND
                               t.Coordinates = s.Coordinates
                               AND
                               t.City = s.City
                               AND
                               t.Conditions = s.Conditions
                               AND
                               t.Conditions_Description = s.Conditions_Description
                               AND
                               t.Temperature = s.Temperature
                               AND
                               t.Min_Temperature = s.Min_Temperature
                               AND
                               t.Max_Temperature = s.Max_Temperature
                               AND
                               t.Pressure = s.Pressure
                               AND
                               t.Humidity = s.Humidity
                               AND
                               t.Wind_Speed = s.Wind_Speed
                               AND
                               t.Wind_Degrees = s.Wind_Degrees )                                             

                         UNION ALL
                         
                         SELECT COUNT(*) FROM(                         
                         SELECT
                                t.*
                         FROM TARGET_WEATHER_INFO t
                         INNER JOIN SOURCE_WEATHER_INFO_DELTA s                           
                         ON  t.City_Id = s.City_Id
                         AND t.Source_Date_Time = s.Source_Date_Time
                         WHERE t.Station <> s.Station
                               or
                               t.Coordinates <> s.Coordinates
                               or
                               t.City <> s.City
                               or
                               t.Conditions <> s.Conditions
                               or
                               t.Conditions_Description <> s.Conditions_Description
                               or
                               t.Temperature <> s.Temperature
                               or
                               t.Min_Temperature <> s.Min_Temperature
                               or
                               t.Max_Temperature <> s.Max_Temperature
                               or
                               t.Pressure <> s.Pressure
                               or
                               t.Humidity <> s.Humidity
                               or
                               t.Wind_Speed <> s.Wind_Speed
                               or
                               t.Wind_Degrees <> s.Wind_Degrees )

                         UNION ALL                        
                         
                         SELECT COUNT(*) FROM(
                         SELECT 
                                s.*
                         FROM SOURCE_WEATHER_INFO_DELTA s
                         INNER JOIN TARGET_WEATHER_INFO t
                         ON  t.City_Id = s.City_Id
                         AND t.Source_Date_Time = s.Source_Date_Time
                         WHERE t.Station <> s.Station
                               or
                               t.Coordinates <> s.Coordinates
                               or
                               t.City <> s.City
                               or
                               t.Conditions <> s.Conditions
                               or
                               t.Conditions_Description <> s.Conditions_Description
                               or
                               t.Temperature <> s.Temperature
                               or
                               t.Min_Temperature <> s.Min_Temperature
                               or
                               t.Max_Temperature <> s.Max_Temperature
                               or
                               t.Pressure <> s.Pressure
                               or
                               t.Humidity <> s.Humidity
                               or
                               t.Wind_Speed <> s.Wind_Speed
                               or
                               t.Wind_Degrees <> s.Wind_Degrees    )                                                                                                                                                                                                                                                                                                                                                                                                                                                
                      """   
 
 
}



