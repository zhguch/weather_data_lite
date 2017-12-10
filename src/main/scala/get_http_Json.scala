package main.scala

import javax.net.ssl._
import java.security.cert.X509Certificate


class url_list (val city_id: String) {
  
  val weather_APPID="c88263ccf9a4756f02ae549e73c7a33e"
  val IATA_api_key="ea86a3ed-082c-42f6-95bb-cb510ef72bf6"

  val url_weather = "http://api.openweathermap.org/data/2.5/weather?id=" + city_id +"&APPID=" + weather_APPID
  
  val url_IATA ="https://iatacodes.org/api/v6/cities?api_key=" + IATA_api_key

}

object Https_Request_Trust extends X509TrustManager {
  val getAcceptedIssuers = null

  def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) = {}

  def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
}

// Verifies host names
object Verifiy_HostNames extends HostnameVerifier {
  def verify(s: String, sslSession: SSLSession) = true
}


object get_IATA_json {
  // SSL Context initialization and configuration
  val ssl_Context = SSLContext.getInstance("SSL")
  ssl_Context.init(null,
                  Array(Https_Request_Trust),
                  new java.security.SecureRandom())
  HttpsURLConnection.setDefaultSSLSocketFactory(ssl_Context.getSocketFactory)
  HttpsURLConnection.setDefaultHostnameVerifier(Verifiy_HostNames)

  val url = new url_list("")
  val response = scala.io.Source.fromURL(url.url_IATA).mkString
  //println(response)
}


object get_weather_josn {

//generate accepted JSON format [{},{}]    
    val response_all = StringBuilder.newBuilder
    response_all.append("[")
    
    val city_list = main.get_city_id_list.city_id_list         
       
    for (city_id <- city_list) {
      val url = new url_list(city_id).url_weather 
      //println(url)
      val response = scala.io.Source.fromURL(url).mkString
      //println(response)

      if( city_id == city_list(0)  ){
         response_all.append(response)
      } 

      else {
         response_all.append(",")
         response_all.append(response)
      }
    }
    response_all.append("]")
    val result = response_all.mkString    
    //println(result)   
}

