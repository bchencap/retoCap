package Recogida

import Recogida.Common.TransformacionesLocal
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]): Unit = {

    println("Executing main")
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val local= currentDirectory.startsWith("C:")
    //Source.fromFile("main.scala").
    var date="";
    var argumentFound = false;
    for(arg<-args)
    {
      if(argumentFound){
        date=arg;
        argumentFound=false;
      }

      if(arg=="-d") {
        argumentFound=true;
      }

    }

    if (local==false){
      println("server")
      //llamar a función dentro de Scala (databricks)
    }
    else {
      //llamar a función local
      println("local")
      TransformacionesLocal.RecogidaInicial(date)
    }
  }



}
