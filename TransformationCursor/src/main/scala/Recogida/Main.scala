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
    for(i <- 0 until args.length by 2){
      if(args.apply(i)=="-d") {
        date=args.apply(i+1)
      }
    }
    if(date==""){
      println("Falta poner el parámetro de fecha que sigue de la siguiente forma : -d 2017-07-01")
      return;
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
