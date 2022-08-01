package Recogida

import Recogida.Common.TransformacionesLocal

object Main {
  def main(args: Array[String]): Unit = {
    println("Executing main")
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val local= currentDirectory.startsWith("C:")
    //Source.fromFile("main.scala").

    for(arg<-args)
      {
        println(currentDirectory) ;
      }
    if (local==false){
      println("server")
      //llamar a función dentro de Scala (databricks)
      }
    else {
      //llamar a función local
      println("local")
      TransformacionesLocal.RecogidaInicial("2017-08-01")
    }
  }



}
