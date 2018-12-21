package com.dbocharov.generator

import java.io.PrintWriter
import java.net.Socket

import com.dbocharov.utils.Dictionary

import scala.util.Random

object Generator {
  private val default_num = 100000
  private val default_host = "localhost"
  private val default_port = 20000
  private val random = new Random(System.nanoTime())

  def generate(num: Int, host: String, port: Int) = {
    import com.dbocharov.utils.Helpers._

    var socket: Socket = null
    var pw: PrintWriter = null
    //Send events
    try {
      socket = new Socket(host, port)
      pw = new PrintWriter(socket.getOutputStream, true)
      for (_ <- 1 to num) {
        pw.println(random.randomEvent().generateCsv())
      }
    }

    catch {
      case e: Throwable => println(s"${Dictionary.ERROR_WRITE_SOCKET_MESSAGE}, cause:${e.getCause}")
    }

    //Close resources
    finally {
      try {
        pw.close()
      }
      catch {
        case e: Throwable => println(s"${Dictionary.ERROR_CLOSE_OUTPUT_STREAM_MESSAGE}, cause:${e.getCause}")
      }
      try {
        socket.close()
      }
      catch {
        case e: Throwable => println(s"${Dictionary.ERROR_CLOSE_SOCKET_MESSAGE}, cause:${e.getCause}")
      }
    }

  }

  //First arg should mean amount events, second - host, 3 - port
  def main(args: Array[String]): Unit = {
    val num = if (args.length > 0) try {
      Integer.parseInt(args(0))
    } catch {
      case e: Throwable => {
        println(s"${Dictionary.ERROR_PARSING_MESSAGE}, cause:${e.getCause}"); default_num
      }
    } else default_num
    val host = if (args.length > 1) args(1) else default_host
    val port = if (args.length > 2) try {
      Integer.parseInt(args(2))
    } catch {
      case e: Throwable => {
        println(s"${Dictionary.ERROR_PARSING_MESSAGE}, cause:${e.getCause}"); default_port
      }
    } else default_port
    generate(num, host, port)
  }
}
