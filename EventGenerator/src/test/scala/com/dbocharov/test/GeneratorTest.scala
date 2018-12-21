package com.dbocharov.test

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ServerSocket, Socket}

import com.dbocharov.generator.Generator
import org.scalatest.FunSuite

class GeneratorTest extends FunSuite {

  private val num = 100
  private val host = "localhost"

  private var port = 1000
  private var server: ServerSocket = null
  private var socket: Socket = null

  test("Generator testing") {
    //Init server socket, maybe, should be implemented in "before" method, but it's not working
    while (server == null) {
      try {
        server = new ServerSocket(port)
      } catch {
        case _: Throwable => {
          port = port + 1
        }
      }
    }

    //Run generator and count lines
    try {
      Generator.generate(num, host, port)
      socket = server.accept()
      val br = new BufferedReader(new InputStreamReader(socket.getInputStream))
      assert(num == br.lines().count()
      )
    }
    finally {
      try {
        if (socket != null) socket.close()
        if (server != null) server.close()
      }
      catch {
        case _: Throwable => {
          println("Error close socket")
        }
      }
    }
  }


}
