package com.samlaberge

object SystemConfig {

  // Network ports used by various components of system.
  val PORTS = new {

    // Port clients will use to communicate with scheduler.
    val SCHEDULER_CLIENT_SERVER = 5001

    // Port executors will use to communicate with server
    val SCHEDULER_EXECUTOR_SERVER = 5001

    // Port used to communicate with executor
    val EXECUTOR_SERVER_DEFAULT = 5003

    val CLIENT_PORT = 5004

    val FILE_LOOKUP_BASE = 8000

//    val FILE_SERVICE = 5001;
  }

  // 2 gigabytes
  val MAX_MESSAGE_SIZE: Int = Int.MaxValue
}
