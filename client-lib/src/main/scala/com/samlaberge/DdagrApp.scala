package com.samlaberge

/**
 * This is a total hack to be able to have the Ddagr client
 * object get a reference to the outer, client app.
 * We need access to the outer class to be able to find the classes
 * directory to do class resolution.
 *
 * Ddagr applications need to extend this trait (like one does with App)
 * and this will pass the implicit argument to the Ddagr class.
 */
trait DdagrApp extends App {
  implicit val ddagrContext: DdagrApp = this
}
