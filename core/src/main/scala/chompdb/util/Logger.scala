package chompdb.util

trait Logger {
  def debug(message: String): Unit
  def info(message: String): Unit
  def warn(message: String): Unit
  def error(message: String, t: Throwable): Unit
}

class ConsoleLogger extends Logger {
  override def debug(message: String) {
    Console.println("[DEBUG] " + message)
  }

  override def info(message: String) {
    Console.println("[INFO] " + message)
  }

  override def warn(message: String) {
    Console.println("[WARN] " + message)
  }

  override def error(message: String, t: Throwable) {
    Console.println("[ERROR] " + message)
    t.printStackTrace()
  }
}