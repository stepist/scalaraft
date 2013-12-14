
import java.io.File
import sbt._
import Keys._
import File.pathSeparator
import scala.reflect.io.File


object MyBuild extends Build {



  lazy val myProject = Project(
    id = "scalaraft",
    base = file("."),
    settings = Defaults.defaultSettings ++ Seq(
        mainRunNobootcpSetting,
        testRunNobootcpSetting
    )
  )

   /*
  override lazy val settings = super.settings ++
    Seq(mainRunNobootcpSetting, testRunNobootcpSetting)    */

  val runNobootcp =
    InputKey[Unit]("run-nobootcp", "Runs main classes without Scala library on the boot classpath")

  val mainRunNobootcpSetting = runNobootcp <<= runNobootcpInputTask(Runtime)
  val testRunNobootcpSetting = runNobootcp <<= runNobootcpInputTask(Test)



  def runNobootcpInputTask(configuration: Configuration) = inputTask {
    (argTask: TaskKey[Seq[String]]) => (argTask, streams, fullClasspath in configuration) map { (at, st, cp) =>

      val runCp = cp.map(_.data).mkString(pathSeparator)
      val runOpts = Seq("-classpath", runCp) ++ at

      val result = Fork.java.fork(None, runOpts, None, Map(), false, LoggedOutput(st.log)).exitValue()
      if (result != 0) error("Run failed")
    }
  }
}
