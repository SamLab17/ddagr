ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

/*
To make standalone executable jar, use
> sbt assembly
This creates the jar in the target/scala-X.Y.Z/ directory of the project
 */

// *****************************************************************************
// Projects
// *****************************************************************************

lazy val `scheduler` =
  project
    .in(file("scheduler"))
    .settings(settings)
    .dependsOn(`ddagr-protocol`)
    .settings(
      libraryDependencies ++= Seq(
        library.protocol,
        library.scalaTest  % Test
      )
    ).settings(
    assembly / mainClass := Some("com.samlaberge.SchedulerMain")
  )
/*
  .settings(
    Compile / mainClass := Some("com.samlaberge.HelloWorld")
  )*/

lazy val `executor` =
  project
    .in(file("executor"))
    .settings(settings)
    .dependsOn(`ddagr-protocol`)
    .settings(libraryDependencies ++= Seq(
      library.apacheCommonsLang,
      library.apacheXbeanAsm,
      library.commonsIO,
      library.scalaParallelCollections
    ))
    .settings(
      assembly / mainClass := Some("com.samlaberge.ExecutorMain")
    )
///*.settings(
//  Compile / mainClass := Some("com.samlaberge.StorageServerMain")
//)*/

lazy val `ddagr-protocol` =
  project
    .in(file("protocol"))
    //    .enablePlugins(AutomateHeaderPlugin, GitVersioning)
    .settings(settings)
    .settings(scalaPbSettings)
    .settings(
      libraryDependencies ++= Seq(
        library.grpcNetty,
        library.scalaPbRuntime,
        library.scalaPbRuntimeGrpc,
        library.scalaTest  % Test
      )
    )

lazy val `client-lib` =
  project
    .in(file("client-lib"))
    .settings(settings)
    .dependsOn(`ddagr-protocol`)
    .settings(
      libraryDependencies ++= Seq(
        library.protocol,
        library.scalaTest  % Test,
        library.apacheCommonsLang,
        library.apacheXbeanAsm,
        library.commonsIO
      )
    )

lazy val `examples` =
  project
    .in(file("examples"))
    .settings(settings)
    .dependsOn(`client-lib`)
    .settings(
      libraryDependencies ++= Seq(
        library.commonsIO,
        library.scalaParallelCollections
      )
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

//import com.trueaccord.scalapb.compiler.{ Version => VersionPb }
val VersionPb = scalapb.compiler.Version
lazy val library =
  new {
    object Version {
      val protocol       = "1.0.0"
      val scalaTest      = "3.2.11"
      val apacheCommonsLang = "3.12.0"
      val apacheXbeanAsm = "4.20"
      val commonsIO = "2.11.0"
      val parallelCollections = "1.0.4"
    }
    val grpcNetty          = "io.grpc"                 % "grpc-netty"                  % VersionPb.grpcJavaVersion
    val protocol           = "com.samlaberge"          %% "ddagr-protocol"   % Version.protocol
    val scalaPbRuntime     = "com.thesamet.scalapb" %% "scalapb-runtime"             % VersionPb.scalapbVersion % "protobuf"
    val scalaPbRuntimeGrpc = "com.thesamet.scalapb" %% "scalapb-runtime-grpc"        % VersionPb.scalapbVersion
    val scalaTest          = "org.scalatest"          %% "scalatest"                   % Version.scalaTest
    val apacheCommonsLang  = "org.apache.commons" % "commons-lang3" % Version.apacheCommonsLang
    val apacheXbeanAsm     = "org.apache.xbean" % "xbean-asm9-shaded" % Version.apacheXbeanAsm
    val commonsIO    = "commons-io" % "commons-io" % Version.commonsIO
    val scalaParallelCollections = "org.scala-lang.modules" %% "scala-parallel-collections" % Version.parallelCollections
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings
//    scalafmtSettings

lazy val commonSettings =
  Seq(
    organization := "com.samlaberge",
    organizationName := "Samuel Laberge",
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_"
    ),
    Compile / unmanagedSourceDirectories := Seq((Compile / scalaSource).value),
    Test / unmanagedSourceDirectories := Seq((Test / scalaSource).value)
  )


lazy val scalaPbSettings = Seq(
  PB.targets in Compile := Seq(scalapb.gen() -> (sourceManaged in Compile).value)
)

//lazy val scalaPbSettings = Seq(
//  Compile / PB.targets := Seq(
//    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
//  ),
//  Compile / PB.protoSources := Seq((ThisBuild / baseDirectory).value / "src" / "main" / "protobuf")
//)

//lazy val scalafmtSettings =
//  Seq(
//    scalafmtOnCompile := true,
//    scalafmtOnCompile.in(Sbt) := false,
//    scalafmtVersion := "1.3.0"
//  )

ThisBuild / assemblyMergeStrategy := {
  case x if x.contains("io.netty.versions.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
