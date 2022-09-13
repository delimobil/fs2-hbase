ThisBuild / organization := "ru.delimobil"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / crossScalaVersions += "3.1.1"

val kindProjectorVersion = "0.13.2"
val fs2Version = "3.2.4"
val hbaseClientVersion = "2.4.1"

val publishSettings = Seq(
  // sonatype config
  publishTo := sonatypePublishToBundle.value,
  ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org",
  sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/delimobil/fs2-hbase")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/delimobil/fs2-hbase"), "scm:git@github.com/delimobil/fs2-hbase.git")
  ),
  developers := List(
    Developer(
      id = "nikiforo",
      name = "Artem Nikiforov",
      email = "anikiforov@delimobil.ru",
      url = url("https://github.com/nikiforo")
    )
  )
)

val commonSettings = Seq(
  version := "0.1.0-RC8",
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((3, _)) =>
        Seq("-source:3.0-migration", "-Ykind-projector:underscores")
      case Some((2, 13)) =>
        Seq("-deprecation", "-Xfatal-warnings")
      case Some((2, 12)) =>
        Seq("-Ypartial-unification")
      case _ =>
        Seq()
    }
  },
  libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) =>
        Seq(
          compilerPlugin(
            ("org.typelevel" %% "kind-projector" % kindProjectorVersion).cross(CrossVersion.full)
          )
        )
      case _ =>
        Seq()
    }
  }
)

val root = (project in file("."))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    name := "fs2-hbase",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "org.apache.hbase" % "hbase-client" % hbaseClientVersion
    )
  )
