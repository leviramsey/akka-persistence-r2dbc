addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.9.0") // for maintenance of copyright file header
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")

// for releasing
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")

//// docs
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.2")
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.47")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.2")
