/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = {
    val defaultConfig = ConfigFactory.load()
    val dialect = defaultConfig.getString("akka.persistence.r2dbc.dialect")

    val dialectConfig = dialect match {
      case "postgres" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc.connection-factory {
            driver = "postgres"
            host = "localhost"
            port = 5432
            user = "postgres"
            password = "postgres"
            database = "postgres"
          }
          """)
      case "yugabyte" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc.connection-factory {
            driver = "postgres"
            host = "localhost"
            port = 5433
            user = "yugabyte"
            password = "yugabyte"
            database = "yugabyte"
          }
          """)

      case "azure-cosmos" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc {
              connection-factory {
              driver = "postgres"
              host = ${AZURE_COSMOSDB_POSTGRES_HOST}
              port = ${AZURE_COSMOSDB_POSTGRES_PORT}
              user = ${AZURE_COSMOSDB_POSTGRES_USER}
              password = ${AZURE_COSMOSDB_POSTGRES_PASS}
              database = "citus"

              ssl {
                enabled = on
                mode = require
              }
            }

            # Assumes that the DB timestamp will not move meaningfully backwards in the
            # duration between successive persists for the same persistence ID.
            #
            # Since we're distributing to Citus workers based on persistence ID, this
            # implies that the maximum amount of time any worker's clock can go backwards
            # is less than the minimum amount of time between event batches for a given
            # persistence ID.
            db-timestamp-monotonic-increasing = on
          }
          """)
    }

    // using load here so that connection-factory can be overridden
    ConfigFactory.load(dialectConfig.withFallback(ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.snapshot-store.plugin = "akka.persistence.r2dbc.snapshot"
    akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
      }
    }
    akka.actor {
      serialization-bindings {
        "akka.persistence.r2dbc.CborSerializable" = jackson-cbor
      }
    }
    akka.actor.testkit.typed.default-timeout = 10s
    """)))
  }

  val backtrackingDisabledConfig: Config =
    ConfigFactory.parseString("akka.persistence.r2dbc.query.backtracking.enabled = off")
}
