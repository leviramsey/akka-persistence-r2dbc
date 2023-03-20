/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import akka.persistence.r2dbc.journal.JournalDao

import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
import io.r2dbc.spi.ConnectionFactory

import scala.concurrent.ExecutionContext

/**
 * INTERNAL API
 *
 * Represents the particular variety of database being used by this persistence plugin and exposes factories for the DAO
 * objects which interact with the database.
 *
 * In earlier versions of this plugin, this interface was not for extension outside of the plugin.
 */
@InternalStableApi
trait Dialect {
  def getJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): JournalDao
}

/**
 * INTERNAL API
 */
@InternalStableApi
object Dialect {
  case object Postgres extends Dialect {
    def getJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
        ec: ExecutionContext,
        system: ActorSystem[_]): JournalDao = Dialect.buildJournalDao(journalSettings, connectionFactory)
  }

  case object Yugabyte extends Dialect {
    def getJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
        ec: ExecutionContext,
        system: ActorSystem[_]): JournalDao = Dialect.buildJournalDao(journalSettings, connectionFactory)
  }

  // postgres and yugabyte have historically used the same DAOs
  private def buildJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): JournalDao =
    new JournalDao(journalSettings, connectionFactory)
}
