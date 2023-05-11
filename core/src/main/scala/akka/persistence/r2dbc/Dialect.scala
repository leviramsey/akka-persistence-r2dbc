/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.journal.PostgresJournalDao
import akka.persistence.r2dbc.snapshot.SnapshotDao
import akka.persistence.r2dbc.snapshot.PostgresSnapshotDao

import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
import io.r2dbc.spi.ConnectionFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.Instant

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
  def getJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): JournalDao

  def getSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): SnapshotDao
}

/**
 * INTERNAL API
 */
@InternalStableApi
object Dialect {
  case object Postgres extends Dialect {
    def getJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
        ec: ExecutionContext,
        system: ActorSystem[_]): JournalDao = Dialect.buildPostgresJournalDao(settings, connectionFactory)

    def getSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
        ec: ExecutionContext,
        system: ActorSystem[_]): SnapshotDao = Dialect.buildPostgresSnapshotDao(settings, connectionFactory)
  }

  case object Yugabyte extends Dialect {
    def getJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
        ec: ExecutionContext,
        system: ActorSystem[_]): JournalDao = Dialect.buildPostgresJournalDao(settings, connectionFactory)

    def getSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
        ec: ExecutionContext,
        system: ActorSystem[_]): SnapshotDao = Dialect.buildPostgresSnapshotDao(settings, connectionFactory)
  }

  // postgres and yugabyte have historically used the same DAOs
  private def buildPostgresJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): JournalDao =
    if (settings.dbTimestampMonotonicIncreasing) {
      new PostgresJournalDao.MonotonicIncreasingDBTimestampJournalDao(settings, connectionFactory)
    } else {
      new PostgresJournalDao.DBTimestampFromSubselectJournalDao(settings, connectionFactory)
    }

  private def buildPostgresSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): SnapshotDao = new PostgresSnapshotDao.StandardImpl(settings, connectionFactory)
}
