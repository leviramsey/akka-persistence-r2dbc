/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.typed.PersistenceId
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.Instant

/**
 * INTERNAL API
 *
 * Common types and utility methods for implementations of the JournalDao.
 */
@InternalApi
object JournalDao {
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedJournalRow(
      slice: Int,
      entityType: String,
      persistenceId: String,
      seqNr: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      payload: Option[Array[Byte]],
      serId: Int,
      serManifest: String,
      writerUuid: String,
      tags: Set[String],
      metadata: Option[SerializedEventMetadata])
      extends BySliceQuery.SerializedRow {

    // the MigrationTool preserves the original timestamp, so we shouldn't replace the timestamp in the row
    // when inserting
    def useTimestampFromDb: Boolean = dbTimestamp == EmptyDbTimestamp
  }

  final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])

  def readMetadata(
      row: Row,
      metaPayloadColumn: String = "meta_payload",
      metaSerializerIdColumn: String = "meta_ser_id",
      metaSerializerManifestColumn: String = "meta_ser_manifest"): Option[SerializedEventMetadata] =
    row.get(metaPayloadColumn, classOf[Array[Byte]]) match {
      case null => None
      case metaPayload =>
        Some(
          SerializedEventMetadata(
            serId = row.get[Integer](metaSerializerIdColumn, classOf[Integer]),
            serManifest = row.get(metaSerializerManifestColumn, classOf[String]),
            metaPayload))
    }
}

/**
 * INTERNAL API
 *
 * Interface for doing DB interaction outside of an actor.
 */
@InternalApi
trait JournalDao {
  import JournalDao.SerializedJournalRow

  /**
   * It may be assumed that all events are for the same persistence ID.
   *
   * If events are being published directly to queries, the returned timestamp must be what was written as the
   * `dbTimestamp` field for the events, which may not necessarily be the `dbTimestamp` of any incoming event. If
   * publishing directly to queries is disabled (see R2dbcSettings.journalPublishEvents), the returned timestamp may
   * instead be `JournalDao.EmptyDbTimestamp`.
   */
  def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant]

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long]

  def readLowestSequenceNr(persistenceId: String): Future[Long]

  def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit]
}

/**
 * INTERNAL API
 */
private[r2dbc] abstract class PostgresJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(
    implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends JournalDao {

  import JournalDao.SerializedJournalRow
  import PostgresJournalDao.Queries
  import PostgresJournalDao.log

  def writeEvents(events: Seq[JournalDao.SerializedJournalRow]): Future[Instant] = {
    require(events.nonEmpty)

    // the events are all for the same persistence ID and in order
    val persistenceId = events.head.persistenceId
    val previousSeqNr = events.head.seqNr - 1
    val totalEvents = events.size

    // as for which insert statement we want to use, that will partially depend on the concrete implementation...
    val (insertStatementFactory, bind) = insertStatementFactoryAndBinder(events.head.useTimestampFromDb, previousSeqNr)

    val result =
      if (totalEvents == 1) {
        r2dbcExecutor.updateOneReturning(s"insert [$persistenceId]")(
          connection => bind(insertStatementFactory(connection), events.head),
          row => row.get(0, classOf[Instant]))
      } else {
        r2dbcExecutor
          .updateInBatchReturning(s"batch insert [$persistenceId], [$totalEvents] events")(
            connection =>
              events.foldLeft(insertStatementFactory(connection)) { (stmt, write) =>
                stmt.add()
                bind(stmt, write)
              },
            row => row.get(0, classOf[Instant]))
          .map(_.head)(ExecutionContexts.parasitic)
      }

    if (log.isDebugEnabled()) {
      result.foreach { _ => log.debug("Wrote [{}] events for persistenceId [{}]", totalEvents, persistenceId) }
    }

    result
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val result =
      r2dbcExecutor
        .select(s"select highest seqNr [$persistenceId]")(
          connection =>
            connection
              .createStatement(queries.selectHighestSequenceNrSql)
              .bind(0, persistenceId)
              .bind(1, fromSequenceNr),
          row => {
            val seqNr = row.get(0, classOf[java.lang.Long])
            if (seqNr eq null) 0L else seqNr.longValue
          })
        .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContexts.parasitic)

    if (log.isDebugEnabled()) {
      result.foreach { seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr) }
    }

    result
  }

  def readLowestSequenceNr(persistenceId: String): Future[Long] = {
    val result =
      r2dbcExecutor
        .select(s"select lowest seqNr [$persistenceId]")(
          connection =>
            connection
              .createStatement(queries.selectLowestSequenceNrSql)
              .bind(0, persistenceId),
          row => {
            val seqNr = row.get(0, classOf[java.lang.Long])
            if (seqNr eq null) 0L else seqNr.longValue
          })
        .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContexts.parasitic)

    if (log.isDebugEnabled()) {
      result.foreach { seqNr => log.debug("Lowest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr) }
    }

    result
  }

  def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit] = {
    val batchSize = journalSettings.cleanupSettings.eventsJournalDeleteBatchSize

    def deleteInBatches(from: Long, maxTo: Long): Future[Unit] =
      if (from + batchSize > maxTo) deleteBatch(from, maxTo, true)
      else {
        val to = from + batchSize - 1
        deleteBatch(from, to, false).flatMap(_ => deleteInBatches(to + 1, maxTo))
      }

    def deleteBatch(from: Long, to: Long, lastBatch: Boolean): Future[Unit] = {
      val deleteRows =
        if (lastBatch && !resetSequenceNumber) {
          r2dbcExecutor
            .update(s"delete [$persistenceId] and insert marker") { connection =>
              Vector(deleteEventsStmt(from, to, connection), insertDeleteMarkerStmt(to, connection))
            }
            .map(_.head)(ExecutionContexts.parasitic)
        } else {
          r2dbcExecutor.updateOne(s"delete [$persistenceId]")(deleteEventsStmt(from, to, _))
        }

      if (log.isDebugEnabled()) {
        deleteRows.foreach { deletedRows =>
          log.debugN(
            "Deleted [{}] events for persistenceId [{}], from seq num [{}] to [{}]",
            deletedRows,
            persistenceId,
            from,
            to)
        }
      }
      deleteRows.flatMap(_ => Future.unit)(ExecutionContexts.parasitic)
    }

    def deleteEventsStmt(from: Long, to: Long, connection: Connection): Statement =
      connection
        .createStatement(queries.deleteEventsSql)
        .bind(0, persistenceId)
        .bind(1, from)
        .bind(2, to)

    def insertDeleteMarkerStmt(deleteMarkerSeqNr: Long, connection: Connection): Statement = {
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)

      connection
        .createStatement(queries.insertDeleteMarkerSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, deleteMarkerSeqNr)
        .bind(4, "") // writer
        .bind(5, "") // adapter_manifest
        .bind(6, 0) // event_ser_id
        .bind(7, "") // event_ser_manifest
        .bindPayload(8, Array.emptyByteArray) // event_payload
        .bind(9, true) // deleted
    }

    highestSequenceNrForDelete(persistenceId, toSequenceNr)
      .flatMap { toSeqNr =>
        lowestSequenceNrForDelete(persistenceId, toSeqNr, batchSize)
          .flatMap { fromSeqNr =>
            deleteInBatches(fromSeqNr, toSeqNr)
          }
      }
  }

  protected val journalTable = journalSettings.journalTableWithSchema
  protected implicit val journalPayloadCodec: PayloadCodec = journalSettings.journalPayloadCodec

  protected val queries = new Queries {
    val selectHighestSequenceNrSql =
      sql"""SELECT MAX(seq_nr) FROM $journalTable WHERE persistence_id = ? AND seq_nr >= ?"""

    val selectLowestSequenceNrSql =
      sql"""SELECT MIN(seq_nr) FROM $journalTable WHERE persistence_id = ?"""

    val deleteEventsSql =
      sql"""DELETE FROM $journalTable WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?"""

    val insertDeleteMarkerSql =
      sql"""INSERT INTO $journalTable
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
      VALUES (?, ?, ?, ?, transaction_timestamp(), ?, ?, ?, ?, ?, ?)"""
  }

  protected def insertStatementFactoryAndBinder(
      useTimestampFromDb: Boolean,
      previousSeqNr: Long): (Connection => Statement, (Statement, SerializedJournalRow) => Statement)

  protected def highestSequenceNrForDelete(persistenceId: String, toSequenceNr: Long): Future[Long] =
    if (toSequenceNr == Long.MaxValue) readHighestSequenceNr(persistenceId, 0L)
    else Future.successful(toSequenceNr)

  protected def lowestSequenceNrForDelete(persistenceId: String, toSeqNr: Long, batchSize: Int): Future[Long] =
    if (toSeqNr <= batchSize) Future.successful(1L)
    else readLowestSequenceNr(persistenceId)

  protected val persistenceExt = Persistence(system)
  protected val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, journalSettings.logDbCallsExceeding)
}

/**
 * INTERNAL API
 *
 * Useful bits common to the Postgres/Yugabyte-style implementations of JournalDao
 */
@InternalApi
private[r2dbc] object PostgresJournalDao {
  val log: Logger = LoggerFactory.getLogger(classOf[PostgresJournalDao])

  trait Queries {
    // (persistence_id, seq_nr) => seq_nr
    def selectHighestSequenceNrSql: String

    // persistence_id => seq_nr
    def selectLowestSequenceNrSql: String

    // (persistence_id, seq_nr, seq_nr)
    def deleteEventsSql: String

    // (
    //   slice, entity_type, persistence_id,
    //   seq_nr, writer, adapter_manifest,
    //   event_ser_id, event_ser_manifest, event_payload,
    //   deleted
    // )
    def insertDeleteMarkerSql: String
  }

  /**
   * INTERNAL API
   *
   * A JournalDao for Postgres-style DBs (including Yugabyte) which assumes that the DB's timestamps are always
   * monotonic increasing from persist batch to persist batch for a given persistence ID (see
   * `R2dbcSettings.dbTimestampMonotonicIncreasing`)
   */
  @InternalApi
  final class MonotonicIncreasingDBTimestampJournalDao(
      journalSettings: R2dbcSettings,
      connectionFactory: ConnectionFactory)(implicit ec: ExecutionContext, system: ActorSystem[_])
      extends PostgresJournalDao(journalSettings, connectionFactory)(ec, system) {
    import JournalDao.SerializedJournalRow

    require(journalSettings.dbTimestampMonotonicIncreasing)

    // Because we're assuming monotonic increasing DB timestamps, the previousSeqNr is irrelevant
    protected def insertStatementFactoryAndBinder(
        useTimestampFromDb: Boolean,
        previousSeqNr: Long): (Connection => Statement, (Statement, JournalDao.SerializedJournalRow) => Statement) =
      if (useTimestampFromDb) transactionTimestampPair
      else parameterTimestampPair

    private val commonInsertSql =
      s"INSERT INTO $journalTable " +
      "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "

    private val insertEventWithParameterTimestampSql = sql"$commonInsertSql ?) RETURNING db_timestamp"
    private val insertEventWithTransactionTimestampSql =
      sql"$commonInsertSql transaction_timestamp()) RETURNING db_timestamp"

    private val parameterTimestampFactory = { (conn: Connection) =>
      conn.createStatement(insertEventWithParameterTimestampSql)
    }

    private val transactionTimestampFactory = { (conn: Connection) =>
      conn.createStatement(insertEventWithTransactionTimestampSql)
    }

    private val bindParameterTimestamp = { (stmt: Statement, write: SerializedJournalRow) =>
      commonBind(stmt, write)
        .bind(13, write.dbTimestamp)
    }

    private val bindTransactionTimestamp = { (stmt: Statement, write: SerializedJournalRow) =>
      commonBind(stmt, write)
    }

    private val transactionTimestampPair = transactionTimestampFactory -> bindTransactionTimestamp
    private val parameterTimestampPair = parameterTimestampFactory -> bindParameterTimestamp

    private def commonBind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind(0, write.slice)
        .bind(1, write.entityType)
        .bind(2, write.persistenceId)
        .bind(3, write.seqNr)
        .bind(4, write.writerUuid)
        .bind(5, "") // FIXME: event_adapter
        .bind(6, write.serId)
        .bind(7, write.serManifest)
        .bindPayload(8, write.payload.get)

      if (write.tags.isEmpty) {
        stmt.bindNull(9, classOf[Array[String]])
      } else {
        stmt.bind(9, write.tags.toArray)
      }

      // optional metadata
      write.metadata match {
        case Some(m) =>
          stmt
            .bind(10, m.serId)
            .bind(11, m.serManifest)
            .bind(12, m.payload)

        case None =>
          stmt
            .bindNull(10, classOf[Integer])
            .bindNull(11, classOf[String])
            .bindNull(12, classOf[Array[Byte]])
      }
    }
  }

  /**
   * INTERNAL API
   *
   * A JournalDao for Postgres-style DBs (including Yugabyte) which uses a subquery on writing events to ensure that the
   * `db_timestamp` column from one persist batch to the next for a given persistence ID is monotonic increasing.
   */
  @InternalApi
  final class DBTimestampFromSubselectJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(
      implicit
      ec: ExecutionContext,
      system: ActorSystem[_])
      extends PostgresJournalDao(journalSettings, connectionFactory)(ec, system) {
    import JournalDao.SerializedJournalRow

    require(!journalSettings.dbTimestampMonotonicIncreasing)

    protected def insertStatementFactoryAndBinder(
        useTimestampFromDb: Boolean,
        previousSeqNr: Long): (Connection => Statement, (Statement, SerializedJournalRow) => Statement) =
      if (useTimestampFromDb) ???
      else parameterTimestampFactory -> bindParameterTimestamp(previousSeqNr)

    private val commonInsertSql =
      s"INSERT INTO $journalTable " +
      "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "

    private val timestampSubselectSql =
      s"(SELECT db_timestamp + '1 microsecond'::interval FROM $journalTable " +
      "WHERE persistence_id = ? AND seq_nr = ?)"

    private val insertEventWithParameterTimestampSql =
      sql"$commonInsertSql GREATEST(?, $timestampSubselectSql)) RETURNING db_timestamp"

    private val insertEventWithTransactionTimestampSql =
      sql"$commonInsertSql GREATEST(transaction_timestamp(), $timestampSubselectSql)) RETURNING db_timestamp"

    private val parameterTimestampFactory = { (conn: Connection) =>
      conn.createStatement(insertEventWithParameterTimestampSql)
    }

    private val transactionTimestampFactory = { (conn: Connection) =>
      conn.createStatement(insertEventWithTransactionTimestampSql)
    }

    private def bindParameterTimestamp(previousSeqNr: Long) = { (stmt: Statement, write: SerializedJournalRow) =>
      commonBind(stmt, write)
        .bind(13, write.dbTimestamp)
        .bind(14, write.persistenceId)
        .bind(15, previousSeqNr)
    }

    private def bindTransactionTimestamp(previousSeqNr: Long) = { (stmt: Statement, write: SerializedJournalRow) =>
      commonBind(stmt, write)
        .bind(13, write.persistenceId)
        .bind(14, previousSeqNr)
    }

    private def commonBind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind(0, write.slice)
        .bind(1, write.entityType)
        .bind(2, write.persistenceId)
        .bind(3, write.seqNr)
        .bind(4, write.writerUuid)
        .bind(5, "") // FIXME event adapter
        .bind(6, write.serId)
        .bind(7, write.serManifest)
        .bindPayload(8, write.payload.get)

      if (write.tags.isEmpty) {
        stmt.bindNull(9, classOf[Array[String]])
      } else {
        stmt.bind(9, write.tags.toArray)
      }

      // optional metadata
      write.metadata match {
        case Some(m) =>
          stmt
            .bind(10, m.serId)
            .bind(11, m.serManifest)
            .bind(12, m.payload)

        case None =>
          stmt
            .bindNull(10, classOf[Integer])
            .bindNull(11, classOf[String])
            .bindNull(12, classOf[Array[Byte]])
      }
    }
  }
}
