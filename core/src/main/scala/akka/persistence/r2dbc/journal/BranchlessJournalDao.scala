/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import JournalDao.{ SerializedEventMetadata, SerializedJournalRow }

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.Dialect.{ Postgres, Yugabyte }
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.typed.PersistenceId

import io.r2dbc.spi.{ Connection, ConnectionFactory, Row, Statement }
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.{ ExecutionContext, Future }

import java.time.Instant

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object BranchlessJournalDao {
  val log: Logger = LoggerFactory.getLogger("JournalDao")
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  def readMetadata(row: Row): Option[SerializedEventMetadata] =
    row.get("meta_payload", classOf[Array[Byte]]) match {
      case null => None
      case metaPayload =>
        Some(
          SerializedEventMetadata(
            serId = row.get("meta_ser_id", classOf[Integer]),
            serManifest = row.get("meta_ser_manifest", classOf[String]),
            metaPayload))
    }

  def apply(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): BranchlessJournalDao =
    journalSettings.dialect match {
      case Postgres | Yugabyte =>
        journalSettings.dbTimestampMonotonicIncreasing match {
          case true  => new MonotonicIncreasingDBTimestampJournalDao(journalSettings, connectionFactory)
          case false => new SubselectBasedDBTimestampJournalDao(journalSettings, connectionFactory)
        }
    }
}

/**
 * INTERNAL API
 *
 * API for doing DB interaction outside of an actor to avoid mistakes in Future callbacks
 */
@InternalApi
trait BranchlessJournalDao {

  /**
   * All events must be for the same persistenceId.
   *
   * The returned timestamp should be what was written as the `dbTimestamp` field for the events, which may not
   * necessarily be the `dbTimestamp` of the incoming event. Publishing events directly to queries depends on this being
   * true. Only if that feature is disabled (see R2dbcSettings.journalPublishEvents), the returned timestamp may be
   * JournalDao.EmptyDbTimestamp.
   */
  def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant]
  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long]
  def readLowestSequenceNr(persistenceId: String): Future[Long]
  def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit]
}

/**
 * INTERNAL API
 */
@InternalApi
object PostgresJournalDao {
  trait Queries {
    // (persistence_id, seq_nr) => seq_nr
    def selectHighestSequenceNrSql: String
    // persistence_id => seq_nr
    def selectLowestSequenceNrSql: String
    // (persistence_id, seq_nr, seq_nr)
    def deleteEventsSql: String
    // (
    //  slice, entity_type, persistence_id,
    //  seq_nr, writer, adapter_manifest,
    //  event_ser_id, event_ser_manifest, event_payload,
    //  deleted
    // )
    def insertDeleteMarkerSql: String
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] abstract class PostgresJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(
    implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends BranchlessJournalDao {
  import BranchlessJournalDao.log
  import PostgresJournalDao.Queries

  def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant] = {
    require(events.nonEmpty)

    // the events are all for the same persistence ID and in order
    val persistenceId = events.head.persistenceId
    val previousSeqNr = events.head.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = events.head.dbTimestamp == Instant.EPOCH
    val totalEvents = events.size

    val (insertStatementFactory, bind) = insertStatementFactoryAndBinder(useTimestampFromDb, previousSeqNr)

    if (totalEvents == 1) {
      val result = r2dbcExecutor.updateOneReturning(s"insert [$persistenceId]")(
        connection => bind(insertStatementFactory(connection), events.head),
        row => row.get(0, classOf[Instant]))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
        }
      result
    } else {
      val result = r2dbcExecutor.updateInBatchReturning(s"batch insert [$persistenceId], [$totalEvents] events")(
        connection =>
          events.foldLeft(insertStatementFactory(connection)) { (stmt, write) =>
            stmt.add()
            bind(stmt, write)
          },
        row => row.get(0, classOf[Instant]))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", totalEvents, events.head.persistenceId)
        }
      result.map(_.head)(ExecutionContexts.parasitic)
    }
  }

  protected def insertStatementFactoryAndBinder(
      useTimestampFromDb: Boolean,
      previousSeqNr: Long): (Connection => Statement, (Statement, SerializedJournalRow) => Statement)

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

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

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

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Lowest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit] = {
    def insertDeleteMarkerStmt(deleteMarkerSeqNr: Long, connection: Connection): Statement = {
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)

      connection
        .createStatement(queries.insertDeleteMarkerSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, deleteMarkerSeqNr)
        .bind(4, "")
        .bind(5, "")
        .bind(6, 0)
        .bind(7, "")
        .bind(8, Array.emptyByteArray)
        .bind(9, true)
    }

    def deleteBatch(from: Long, to: Long, lastBatch: Boolean): Future[Unit] =
      (if (lastBatch && !resetSequenceNumber) {
         r2dbcExecutor
           .update(s"delete [$persistenceId] and insert marker") { connection =>
             Vector(
               connection
                 .createStatement(queries.deleteEventsSql)
                 .bind(0, persistenceId)
                 .bind(1, from)
                 .bind(2, to),
               insertDeleteMarkerStmt(to, connection))
           }
           .map(_.head)(ExecutionContexts.parasitic)
       } else {
         r2dbcExecutor
           .updateOne(s"delete [$persistenceId]") { connection =>
             connection
               .createStatement(queries.deleteEventsSql)
               .bind(0, persistenceId)
               .bind(1, from)
               .bind(2, to)
           }
       }).map(deletedRows =>
        if (log.isDebugEnabled) {
          log.debugN(
            "Deleted [{}] events for persistenceId [{}], from seq num [{}] to [{}]",
            deletedRows,
            persistenceId,
            from,
            to)
        })(ExecutionContexts.parasitic)

    val batchSize = journalSettings.cleanupSettings.eventsJournalDeleteBatchSize

    def deleteInBatches(from: Long, maxTo: Long): Future[Unit] =
      if (from + batchSize > maxTo) deleteBatch(from, maxTo, true)
      else {
        val to = from + batchSize - 1
        deleteBatch(from, to, false).flatMap(_ => deleteInBatches(to + 1, maxTo))
      }

    highestSequenceNrForDelete(persistenceId, toSequenceNr)
      .flatMap { toSeqNr =>
        lowestSequenceNrForDelete(persistenceId, toSeqNr, batchSize)
          .flatMap { fromSeqNr =>
            deleteInBatches(fromSeqNr, toSeqNr)
          }
          .flatMap(_ => Future.unit)
      }
  }

  protected val persistenceExt = Persistence(system)
  protected val journalTable = journalSettings.journalTableWithSchema
  protected val queries: Queries = new Queries {
    val selectHighestSequenceNrSql =
      sql"""SELECT MAX(seq_nr) FROM $journalTable WHERE persistence_id = ? AND seq_nr >= ?"""

    val selectLowestSequenceNrSql = sql"""SELECT MIN(seq_nr) FROM $journalTable WHERE persistence_id = ?"""

    val deleteEventsSql =
      sql"""DELETE FROM $journalTable WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?"""

    val insertDeleteMarkerSql =
      sql"""
        INSERT INTO $journalTable
        (slice, entity_type, persistence_id, seq_nr, db_timestamp,            writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted) VALUES
        (?,     ?,           ?,              ?,      transaction_timestamp(), ?,      ?,                ?,            ?,                  ?,             ?)"""
  }

  protected val r2dbcExecutor =
    new R2dbcExecutor(connectionFactory, log, journalSettings.logDbCallsExceeding)(ec, system)

  protected def highestSequenceNrForDelete(persistenceId: String, toSequenceNr: Long): Future[Long] =
    if (toSequenceNr == Long.MaxValue) readHighestSequenceNr(persistenceId, 0L)
    else Future.successful(toSequenceNr)

  protected def lowestSequenceNrForDelete(persistenceId: String, toSeqNr: Long, batchSize: Int): Future[Long] =
    if (toSeqNr <= batchSize) Future.successful(1L)
    else readLowestSequenceNr(persistenceId)
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class MonotonicIncreasingDBTimestampJournalDao(
    journalSettings: R2dbcSettings,
    connectionFactory: ConnectionFactory)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends PostgresJournalDao(journalSettings, connectionFactory)(ec, system) {
  import BranchlessJournalDao.log

  require(journalSettings.dbTimestampMonotonicIncreasing)

  // because this DAO assumes that the DB's timestamps are monotonic increasing from persist batch to persist batch
  // for a given persistence ID, the previousSequenceNr is irrelevant
  protected def insertStatementFactoryAndBinder(
      useTimestampFromDb: Boolean,
      ignored: Long): (Connection => Statement, (Statement, SerializedJournalRow) => Statement) =
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
    stmt.bind(13, write.dbTimestamp)

    stmt
  }

  private val bindTransactionTimestamp = { (stmt: Statement, write: SerializedJournalRow) =>
    commonBind(stmt, write)
    stmt
  }

  private val transactionTimestampPair = transactionTimestampFactory -> bindTransactionTimestamp
  private val parameterTimestampPair = parameterTimestampFactory -> bindParameterTimestamp

  private def commonBind(stmt: Statement, write: SerializedJournalRow): Unit = {
    stmt
      .bind(0, write.slice)
      .bind(1, write.entityType)
      .bind(2, write.persistenceId)
      .bind(3, write.seqNr)
      .bind(4, write.writerUuid)
      .bind(5, "") // FIXME event adapter
      .bind(6, write.serId)
      .bind(7, write.serManifest)
      .bind(8, write.payload.get)

    if (write.tags.isEmpty)
      stmt.bindNull(9, classOf[Array[String]])
    else
      stmt.bind(9, write.tags.toArray)

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
 */
@InternalApi
private[r2dbc] class SubselectBasedDBTimestampJournalDao(
    journalSettings: R2dbcSettings,
    connectionFactory: ConnectionFactory)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends PostgresJournalDao(journalSettings, connectionFactory)(ec, system) {
  import BranchlessJournalDao.log

  require(!journalSettings.dbTimestampMonotonicIncreasing)

  protected def insertStatementFactoryAndBinder(
      useTimestampFromDb: Boolean,
      previousSeqNr: Long): (Connection => Statement, (Statement, SerializedJournalRow) => Statement) =
    if (useTimestampFromDb) transactionTimestampFactory -> bindTransactionTimestamp(previousSeqNr)
    else parameterTimestampFactory -> bindParameterTimestamp(previousSeqNr)

  private val commonInsertSql =
    s"INSERT INTO $journalTable " +
    "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "

  private val timestampSubselect =
    s"(SELECT db_timestamp + '1 microsecond'::interval FROM $journalTable " +
    "WHERE persistence_id = ? AND seq_nr = ?)"

  private val insertEventWithParameterTimestampSql =
    sql"$commonInsertSql GREATEST(?, $timestampSubselect)) RETURNING db_timestamp"

  private val insertEventWithTransactionTimestampSql =
    sql"$commonInsertSql GREATEST(transaction_timestamp(), $timestampSubselect)) RETURNING db_timestamp"

  private val parameterTimestampFactory = { (conn: Connection) =>
    conn.createStatement(insertEventWithParameterTimestampSql)
  }

  private val transactionTimestampFactory = { (conn: Connection) =>
    conn.createStatement(insertEventWithTransactionTimestampSql)
  }

  private def bindParameterTimestamp(previousSeqNr: Long) = { (stmt: Statement, write: SerializedJournalRow) =>
    commonBind(stmt, write)

    stmt
      .bind(13, write.dbTimestamp)
      .bind(14, write.persistenceId)
      .bind(15, previousSeqNr)

    stmt
  }

  private def bindTransactionTimestamp(previousSeqNr: Long) = { (stmt: Statement, write: SerializedJournalRow) =>
    commonBind(stmt, write)

    stmt
      .bind(13, write.persistenceId)
      .bind(14, previousSeqNr)
  }

  private def commonBind(stmt: Statement, write: SerializedJournalRow): Unit = {
    stmt
      .bind(0, write.slice)
      .bind(1, write.entityType)
      .bind(2, write.persistenceId)
      .bind(3, write.seqNr)
      .bind(4, write.writerUuid)
      .bind(5, "") // FIXME event adapter
      .bind(6, write.serId)
      .bind(7, write.serManifest)
      .bind(8, write.payload.get)

    if (write.tags.isEmpty)
      stmt.bindNull(9, classOf[Array[String]])
    else
      stmt.bind(9, write.tags.toArray)

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
