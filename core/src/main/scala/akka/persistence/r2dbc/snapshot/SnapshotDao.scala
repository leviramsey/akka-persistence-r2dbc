/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.snapshot

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.R2dbcExecutor

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.typed.PersistenceId
import akka.persistence.SnapshotSelectionCriteria
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.LoggerFactory
import org.slf4j.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
object SnapshotDao {
  final case class SerializedSnapshotRow(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Long,
      snapshot: Array[Byte],
      serializerId: Int,
      serializerManifest: String,
      metadata: Option[SerializedSnapshotMetadata])

  final case class SerializedSnapshotMetadata(payload: Array[Byte], serializerId: Int, serializerManifest: String)

  private[r2dbc] val log: Logger = LoggerFactory.getLogger(classOf[SnapshotDao])
}

/**
 * INTERNAL API
 *
 * Interface for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
trait SnapshotDao {
  import SnapshotDao.SerializedSnapshotRow

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]]
  def store(serializedRow: SerializedSnapshotRow): Future[Unit]
  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit]
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object PostgresSnapshotDao {
  import SnapshotDao.SerializedSnapshotRow
  import SnapshotDao.SerializedSnapshotMetadata

  /**
   * INTERNAL API
   */
  @InternalApi
  final class StandardImpl(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_])
      extends SnapshotDao {

    import SnapshotDao._

    def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]] =
      r2dbcExecutor
        .select(s"select snapshot [$persistenceId], criteria: [$criteria]")(
          { connection =>
            val statementWithPersistenceId = connection.createStatement(selectSql(criteria)).bind(0, persistenceId)

            bindCriteria(statementWithPersistenceId, criteria, 0)
          },
          collectSerializedSnapshot)
        .map(_.headOption)(ExecutionContexts.parasitic)

    def store(serializedRow: SerializedSnapshotRow): Future[Unit] = {
      val entityType = PersistenceId.extractEntityType(serializedRow.persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(serializedRow.persistenceId)

      r2dbcExecutor
        .updateOne(s"upsert snapshot [${serializedRow.persistenceId}], sequence number [${serializedRow.seqNr}]") {
          connection =>
            val statement =
              connection
                .createStatement(upsertSql)
                .bind(0, slice)
                .bind(1, entityType)
                .bind(2, serializedRow.persistenceId)
                .bind(3, serializedRow.seqNr)
                .bind(4, serializedRow.writeTimestamp)
                .bindPayload(5, serializedRow.snapshot)
                .bind(6, serializedRow.serializerId)
                .bind(7, serializedRow.serializerManifest)

            serializedRow.metadata match {
              case Some(SerializedSnapshotMetadata(serializedMeta, serializerId, serializerManifest)) =>
                statement
                  .bind(8, serializedMeta)
                  .bind(9, serializerId)
                  .bind(10, serializerManifest)

              case None =>
                statement
                  .bindNull(8, classOf[Array[Byte]])
                  .bindNull(9, classOf[Integer])
                  .bindNull(10, classOf[String])
            }

            statement
        }
        .flatMap(_ => Future.unit)(ExecutionContexts.parasitic)
    }

    def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
      r2dbcExecutor
        .updateOne(s"delete snapshot [$persistenceId], criteria [$criteria]") { connection =>
          val statementWithPersistenceId = connection.createStatement(deleteSql(criteria)).bind(0, persistenceId)

          bindCriteria(statementWithPersistenceId, criteria, 0)
        }
        .flatMap(_ => Future.unit)(ExecutionContexts.parasitic)

    private val snapshotTable = settings.snapshotsTableWithSchema
    private val persistenceExt = Persistence(system)
    private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, settings.logDbCallsExceeding)(ec, system)
    private implicit val snapshotPayloadCodec = settings.snapshotPayloadCodec

    private val upsertSql = sql"""
      INSERT INTO $snapshotTable
      (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (persistence_id)
      DO UPDATE SET
        seq_nr = excluded.seq_nr,
        write_timestamp = excluded.write_timestamp,
        snapshot = excluded.snapshot,
        ser_id = excluded.ser_id,
        ser_manifest = excluded.ser_manifest,
        meta_payload = excluded.meta_payload,
        meta_ser_id = excluded.meta_ser_id,
        meta_ser_manifest = excluded.meta_ser_manifest"""

    private def selectSql(criteria: SnapshotSelectionCriteria): String =
      sql"""
        SELECT persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest
        FROM $snapshotTable
        WHERE persistence_id = ?
        ${selectionConditions(criteria)}
        LIMIT 1"""

    private def deleteSql(criteria: SnapshotSelectionCriteria): String =
      sql"""
        DELETE FROM $snapshotTable
        WHERE persistence_id = ?
        ${selectionConditions(criteria)}"""

    private def selectionConditions(criteria: SnapshotSelectionCriteria): String = {
      val maxSeqNrCondition =
        if (criteria.maxSequenceNr != Long.MaxValue) " AND seq_nr <= ?"
        else ""

      val minSeqNrCondition =
        if (criteria.minSequenceNr > 0L) " AND seq_nr >= ?"
        else ""

      val maxTimestampCondition =
        if (criteria.maxTimestamp != Long.MaxValue) " AND write_timestamp <= ?"
        else ""

      val minTimestampCondition =
        if (criteria.minTimestamp != 0L) " AND write_timestamp >= ?"
        else ""

      s"$maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition"
    }

    private def bindCriteria(
        statement: Statement,
        criteria: SnapshotSelectionCriteria,
        maxAlreadyBoundIdx: Int): Statement = {
      var bindIdx = maxAlreadyBoundIdx

      if (criteria.maxSequenceNr != Long.MaxValue) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.maxSequenceNr)
      }

      if (criteria.minSequenceNr > 0L) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.minSequenceNr)
      }

      if (criteria.maxTimestamp != Long.MaxValue) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.maxTimestamp)
      }

      if (criteria.minTimestamp > 0L) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.minTimestamp)
      }

      statement
    }

    private def collectSerializedSnapshot(row: Row): SerializedSnapshotRow =
      SerializedSnapshotRow(
        row.get("persistence_id", classOf[String]),
        row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
        row.get[java.lang.Long]("write_timestamp", classOf[java.lang.Long]),
        row.getPayload("snapshot"),
        row.get[Integer]("ser_id", classOf[Integer]),
        row.get("ser_manifest", classOf[String]), {
          val metaSerializerId = row.get("meta_ser_id", classOf[Integer])
          if (metaSerializerId eq null) None
          else
            Some(
              SerializedSnapshotMetadata(
                row.get("meta_payload", classOf[Array[Byte]]),
                metaSerializerId,
                row.get("meta_ser_manifest", classOf[String])))
        })
  }
}
