/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.Dialect
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.query.scaladsl.QueryDao.setFromDb
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object QueryDao {
  val log: Logger = LoggerFactory.getLogger(classOf[QueryDao])
  private def setFromDb[T](array: Array[T]): Set[T] = array match {
    case null    => Set.empty[T]
    case entries => entries.toSet
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class QueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends BySliceQuery.Dao[SerializedJournalRow] {
  import JournalDao.readMetadata
  import QueryDao.log

  private val journalTable = settings.journalTableWithSchema
  private implicit val journalPayloadCodec: PayloadCodec = settings.journalPayloadCodec

  private val currentDbTimestampSql =
    "SELECT transaction_timestamp() AS db_timestamp"

  private def eventsBySlicesRangeSql(
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= ?" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < transaction_timestamp() - interval '${behindCurrentTime.toMillis} milliseconds'"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT slice, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp, tags "
      else
        "SELECT slice, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp, tags, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    sql"""
      $selectColumns
      FROM $journalTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      AND deleted = false
      ORDER BY db_timestamp, seq_nr
      LIMIT ?"""
  }

  private def sliceCondition(minSlice: Int, maxSlice: Int): String = {
    settings.dialect match {
      case Dialect.Yugabyte => s"slice BETWEEN $minSlice AND $maxSlice"
      case Dialect.Postgres => s"slice in (${(minSlice to maxSlice).mkString(",")})"
    }
  }

  private def selectBucketsSql(minSlice: Int, maxSlice: Int): String = {
    sql"""
      SELECT extract(EPOCH from db_timestamp)::BIGINT / 10 AS bucket, count(*) AS count
      FROM $journalTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? AND db_timestamp <= ?
      AND deleted = false
      GROUP BY bucket ORDER BY bucket LIMIT ?
      """
  }

  private val selectTimestampOfEventSql = sql"""
    SELECT db_timestamp FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = false"""

  private val selectOneEventSql = sql"""
    SELECT slice, entity_type, db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload, tags
    FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = false"""

  private val selectEventsSql = sql"""
    SELECT slice, entity_type, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
    from $journalTable
    WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?
    AND deleted = false
    ORDER BY seq_nr
    LIMIT ?"""

  private val allPersistenceIdsSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable ORDER BY persistence_id LIMIT ?"

  private val persistenceIdsForEntityTypeSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id LIKE ? ORDER BY persistence_id LIMIT ?"

  private val allPersistenceIdsAfterSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"

  private val persistenceIdsForEntityTypeAfterSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id LIKE ? AND persistence_id > ? ORDER BY persistence_id LIMIT ?"

  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, settings.logDbCallsExceeding)(ec, system)

  def currentDbTimestamp(): Future[Instant] = {
    r2dbcExecutor
      .selectOne("select current db timestamp")(
        connection => connection.createStatement(currentDbTimestampSql),
        row => row.get("db_timestamp", classOf[Instant]))
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
      }
  }

  def rowsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): Source[SerializedJournalRow, NotUsed] = {
    val result = r2dbcExecutor.select(s"select eventsBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection
          .createStatement(
            eventsBySlicesRangeSql(
              toDbTimestampParam = toTimestamp.isDefined,
              behindCurrentTime,
              backtracking,
              minSlice,
              maxSlice))
          .bind(0, entityType)
          .bind(1, fromTimestamp)
        toTimestamp match {
          case Some(until) =>
            stmt.bind(2, until)
            stmt.bind(3, settings.querySettings.bufferSize)
          case None =>
            stmt.bind(2, settings.querySettings.bufferSize)
        }
        stmt
      },
      row =>
        if (backtracking)
          SerializedJournalRow(
            slice = row.get[Integer]("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.get("db_timestamp", classOf[Instant]),
            readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
            payload = None, // lazy loaded for backtracking
            serId = 0,
            serManifest = "",
            writerUuid = "", // not need in this query
            tags = setFromDb(row.get("tags", classOf[Array[String]])),
            metadata = None)
        else
          SerializedJournalRow(
            slice = row.get[Integer]("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.get("db_timestamp", classOf[Instant]),
            readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
            payload = Some(row.getPayload("event_payload")),
            serId = row.get[Integer]("event_ser_id", classOf[Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = "", // not need in this query
            tags = setFromDb(row.get("tags", classOf[Array[String]])),
            metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debugN("Read [{}] events from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  override def countBuckets(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      limit: Int): Future[Seq[Bucket]] = {

    val toTimestamp = {
      val now = InstantFactory.now() // not important to use database time
      if (fromTimestamp == Instant.EPOCH)
        now
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(now)) now else t
      }
    }

    val result = r2dbcExecutor.select(s"select bucket counts [$minSlice - $maxSlice]")(
      connection =>
        connection
          .createStatement(selectBucketsSql(minSlice, maxSlice))
          .bind(0, entityType)
          .bind(1, fromTimestamp)
          .bind(2, toTimestamp)
          .bind(3, limit),
      row => {
        val bucketStartEpochSeconds = row.get[java.lang.Long]("bucket", classOf[java.lang.Long]).toLong * 10
        val count = row.get[java.lang.Long]("count", classOf[java.lang.Long]).toLong
        Bucket(bucketStartEpochSeconds, count)
      })

    if (log.isDebugEnabled)
      result.foreach(rows => log.debugN("Read [{}] bucket counts from slices [{} - {}]", rows.size, minSlice, maxSlice))

    result
  }

  /**
   * Events are append only
   */
  override def countBucketsMayChange: Boolean = false

  def timestampOfEvent(persistenceId: String, seqNr: Long): Future[Option[Instant]] = {
    r2dbcExecutor.selectOne("select timestampOfEvent")(
      connection =>
        connection
          .createStatement(selectTimestampOfEventSql)
          .bind(0, persistenceId)
          .bind(1, seqNr),
      row => row.get("db_timestamp", classOf[Instant]))
  }

  def loadEvent(persistenceId: String, seqNr: Long): Future[Option[SerializedJournalRow]] =
    r2dbcExecutor.selectOne("select one event")(
      connection =>
        connection
          .createStatement(selectOneEventSql)
          .bind(0, persistenceId)
          .bind(1, seqNr),
      row =>
        SerializedJournalRow(
          slice = row.get[Integer]("slice", classOf[Integer]),
          entityType = row.get("entity_type", classOf[String]),
          persistenceId,
          seqNr,
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
          readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
          payload = Some(row.getPayload("event_payload")),
          serId = row.get[Integer]("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = "", // not need in this query
          tags = setFromDb(row.get("tags", classOf[Array[String]])),
          metadata = readMetadata(row)))

  def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalRow, NotUsed] = {

    val result = r2dbcExecutor.select(s"select eventsByPersistenceId [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectEventsSql)
          .bind(0, persistenceId)
          .bind(1, fromSequenceNr)
          .bind(2, toSequenceNr)
          .bind(3, settings.querySettings.bufferSize),
      row =>
        SerializedJournalRow(
          slice = row.get[Integer]("slice", classOf[Integer]),
          entityType = row.get("entity_type", classOf[String]),
          persistenceId = row.get("persistence_id", classOf[String]),
          seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
          readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
          payload = Some(row.getPayload("event_payload")),
          serId = row.get[Integer]("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = row.get("writer", classOf[String]),
          tags = setFromDb(row.get("tags", classOf[Array[String]])),
          metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events for persistenceId [{}]", rows.size, persistenceId))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  def persistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"
    val result = r2dbcExecutor.select(s"select persistenceIds by entity type")(
      connection =>
        afterId match {
          case Some(after) =>
            connection
              .createStatement(persistenceIdsForEntityTypeAfterSql)
              .bind(0, entityType + likeStmtPostfix)
              .bind(1, after)
              .bind(2, limit)
          case None =>
            connection
              .createStatement(persistenceIdsForEntityTypeSql)
              .bind(0, entityType + likeStmtPostfix)
              .bind(1, limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids by entity type [{}]", rows.size, entityType))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            connection
              .createStatement(allPersistenceIdsAfterSql)
              .bind(0, after)
              .bind(1, limit)
          case None =>
            connection
              .createStatement(allPersistenceIdsSql)
              .bind(0, limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

}
