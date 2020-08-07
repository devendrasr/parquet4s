package com.github.mjakubowski84.parquet4s.parquet

import java.util.UUID

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import com.github.mjakubowski84.parquet4s._
import fs2.concurrent.{Queue, SignallingRef}
import fs2.{Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.MessageType

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object rotatingWriter {

  private sealed trait WriterEvent
  private case class DataEvent[T](data: T) extends WriterEvent
  private case class RotateEvent(tick: FiniteDuration) extends WriterEvent
  private case object StopEvent extends WriterEvent

  private object RecordWriter {
    def apply[F[_]](path: Path, schema: MessageType, options: ParquetWriter.Options)(implicit F: Sync[F]): F[RecordWriter[F]] =
      // TODO Blocker needed here as blocking IO is performed (and in many other places, too)
      F.delay(ParquetWriter.internalWriter(path, schema, options)).map(iw => new RecordWriter(iw))
  }

  private class RecordWriter[F[_]](internalWriter: ParquetWriter.InternalWriter)(implicit F: Sync[F]) {

    def write(record: RowParquetRecord): F[Unit] = F.delay(internalWriter.write(record))

    def dispose: F[Unit] = F.delay(internalWriter.close())

  }

  private class RotatingWriter[T, F[_]](
                                        basePath: Path,
                                        options: ParquetWriter.Options,
                                        writersRef: Ref[F, Map[Path, RecordWriter[F]]],
                                        interrupter: SignallingRef[F, Boolean],
                                        maxCount: Long,
                                        partitionBy: Seq[String],
                                        schema: MessageType,
                                        encode: T => F[RowParquetRecord]
                                      )(implicit F: Sync[F]) {

    private def writePull(entity: T): Pull[F, T, Unit] =
      Pull.eval(write(entity)) >> Pull.output1(entity)

    private def newFileName: String = {
      val compressionExtension = options.compressionCodecName.getExtension
      UUID.randomUUID().toString + compressionExtension + ".parquet"
    }

    private def getOrCreateWriter(path: Path): F[RecordWriter[F]] = {
      writersRef.access.flatMap { case (writers, setter) =>
        writers.get(path) match {
          case Some(writer) =>
            F.pure(writer)
          case None =>
            RecordWriter(new Path(path, newFileName), schema, options).flatMap {
              writer =>
                setter(writers.updated(path, writer)).flatMap {
                  case true =>
                    F.pure(writer)
                  case false =>
                    writer.dispose >> F.raiseError(new RuntimeException(
                      "Concurrent writers access, probably due to abrupt stream termination"
                    ))
                }
            }
        }
      }
    }

    private def write(entity: T): F[Unit] =
      for {
        record <- encode(entity)
        path <- Stream.iterable(partitionBy).evalMap(partition(record)).fold(basePath) {
          case (path, (partitionName, partitionValue)) => new Path(path, s"$partitionName=$partitionValue")
        }.compile.lastOrError
        writer <- getOrCreateWriter(path)
        _ <- writer.write(record)
      } yield ()

    private def partition(record: RowParquetRecord)(partitionPath: String): F[(String, String)] =
      record.remove(partitionPath) match {
        case None => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' does not exist."))
        case Some(NullValue) => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' is null."))
        case Some(BinaryValue(binary)) => F.delay(partitionPath -> binary.toStringUsingUTF8)
        case _ => F.raiseError(new IllegalArgumentException("Only String field can be used for partitioning."))
      }

    private def dispose: F[Unit] =
      for {
        writers <- writersRef.modify(writers => (Map.empty, writers.values))
        _ <- Stream.iterable(writers).evalMap(_.dispose).compile.drain
      } yield ()

    private def rotatePull(reason: String): Pull[F, T, Unit] =
      Pull.eval(F.delay(println(s"\nRotating on $reason"))) >> Pull.eval(dispose)

    private def writeAllPull(in: Stream[F, WriterEvent], count: Long): Pull[F, T, Unit] = {
      in.pull.uncons1.flatMap {
        case Some((DataEvent(data: T), tail)) if count < maxCount =>
          writePull(data) >> writeAllPull(tail, count + 1)
        case Some((DataEvent(data: T), tail)) =>
          writePull(data) >> rotatePull("max count reached") >> writeAllPull(tail, count = 0)
        case Some((RotateEvent(tick), tail)) =>
          rotatePull(s"tick $tick") >> writeAllPull(tail, count = 0)
        case Some((StopEvent, _)) =>
          Pull.eval(interrupter.set(true))
        case None =>
          Pull.done
      }
    }

    def writeAll(in: Stream[F, WriterEvent]): Stream[F, T] =
      writeAllPull(in, count = 0).stream.onFinalize(dispose)
  }

  def write[F[_] : Timer : Concurrent, T: ParquetRecordEncoder : SkippingParquetSchemaResolver](
                                                                                                path: String,
                                                                                                maxCount: Long,
                                                                                                maxDuration: FiniteDuration,
                                                                                                partitionBy: Seq[String],
                                                                                                options: ParquetWriter.Options
                                                                                              )(implicit F: Sync[F]): Pipe[F, T, T] =
    in =>
      for {
        schema <- Stream.eval(Sync[F].delay(SkippingParquetSchemaResolver.resolveSchema[T](partitionBy)))
        valueCodecConfiguration <- Stream.eval(F.delay(options.toValueCodecConfiguration))
        encode = { (entity: T) => F.delay(ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration)) }
        signal <- Stream.eval(SignallingRef[F, Boolean](false))
        inQueue <- Stream.eval(Queue.bounded[F, WriterEvent](options.rowGroupSize))
        outQueue <- Stream.eval(Queue.unbounded[F, T])
        rotatingWriter <- Stream.emit(
          new RotatingWriter[T, F](new Path(path), options, Ref.unsafe(Map.empty), signal, maxCount, partitionBy, schema, encode)
        )
        out <- Stream(
          Stream.awakeEvery[F](maxDuration).map[WriterEvent](RotateEvent.apply).through(inQueue.enqueue).interruptWhen(signal),
          in.map[WriterEvent](DataEvent.apply).append(Stream.emit(StopEvent)).through(inQueue.enqueue),
          rotatingWriter.writeAll(inQueue.dequeue).through(outQueue.enqueue)
        ).parJoin(3).drain.mergeHaltL(outQueue.dequeue)
      } yield out

}