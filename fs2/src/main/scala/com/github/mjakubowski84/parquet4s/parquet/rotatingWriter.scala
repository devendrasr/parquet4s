package com.github.mjakubowski84.parquet4s.parquet

import java.util.{ConcurrentModificationException, UUID}

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Concurrent, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.mjakubowski84.parquet4s._
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import fs2.concurrent.{Queue, SignallingRef}
import fs2.{Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.MessageType

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object rotatingWriter {

  private sealed trait WriterEvent[T, W]
  private case class DataEvent[T, W](data: W) extends WriterEvent[T, W]
  private case class RotateEvent[T, W]() extends WriterEvent[T, W]
  private case class OutputEvent[T, W](out: T) extends WriterEvent[T, W]
  private case class StopEvent[T, W]() extends WriterEvent[T, W]

  private object RecordWriter {
    def apply[F[_]: Sync: ContextShift](blocker: Blocker,
                                        path: Path,
                                        schema: MessageType,
                                        options: ParquetWriter.Options
                                       ): F[RecordWriter[F]] =
      blocker.delay(ParquetWriter.internalWriter(path, schema, options)).map(iw => new RecordWriter(blocker, iw))
  }

  private class RecordWriter[F[_]: Sync: ContextShift](blocker: Blocker, internalWriter: ParquetWriter.InternalWriter) {

    def write(record: RowParquetRecord): F[Unit] = blocker.delay(internalWriter.write(record))

    def dispose: F[Unit] = blocker.delay(internalWriter.close())

  }

  private class RotatingWriter[T, W, F[_]](blocker: Blocker,
                                        basePath: Path,
                                        options: ParquetWriter.Options,
                                        writersRef: Ref[F, Map[Path, RecordWriter[F]]],
                                        interrupter: SignallingRef[F, Boolean],
                                        maxCount: Long,
                                        partitionBy: Seq[String],
                                        schema: MessageType,
                                        encode: W => F[RowParquetRecord],
                                        logger: Logger[F]
                                      )(implicit F: Sync[F], cs: ContextShift[F]) {

    private def writePull(entity: W): Pull[F, T, Unit] =
      Pull.eval(write(entity)) >> Pull.eval(F.delay(println(s"Just wrote: $entity"))) // TODO remove me

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
            RecordWriter(blocker, new Path(path, newFileName), schema, options).flatMap {
              writer =>
                setter(writers.updated(path, writer)).flatMap {
                  case true =>
                    F.pure(writer)
                  case false =>
                    writer.dispose >> F.raiseError[RecordWriter[F]](new ConcurrentModificationException(
                      "Concurrent writers access, probably due to abrupt stream termination"
                    ))
                }
            }
        }
      }
    }

    private def write(entity: W): F[Unit] =
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
      Pull.eval(logger.debug(s"Rotating on $reason")) >> Pull.eval(dispose)

    private def writeAllPull(in: Stream[F, WriterEvent[T, W]], count: Long): Pull[F, T, Unit] = {
      in.pull.uncons1.flatMap {
        case Some((DataEvent(data), tail)) if count < maxCount =>
          writePull(data) >> writeAllPull(tail, count + 1)
        case Some((DataEvent(data), tail)) =>
          writePull(data) >> rotatePull("max count reached") >> writeAllPull(tail, count = 0)
        case Some((RotateEvent(), tail)) =>
          rotatePull(s"write timeout") >> writeAllPull(tail, count = 0)
        case Some((OutputEvent(out), tail)) =>
          Pull.output1(out) >> writeAllPull(tail, count)
        case Some((StopEvent(), _)) =>
          Pull.eval(interrupter.set(true))
        case None =>
          Pull.done
      }
    }

    def writeAll(in: Stream[F, WriterEvent[T, W]]): Stream[F, T] =
      writeAllPull(in, count = 0).stream.onFinalize(dispose)
  }

  def write[F[_] : Timer : Concurrent: ContextShift, T, W: ParquetRecordEncoder : SkippingParquetSchemaResolver](blocker: Blocker,
                                                                                                path: String,
                                                                                                maxCount: Long,
                                                                                                maxDuration: FiniteDuration,
                                                                                                partitionBy: Seq[String],
                                                                                                prewriteTransformation: T => Stream[F, W],
                                                                                                options: ParquetWriter.Options
                                                                                               )(implicit F: Sync[F]): Pipe[F, T, T] =
    in =>
      for {
        hadoopPath <- Stream.eval(io.makePath(path))
        schema <- Stream.eval(F.delay(SkippingParquetSchemaResolver.resolveSchema[W](partitionBy)))
        valueCodecConfiguration <- Stream.eval(F.delay(options.toValueCodecConfiguration))
        encode = { (entity: W) => F.delay(ParquetRecordEncoder.encode[W](entity, valueCodecConfiguration)) }
        signal <- Stream.eval(SignallingRef[F, Boolean](false))
        eventQueue <- Stream.eval(Queue.bounded[F, WriterEvent[T, W]](options.pageSize))
        logger <- Stream.eval(logger[F](this.getClass))
        rotatingWriter <- Stream.emit(
          new RotatingWriter[T, W, F](blocker, hadoopPath, options, Ref.unsafe(Map.empty), signal, maxCount, partitionBy,
            schema, encode, logger)
        )
        out <- Stream(
          Stream.awakeEvery[F](maxDuration)
            .map(_ => RotateEvent[T, W]())
            .through(eventQueue.enqueue)
            .interruptWhen(signal)
            .drain,
          in
            .flatMap(inputElement => prewriteTransformation(inputElement).map(DataEvent.apply[T, W]).append(Stream.emit(OutputEvent[T, W](inputElement))))
            .append(Stream.emit(StopEvent[T, W]()))
            .through(eventQueue.enqueue)
            .drain,
          rotatingWriter.writeAll(eventQueue.dequeue)
        ).parJoin(maxOpen = 3)
      } yield out

}
