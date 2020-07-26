package com.github.mjakubowski84.parquet4s

import java.util.UUID

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import fs2.concurrent.{Queue, SignallingRef}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

package object parquet {

  private class Writer[T, F[_]](internalWriter: ParquetWriter.InternalWriter, encode: T => F[RowParquetRecord])
                               (implicit F: Sync[F]) extends AutoCloseable {

    def write(elem: T): F[Unit] =
      for {
        _ <- F.delay(print("."))
        record <- encode(elem)
        _ <- F.delay(internalWriter.write(record))
      } yield ()

    def writePull(chunk: Chunk[T]): Pull[F, Nothing, Unit] =
      Pull.eval(chunk.traverse_(write))

    def writeAll(in: Stream[F, T]): Pull[F, Nothing, Unit] =
      in.pull.unconsNonEmpty.flatMap {
        case Some((chunk, tail)) => writePull(chunk) >> writeAll(tail)
        case None                => Pull.done
      }

    override def close(): Unit = {
      println("\nClosing!")
      internalWriter.close()
    }
  }

  private def writerResource[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]](path: Path, options: ParquetWriter.Options)
                                                                                    (implicit F: Sync[F]): Resource[F, Writer[T, F]] =
      Resource.fromAutoCloseable(
        for {
          valueCodecConfiguration <- F.delay(options.toValueCodecConfiguration)
          schema <- F.delay(ParquetSchemaResolver.resolveSchema[T])
          internalWriter <- F.delay(ParquetWriter.internalWriter(path, schema, options))
          encode = { (entity: T) => F.delay(ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration)) }
        } yield new Writer[T, F](internalWriter, encode)
      )

  def writeSingleFile[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync](path: String,
                                                                                    options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                   ): Pipe[F, T, Unit] =
    in =>
      Stream
        .resource(writerResource[T, F](new Path(path), options))
        .flatMap(_.writeAll(in).stream)

  private def readerResource[F[_]](path: String,
                                   options: ParquetReader.Options,
                                   filter: Filter
                                  )(implicit F: Sync[F]): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseable(
      F.delay(
        HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path))
          .withConf(options.hadoopConf)
          .withFilter(filter.toFilterCompat(options.toValueCodecConfiguration))
          .build())
    )

  def read[T: ParquetRecordDecoder, F[_]](
                                           path: String,
                                           options: ParquetReader.Options = ParquetReader.Options(),
                                           filter: Filter = Filter.noopFilter
                                          )(implicit F: Sync[F]): Stream[F, T] = {
    val vcc = options.toValueCodecConfiguration
    val decode = (record: RowParquetRecord) => F.delay(ParquetRecordDecoder.decode(record, vcc))
    Stream.resource(readerResource(path, options, filter)).flatMap { reader =>
      Stream.unfoldEval(reader) { r =>
        F.delay(r.read()).map(record => Option(record).map((_, r)))
      }.evalMap(decode)
    }
  }

  private sealed trait WriterEvent
  private case class DataEvent[T](data: T) extends WriterEvent
  private case class RotateEvent(tick: FiniteDuration) extends WriterEvent
  private case object StopEvent extends WriterEvent

  def viaParquet[T : ParquetRecordEncoder : ParquetSchemaResolver: PartitionLens, F[_]: Sync : Timer : Concurrent](path: String,
                                                                                                                   maxDuration: FiniteDuration,
                                                                                                                   maxCount: Long,
                                                                                                                   partitionBy: Seq[String] = Seq.empty,
                                                                                                                   options: ParquetWriter.Options = ParquetWriter.Options()
  ): Pipe[F, T, T] = {
        // TODO schema and all other configs should be resolved once here - not in every writer
    in =>
      for {
        signal <- Stream.eval(SignallingRef[F, Boolean](false))
        inQueue <- Stream.eval(Queue.bounded[F, WriterEvent](maxSize = 1))
        outQueue <- Stream.eval(Queue.unbounded[F, T])
        rotatingWriter <- Stream.emit(new RotatingWriter[T, F](new Path(path), options, Ref.unsafe(Map.empty), signal, maxCount, partitionBy))
        out <- Stream(
          Stream.awakeEvery[F](maxDuration).map[WriterEvent](RotateEvent.apply).through(inQueue.enqueue).interruptWhen(signal),
          in.map[WriterEvent](DataEvent.apply).append(Stream.emit(StopEvent)).through(inQueue.enqueue),
          rotatingWriter.writeAll(inQueue.dequeue).through(outQueue.enqueue)
        ).parJoin(3).drain.mergeHaltL(outQueue.dequeue)
      } yield out

  }

  private class RotatingWriter[T : ParquetRecordEncoder : ParquetSchemaResolver: PartitionLens, F[_]](
                                                                                                       basePath: Path,
                                                                                                       options: ParquetWriter.Options,
                                                                                                       writersRef: Ref[F, Map[Path, Writer[T, F]]],
                                                                                                       interrupter: SignallingRef[F, Boolean],
                                                                                                       maxCount: Long,
                                                                                                       partitionBy: Seq[String]
                                                                                                     )(implicit F: Sync[F]) {

    private def writePull(entity: T): Pull[F, T, Unit] =
      Pull.eval(write(entity)) >> Pull.output1(entity)

    private def newFileName: String = {
        val compressionExtension = options.compressionCodecName.getExtension
        UUID.randomUUID().toString + compressionExtension + ".parquet"
    }

    private def getOrCreateWriter(path: Path): F[Writer[T, F]] = {
      writersRef.access.flatMap { case (wx, setter) =>
        wx.get(path) match {
          case Some(writer) =>
            F.pure(writer)
          case None =>
            writerResource[T, F](new Path(path, newFileName), options).allocated.flatMap {
              case (writer, finalizer) =>
                setter(wx.updated(path, writer)).flatMap {
                  case true =>
                    F.pure(writer)
                  case false =>
                    finalizer >> F.raiseError(new RuntimeException(
                      "Concurrent writers finalisation, probably due to abrupt stream termination"
                    ))
                }
            }
        }
      }
    }

    private def write(entity: T): F[Unit] =
      for {
        path <- Stream.iterable(partitionBy).evalMap(pp => F.delay(PartitionLens(entity, pp))).fold(basePath) {
          case (path, (partitionName, partitionValue)) => new Path(path, s"$partitionName=$partitionValue")
        }.compile.lastOrError
        writer <- getOrCreateWriter(path)
        _ <- writer.write(entity)
      } yield ()


    private def dispose(): F[Unit] =
      for {
        writers <- writersRef.modify(writers => (Map.empty, writers.values))
        _ <- Stream.iterable(writers).evalMap(writer => F.delay(writer.close())).compile.drain
      } yield ()

    private def rotatePull(reason: String): Pull[F, T, Unit] =
      Pull.eval(F.delay(println(s"\nRotating on $reason"))) >> Pull.eval(dispose())

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
      writeAllPull(in, count = 0).stream.onFinalize(dispose())
  }

}
