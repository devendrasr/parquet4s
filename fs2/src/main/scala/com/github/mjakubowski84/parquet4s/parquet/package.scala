package com.github.mjakubowski84.parquet4s

import java.util.UUID

import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import fs2.concurrent.{Queue, SignallingRef}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.higherKinds

package object parquet {

  private class Writer[T, F[_]](internalWriter: ParquetWriter.InternalWriter, encode: T => RowParquetRecord)
                               (implicit F: Sync[F]) extends AutoCloseable {

    def write(elem: T): F[Writer[T, F]] =
      for {
        _ <- F.delay(print("."))
        record <- F.delay(encode(elem))
        _ <- F.delay(internalWriter.write(record))
      } yield this

    def writePull(chunk: Chunk[T]): Pull[F, Nothing, Writer[T, F]] =
      Pull.eval(chunk.foldM(this)(_.write(_)))

    // TODO test what is faster, writing chunks using unconsNonEmpty or one by one using uncons1
    def writeAll(in: Stream[F, T]): Pull[F, Nothing, Writer[T, F]] = {
      in.pull.unconsNonEmpty.flatMap {
        case Some((chunk, tail)) => writePull(chunk).flatMap(_.writeAll(tail))
        case None                => Pull.pure(this)
      }
    }

    override def close(): Unit = {
      println("\nClosing!")
      internalWriter.close()
    }
  }

  private def writerResource[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]](path: String, options: ParquetWriter.Options)
                                                                                    (implicit F: Sync[F]): Resource[F, Writer[T, F]] =
      Resource.fromAutoCloseable(
        for {
          valueCodecConfiguration <- F.delay(options.toValueCodecConfiguration)
          schema <- F.delay(ParquetSchemaResolver.resolveSchema[T])
          internalWriter <- F.delay(ParquetWriter.internalWriter(new Path(path), schema, options))
          encode = { (entity: T) => ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration) }
        } yield new Writer[T, F](internalWriter, encode)
      )

  def writeSingleFile[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync](path: String,
                                                                                    options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                   ): Pipe[F, T, Unit] =
    in =>
      Stream
        .resource(writerResource[T, F](path, options))
        .flatMap(_.writeAll(in).void.stream)

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

  def read[T: ParquetRecordDecoder, F[_]: Sync](
                                                 path: String,
                                                 options: ParquetReader.Options = ParquetReader.Options(),
                                                 filter: Filter = Filter.noopFilter
                                   ): Stream[F, T] = {
    val vcc = options.toValueCodecConfiguration
    val decode = (record: RowParquetRecord) => Sync[F].delay(ParquetRecordDecoder.decode(record, vcc))
    Stream.resource(readerResource(path, options, filter)).flatMap { reader =>
      Stream.unfoldEval(reader) { r =>
        Sync[F].delay(r.read()).map(record => Option(record).map((_, r)))
      }.evalMap(decode)
    }
  }

  private sealed trait WriterEvent
  private case class DataEvent[T](data: T) extends WriterEvent
  private case class RotateEvent(tick: FiniteDuration) extends WriterEvent
  private case object StopEvent extends WriterEvent

  def viaParquet[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync : Timer : Concurrent](path: String,
                                                                                                    options: ParquetWriter.Options = ParquetWriter.Options(),
                                                                                                    maxDuration: FiniteDuration,
                                                                                                    maxCount: Long
                                                                              ): Pipe[F, T, Unit] = {
    in =>
      for {
        signal <- Stream.eval(SignallingRef[F, Boolean](false))
        queue <- Stream.eval(Queue.bounded[F, WriterEvent](1))
        rotatingWriter <- Stream.eval(rotatingWriter[T, F](path, options, maxCount, signal))
        _ <- Stream(
          Stream.awakeEvery[F](maxDuration).map[WriterEvent](RotateEvent.apply).through(queue.enqueue).interruptWhen(signal),
          in.map[WriterEvent](DataEvent.apply).append(Stream.emit(StopEvent)).through(queue.enqueue),
          rotatingWriter.writeAll(queue.dequeue, count = 0).void.stream
        ).parJoin(3)
      } yield ()

  }

  private class RotatingWriter[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]](
                                                                                        path: String,
                                                                                        options: ParquetWriter.Options,
                                                                                        writer: Writer[T, F],
                                                                                        disposeWriter: F[Unit],
                                                                                        interrupter: SignallingRef[F, Boolean],
                                                                                        maxCount: Long
                                                                                      )(implicit F: Sync[F]) {

    private def writePull(entity: T): Pull[F, Nothing, Unit] = Pull.eval(writer.write(entity).void)

    private def disposePull: Pull[F, Nothing, Unit] = Pull.eval(disposeWriter)

    private def recreateWriterPull(tick: FiniteDuration): Pull[F, Nothing, RotatingWriter[T, F]] =
      for {
        _ <- Pull.eval(F.delay(println(s"\nRotating on ${tick.toMillis}")))
        _ <- disposePull
        newWriter <- Stream.eval(rotatingWriter[T, F](path, options, maxCount, interrupter)).pull.headOrError
      } yield newWriter

    def writeAll(in: Stream[F, WriterEvent], count: Long): Pull[F, Nothing, Unit] = {
      in.pull.uncons1.flatMap {
        case Some((DataEvent(data: T), tail)) if count < maxCount =>
          writePull(data).flatMap(_ => writeAll(tail, count + 1))
        case Some((DataEvent(data: T), tail)) =>
          writePull(data).flatMap(_ => recreateWriterPull(Duration.Zero).flatMap(_.writeAll(tail, count = 0)))
        case Some((RotateEvent(tick), tail)) =>
          recreateWriterPull(tick).flatMap(_.writeAll(tail, count = 0))
        case Some((StopEvent, _)) =>
          disposePull.flatMap(_ => Pull.eval(interrupter.set(true)))
        case None =>
          disposePull
      }
    }

  }

  private def rotatingWriter[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]](
                                                                                              path: String,
                                                                                              options: ParquetWriter.Options,
                                                                                              maxCount: Long,
                                                                                              interrupter: SignallingRef[F, Boolean]
                                                                                            )(implicit F: Sync[F]): F[RotatingWriter[T, F]] = {
    val compressionExtension = options.compressionCodecName.getExtension
    val fileName = UUID.randomUUID().toString + compressionExtension + ".parquet"
    // TODO no path ops on string
    writerResource(s"$path/$fileName", options)
      .allocated
      .map { case (writer, disposeWriter) =>
        new RotatingWriter[T, F](path, options, writer, disposeWriter, interrupter, maxCount)
      }
  }

}
