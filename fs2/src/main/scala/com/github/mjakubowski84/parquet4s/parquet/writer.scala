package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Resource, Sync}
import com.github.mjakubowski84.parquet4s.{ParquetRecordEncoder, ParquetSchemaResolver, ParquetWriter, RowParquetRecord}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.MessageType
import cats.implicits._

import scala.language.higherKinds

private[parquet4s] object writer {

  class Writer[T, F[_]](internalWriter: ParquetWriter.InternalWriter, encode: T => F[RowParquetRecord])
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

  def write[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync](path: String,
                                                                          options: ParquetWriter.Options
                                                                         ): Pipe[F, T, Unit] =
    in =>
      Stream
        .resource(writerResource[T, F](new Path(path), options))
        .flatMap(_.writeAll(in).stream)

  def writerResource[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]](path: Path, options: ParquetWriter.Options)
                                                                                    (implicit F: Sync[F]): Resource[F, Writer[T, F]] =
    Resource.fromAutoCloseable(
      for {
        schema <- F.delay(ParquetSchemaResolver.resolveSchema[T])
        valueCodecConfiguration <- F.delay(options.toValueCodecConfiguration)
        encode = { (entity: T) => F.delay(ParquetRecordEncoder.encode[T](entity, valueCodecConfiguration)) }
        internalWriter <- F.delay(ParquetWriter.internalWriter(path, schema, options))
      } yield new Writer[T, F](internalWriter, encode)
    )

  def writerResource[T, F[_]](path: Path,
                                      schema: MessageType,
                                      encode: T => F[RowParquetRecord],
                                      options: ParquetWriter.Options)
                                     (implicit F: Sync[F]): Resource[F, Writer[T, F]] =
    Resource.fromAutoCloseable(
      F.delay(ParquetWriter.internalWriter(path, schema, options))
        .map(internalWriter => new Writer[T, F](internalWriter, encode))
    )

}
