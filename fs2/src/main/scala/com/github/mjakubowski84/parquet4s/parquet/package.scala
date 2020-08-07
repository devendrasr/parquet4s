package com.github.mjakubowski84.parquet4s

import cats.effect.{Concurrent, Sync, Timer}
import fs2.{Pipe, Stream}

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

package object parquet {

  def read[T: ParquetRecordDecoder, F[_]: Sync](
                                                 path: String,
                                                 options: ParquetReader.Options = ParquetReader.Options(),
                                                 filter: Filter = Filter.noopFilter
                                               ): Stream[F, T] =
    reader.read(path, options, filter)



  def writeSingleFile[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync](path: String,
                                                                                    options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                   ): Pipe[F, T, Unit] =
    writer.write(path, options)

  def viaParquet[T : SkippingParquetRecordEncoder : SkippingParquetSchemaResolver: PartitionLens, F[_]: Sync : Timer : Concurrent](
                                                                                                                                    path: String,
                                                                                                                   maxDuration: FiniteDuration,
                                                                                                                   maxCount: Long,
                                                                                                                   partitionBy: Seq[String] = Seq.empty,
                                                                                                                   options: ParquetWriter.Options = ParquetWriter.Options()
  ): Pipe[F, T, T] =
    rotatingWriter.write(path, maxCount, maxDuration, partitionBy, options)

}
