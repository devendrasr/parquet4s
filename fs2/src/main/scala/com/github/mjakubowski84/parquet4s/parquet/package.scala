package com.github.mjakubowski84.parquet4s

import cats.effect.{Blocker, Concurrent, ContextShift, Sync, Timer}
import fs2.{Pipe, Stream}

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

package object parquet {

  def read[T: ParquetRecordDecoder, F[_]: Sync: ContextShift](blocker: Blocker,
                                                              path: String,
                                                              options: ParquetReader.Options = ParquetReader.Options(),
                                                              filter: Filter = Filter.noopFilter
                                                             ): Stream[F, T] =
    reader.read(blocker, path, options, filter)


  def writeSingleFile[T : ParquetRecordEncoder : ParquetSchemaResolver, F[_]: Sync: ContextShift](blocker: Blocker,
                                                                                                  path: String,
                                                                                                  options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                                 ): Pipe[F, T, Unit] =
    writer.write(blocker, path, options)

  def viaParquet[T : ParquetRecordEncoder : SkippingParquetSchemaResolver, F[_]: Sync : Timer : Concurrent: ContextShift](blocker: Blocker,
                                                                                                                          path: String,
                                                                                                                          maxDuration: FiniteDuration,
                                                                                                                          maxCount: Long,
                                                                                                                          partitionBy: Seq[String] = Seq.empty,
                                                                                                                          options: ParquetWriter.Options = ParquetWriter.Options()
                                                                                                                         ): Pipe[F, T, T] =
    rotatingWriter.write(blocker, path, maxCount, maxDuration, partitionBy, options)

}
