package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.github.mjakubowski84.parquet4s._
import fs2.Stream
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}

import scala.language.higherKinds

private[parquet4s] object reader {

  def read[F[_]: ContextShift, T: ParquetRecordDecoder](blocker: Blocker,
                                                        path: String,
                                                        options: ParquetReader.Options,
                                                        filter: Filter)
                                                       (implicit F: Sync[F]): Stream[F, T] = {
    val vcc = options.toValueCodecConfiguration
    val decode = (record: RowParquetRecord) => F.delay(ParquetRecordDecoder.decode(record, vcc))
    Stream.resource(readerResource(blocker, path, options, filter)).flatMap { reader =>
      Stream.unfoldEval(reader) { r =>
        blocker.delay(r.read()).map(record => Option(record).map((_, r)))
      }.evalMap(decode)
    }
  }

  private def readerResource[F[_]: Sync: ContextShift](blocker: Blocker,
                                                       path: String,
                                                       options: ParquetReader.Options,
                                                       filter: Filter
                                                      ): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseable(
      blocker.delay(
        HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), new Path(path))
          .withConf(options.hadoopConf)
          .withFilter(filter.toFilterCompat(options.toValueCodecConfiguration))
          .build()
      )
    )

}
