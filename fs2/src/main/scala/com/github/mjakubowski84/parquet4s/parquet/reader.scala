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

    for {
      hadoopPath <- Stream.eval(io.makePath(path))
      vcc <- Stream.eval(F.delay(options.toValueCodecConfiguration))
      decode = (record: RowParquetRecord) => F.delay(ParquetRecordDecoder.decode(record, vcc))
      reader <- Stream.resource(readerResource(blocker, hadoopPath, options, filter))
      entity <- Stream.unfoldEval(reader) { r =>
        blocker.delay(r.read()).map(record => Option(record).map((_, r)))
      }.evalMap(decode)
    } yield entity
  }

  private def readerResource[F[_]: Sync: ContextShift](blocker: Blocker,
                                                       path: Path,
                                                       options: ParquetReader.Options,
                                                       filter: Filter
                                                      ): Resource[F, HadoopParquetReader[RowParquetRecord]] =
    Resource.fromAutoCloseableBlocking(blocker)(
      Sync[F].delay(
        HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), path)
          .withConf(options.hadoopConf)
          .withFilter(filter.toFilterCompat(options.toValueCodecConfiguration))
          .build()
      )
    )

}
