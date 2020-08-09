package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import com.github.mjakubowski84.parquet4s.ParquetWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import cats.implicits._
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger

import scala.language.higherKinds

private[parquet] object io {

  def makePath[F[_]](path: String)(implicit F: Sync[F]): F[Path] = F.delay(new Path(path))

  def validateWritePath[F[_]: ContextShift](blocker: Blocker,
                                            path: Path,
                                            writeOptions: ParquetWriter.Options,
                                            logger: Logger[F]
                                           )(implicit F: Sync[F]): F[Unit] =
    Resource
      .fromAutoCloseableBlocking(blocker)(F.delay(path.getFileSystem(writeOptions.hadoopConf)))
      .use { fs =>
        blocker.delay(fs.exists(path)).flatMap {
          case true if writeOptions.writeMode == ParquetFileWriter.Mode.CREATE =>
            F.raiseError(new AlreadyExistsException(s"File or directory already exists: $path"))
          case true =>
            logger.debug(s"Deleting $path in order to override with new data.") >>
              blocker.delay(fs.delete(path, true)).void
          case false =>
            F.unit
        }
      }

}
