package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import com.github.mjakubowski84.parquet4s.{ParquetWriter, PartitionedDirectory, PartitionedPath}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException
import org.apache.parquet.hadoop.ParquetFileWriter
import cats.implicits._
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import org.apache.hadoop.conf.Configuration

import scala.language.higherKinds
import scala.util.matching.Regex

private[parquet] object io {

  private type Partition = (String, String)

  private val PartitionRegexp: Regex = """([a-zA-Z0-9._]+)=([a-zA-Z0-9._]+)""".r

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

  def findPartitionedPaths[F[_]: ContextShift](blocker: Blocker,
                                               path: Path,
                                               configuration: Configuration)
                                              (implicit F: Sync[F]): F[PartitionedDirectory] =
    Resource.fromAutoCloseableBlocking(blocker)(F.delay(path.getFileSystem(configuration))).use { fs =>
      blocker.blockOn(
        F.fromEither(
          findPartitionedPaths(fs, path, List.empty).fold(
            PartitionedDirectory.failed,
            PartitionedDirectory.apply
          )
        )
      )
    }

  private def findPartitionedPaths(fs: FileSystem,
                                   path: Path,
                                   partitions: List[Partition]): Either[List[Path], List[PartitionedPath]] = {
    val (dirs, files) = fs.listStatus(path).toList.partition(_.isDirectory)
    if (dirs.nonEmpty && files.nonEmpty)
      Left(path :: Nil) // path is invalid because it contains both dirs and files
    else {
      val partitionedDirs = dirs.flatMap(matchPartition)
      if (partitionedDirs.isEmpty)
        Right(List(PartitionedPath(path, partitions))) // leaf dir
      else
        partitionedDirs
          .map { case (subPath, partition) => findPartitionedPaths(fs, subPath, partitions :+ partition) }
          .foldLeft[Either[List[Path], List[PartitionedPath]]](Right(List.empty)) {
            case (Left(invalidPaths), Left(moreInvalidPaths)) =>
              Left(invalidPaths ++ moreInvalidPaths)
            case (Right(partitionedPaths), Right(morePartitionedPaths)) =>
              Right(partitionedPaths ++ morePartitionedPaths)
            case (left: Left[_, _], _) =>
              left
            case (_, left: Left[_, _]) =>
              left
          }
    }
  }

  private def matchPartition(fileStatus: FileStatus): Option[(Path, Partition)] = {
    val path = fileStatus.getPath
    path.getName match {
      case PartitionRegexp(name, value) => Some(path, (name, value))
      case _                            => None
    }
  }

}
