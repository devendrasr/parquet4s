package com.github.mjakubowski84.parquet4s

import java.nio.file.Paths

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.mjakubowski84.parquet4s.Fs2ParquetItSpec.Data
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.compat.immutable.LazyList
import scala.util.Random
import fs2.Stream
import fs2.io.file.directoryStream

import scala.concurrent.duration._

object Fs2ParquetItSpec {

  case class Data(i: Long, s: String)

}

class Fs2ParquetItSpec extends AsyncFlatSpec with Matchers with TestUtils with BeforeAndAfter {

  before {
    clearTemp()
  }

  implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
  implicit val timer: Timer[IO] = IO.timer(executionContext)

  val writeOptions: ParquetWriter.Options = ParquetWriter.Options(
    compressionCodecName = CompressionCodecName.SNAPPY,
    pageSize = 512,
    rowGroupSize = 4 * 512,
    hadoopConf = configuration
  )

  val count: Int = 4 * writeOptions.rowGroupSize
  val dict: Seq[String] = Vector("a", "b", "c", "d")
  val data: LazyList[Data] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i = i, s = dict(Random.nextInt(4))))

  it should "write and read single parquet file" in {
    val outputFileName = "data.parquet"
    val writeIO = Stream
      .iterable(data)
      .through(parquet.writeSingleFile[Data, IO](s"$tempPath/$outputFileName", writeOptions))
      .compile
      .drain

    val readIO = parquet.read[Data, IO](tempPathString).compile.toVector

    val testIO = for {
      _ <- writeIO
      readData <- readIO
    } yield {
      readData should contain theSameElementsInOrderAs data
    }

    testIO.unsafeToFuture()
  }


  it should "write files and rotate by max file size" in {
    val writeIO = Stream
      .iterable(data)
      .through(parquet.viaParquet[Data, IO](tempPathString, writeOptions, maxDuration = 1.minute, maxCount = writeOptions.rowGroupSize))
      .compile
      .drain

    val readIO = parquet.read[Data, IO](tempPathString).compile.toVector

    val listParquetFilesIO = Stream.resource(Blocker[IO])
      .flatMap(blocker => directoryStream[IO](blocker, Paths.get(tempPath.toUri)))
      .filter(_.toString.endsWith(".parquet"))
      .compile
      .toVector

    val testIO = for {
      _ <- writeIO
      readData <- readIO
      parquetFiles <- listParquetFilesIO
    } yield {
      readData should contain theSameElementsAs data
      parquetFiles should have size 4
    }

    testIO.unsafeToFuture()
  }

  it should "write files and rotate by max write duration" in {
    val writeIO = Stream
      .iterable(data)
      .through(parquet.viaParquet[Data, IO](tempPathString, writeOptions, maxDuration = 25.millis, maxCount = count))
      .compile
      .drain

    val readIO = parquet.read[Data, IO](tempPathString).compile.toVector

    val listParquetFilesIO = Stream.resource(Blocker[IO])
      .flatMap(blocker => directoryStream[IO](blocker, Paths.get(tempPath.toUri)))
      .filter(_.toString.endsWith(".parquet"))
      .compile
      .toVector

    val testIO = for {
      _ <- writeIO
      readData <- readIO
      parquetFiles <- listParquetFilesIO
    } yield {
      readData should contain theSameElementsAs data
      parquetFiles.size should be > 1
    }

    testIO.unsafeToFuture()
  }


}
