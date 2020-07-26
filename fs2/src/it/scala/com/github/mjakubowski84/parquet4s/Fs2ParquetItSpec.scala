package com.github.mjakubowski84.parquet4s

import java.nio.file.{FileVisitOption, Paths}

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.github.mjakubowski84.parquet4s.Fs2ParquetItSpec.{Data, DataPartitioned}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.{BeforeAndAfter, Inspectors}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.compat.immutable.LazyList
import scala.util.Random
import fs2.Stream
import fs2.io.file.{directoryStream, walk}

import scala.concurrent.duration._

object Fs2ParquetItSpec {

  case class Data(i: Long, s: String)

  case class DataPartitioned(i: Long, s: String, a: String, b: String)

}

class Fs2ParquetItSpec extends AsyncFlatSpec with Matchers with TestUtils with BeforeAndAfter with Inspectors {

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
  val dictS: Seq[String] = Vector("a", "b", "c", "d")
  val dictA: Seq[String] = Vector("1", "2", "3")
  val dictB: Seq[String] = Vector("x", "y", "z")
  val data: LazyList[Data] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i => Data(i = i, s = dictS(Random.nextInt(4))))
  val dataPartitioned: LazyList[DataPartitioned] = LazyList
    .range(start = 0L, end = count, step = 1L)
    .map(i => DataPartitioned(
      i = i,
      s = dictS(Random.nextInt(4)),
      a = dictA(Random.nextInt(3)),
      b = dictB(Random.nextInt(3))
    ))


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
      .through(parquet.viaParquet[Data, IO](tempPathString, maxDuration = 1.minute, maxCount = writeOptions.rowGroupSize, options = writeOptions))
      .compile
      .toVector

    val readIO = parquet.read[Data, IO](tempPathString).compile.toVector

    val listParquetFilesIO = Stream.resource(Blocker[IO])
      .flatMap(blocker => directoryStream[IO](blocker, Paths.get(tempPath.toUri)))
      .filter(_.toString.endsWith(".parquet"))
      .compile
      .toVector

    val testIO = for {
      writtenData <- writeIO
      readData <- readIO
      parquetFiles <- listParquetFilesIO
    } yield {
      writtenData should contain theSameElementsAs data
      readData should contain theSameElementsAs data
      parquetFiles should have size 4
    }

    testIO.unsafeToFuture()
  }

  it should "write files and rotate by max write duration" in {
    val writeIO = Stream
      .iterable(data)
      .through(parquet.viaParquet[Data, IO](tempPathString, maxDuration = 25.millis, maxCount = count, options = writeOptions))
      .compile
      .toVector

    val readIO = parquet.read[Data, IO](tempPathString).compile.toVector

    val listParquetFilesIO = Stream.resource(Blocker[IO])
      .flatMap(blocker => directoryStream[IO](blocker, Paths.get(tempPath.toUri)))
      .filter(_.toString.endsWith(".parquet"))
      .compile
      .toVector

    val testIO = for {
      writtenData <- writeIO
      readData <- readIO
      parquetFiles <- listParquetFilesIO
    } yield {
      writtenData should contain theSameElementsAs data
      readData should contain theSameElementsAs data
      parquetFiles.size should be > 1
    }

    testIO.unsafeToFuture()
  }


  it should "partition written files" in {
    val writeIO = Stream
      .iterable(dataPartitioned)
      .through(parquet.viaParquet[DataPartitioned, IO](
        tempPathString,
        maxDuration = 1.minute,
        maxCount = count,
        partitionBy = List("a", "b"),
        options = writeOptions
      ))
      .compile
      .toVector

//    val readIO = parquet.read[Data, IO](tempPathString).compile.toVector

    val listParquetFilesIO = Stream.resource(Blocker[IO])
      .flatMap(blocker => walk[IO](blocker, Paths.get(tempPath.toUri)))
      .filter(_.toString.endsWith(".parquet"))
      .compile
      .toVector

    def partitionValue(path: java.nio.file.Path): (String, String) = {
      val split = path.getFileName.toString.split("=")
      (split(0), split(1))
    }

    val testIO = for {
      writtenData <- writeIO
      parquetFiles <- listParquetFilesIO
    } yield {
      writtenData should contain theSameElementsAs dataPartitioned
      parquetFiles should not be empty
      val partitions = parquetFiles.map { path =>
        (partitionValue(path.getParent.getParent), partitionValue(path.getParent))
      }
      forEvery(partitions) { case (("a", aVal), ("b", bVal)) =>
        dictA should contain(aVal)
        dictB should contain(bVal)
      }
    }

    testIO.unsafeToFuture()
  }

}
