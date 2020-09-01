package com.github.mjakubowski84.parquet4s.fs2

import java.nio.file.Files
import java.sql.Timestamp
import java.util.UUID

import cats.data.State
import cats.effect.{Blocker, ExitCode, IO, IOApp, Sync}
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.github.mjakubowski84.parquet4s.parquet._
import fs2.kafka.{ProducerSettings, _}
import fs2.{Pipe, Stream}
import net.manub.embeddedkafka.{EmbeddedK, EmbeddedKafka}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Random

object ExampleApp extends IOApp {

  private type Record = CommittableConsumerRecord[IO, String, String]

  private sealed trait Fluctuation {
    def delay: FiniteDuration
  }
  private case class Up(delay: FiniteDuration) extends Fluctuation
  private case class Down(delay: FiniteDuration) extends Fluctuation

  val MinDelay: FiniteDuration = 1.milli
  val MaxDelay: FiniteDuration = 500.millis
  val StartDelay: FiniteDuration = 100.millis
  private val words = Seq("Example", "how", "to", "setup", "indefinite", "stream", "with", "Parquet", "writer")
  private def nextWord: String = words(Random.nextInt(words.size - 1))
  private val path = Files.createTempDirectory("example").toString
  private val writerOptions = ParquetWriter.Options(compressionCodecName = CompressionCodecName.SNAPPY)

  case class Data(
//                   year: String,
//                   month: String,
//                   day: String,
                   timestamp: Option[Timestamp],
                   word: String
                 )


  private val fluctuate: State[Fluctuation, Unit] = State[Fluctuation, Unit] { fluctuation =>
    val rate = Random.nextFloat() / 10.0f
    val step = (fluctuation.delay.toMillis * rate).millis
    val newFluctuation: Fluctuation = fluctuation match {
      case Up(delay) if delay + step < MaxDelay =>
        Up(delay + step)
      case Up(delay) =>
        Down(delay - step)
      case Down(delay) if delay - step > MinDelay =>
        Down(delay - step)
      case Down(delay) =>
        Up(delay + step)
    }
    (newFluctuation, ())
  }

  private val Init: Fluctuation = Down(StartDelay)
  val MaxChunkSize: Int = 128
  val ChunkWriteTimeWindow: FiniteDuration = 10.seconds

  private def write(blocker: Blocker): Pipe[IO, Record, fs2.INothing] =
    _
      .map(record => Data(record.record.timestamp.createTime.map(l => new Timestamp(l)), record.record.value))
      .through(viaParquet[IO, Data]
        .options(writerOptions)
        .maxCount(MaxChunkSize)
        .maxDuration(ChunkWriteTimeWindow)
        .write(blocker, path))
      .drain

  private def pass: Pipe[IO, Record, Record] = identity

  override def run(args: List[String]): IO[ExitCode] = {

    val stream = for {
      blocker <- Stream.resource(Blocker[IO])
      kafkaPort <- Stream
        .bracket(blocker.delay[IO, EmbeddedK](EmbeddedKafka.start()))(_ => blocker.delay[IO, Unit](EmbeddedKafka.stop()))
        .map(_.config.kafkaPort)
      producerSettings = ProducerSettings[IO, String, String].withBootstrapServers(s"localhost:$kafkaPort")
      consumerSettings = ConsumerSettings[IO, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(s"localhost:$kafkaPort")
        .withGroupId("group")
      _ <- Stream(
        Stream
          .iterate(fluctuate.runS(Init).value)(f => fluctuate.runS(f).value)
          .flatMap(fluctuation => Stream.sleep_(fluctuation.delay) ++ Stream.emit(nextWord))
          .map(word => ProducerRecord(topic = "topic", key = UUID.randomUUID().toString, value = word))
          .map(ProducerRecords.one[String, String])
          .through(produce(producerSettings))
          .drain,
        consumerStream[IO]
          .using(consumerSettings)
          .evalTap(_.subscribeTo("topic"))
          .flatMap(_.stream)
          .broadcastThrough(write(blocker), pass)
          .map(_.offset)
          .through(commitBatchWithin(MaxChunkSize, ChunkWriteTimeWindow))
          .drain
      ).parJoin(2)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)

  }
}
