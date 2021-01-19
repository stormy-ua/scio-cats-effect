package com.spotify.cats.effect.queue

import java.util.concurrent.LinkedBlockingQueue

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{ContextShift, IO}
import org.slf4j.LoggerFactory
import cats._
import cats.implicits._
import cats.effect.implicits._
import scala.concurrent.ExecutionContext.Implicits.global

final class SimpleMultiConsumerBoundedQueue[A](maxPendingItems: Int, parallelismLevel: Int)
  extends MultiConsumerBoundedQueue[IO, A] {

    private val log = LoggerFactory.getLogger(getClass)
    private val queue = new LinkedBlockingQueue[A](maxPendingItems)

    private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
    private val threadId = Thread.currentThread().getId
    private val pendingItemsSemaphore = Semaphore[IO](0).unsafeRunSync()

    def put(element: A): Unit = {
      queue.put(element)
      pendingItemsSemaphore.release.unsafeRunSync()
    }

    def drainAndClose(): Unit = {
      pendingItemsSemaphore.releaseN(parallelismLevel).unsafeRunSync()
    }

    def consume(consumer: A => IO[Unit]): IO[Long] = for {
          counterR <- Ref.of[IO, Int](0)
          _ = log.debug("New consume loop. thread id = [{}]", threadId)
          _ <- contextShift.shift
          consumedCounts <- (0 until parallelismLevel).toList
            .map(id => consume(id, 0, counterR, consumer))
            .parSequence
          consumedCount <- counterR.get
          pendingCount  <- pendingItemsSemaphore.count
          _ = log.debug("[{}] Total number of consumed elements: {} == sum({}), pending count = {}",
            threadId.toString, consumedCount.toString, consumedCounts.toString, pendingCount.toString)
        } yield consumedCount


    private def consume(id: Int,
                        selfCounter: Long,
                        counterR: Ref[IO, Int],
                        consumer: A => IO[Unit]): IO[Long] = for {
      _        <- pendingItemsSemaphore.acquire
      elem     <- IO(Option(queue.poll()))
      cnt      <- elem match {
        case Some(elem) =>
          for {
            res       <- consumer(elem).attempt
            consumedCount <- counterR.updateAndGet(_ + 1)
            _       <- contextShift.shift
            _        = log.debug(
              "[{}-{}] Consumed [{}]. thread id = {}. total consumed={} self consumed={}",
              threadId.toString, id.toString, elem.toString, Thread.currentThread().getId.toString,
              consumedCount.toString, (selfCounter + 1).toString)
            updCnt  <- consume(id, selfCounter + 1, counterR, consumer)
          } yield updCnt
        case None =>
          IO(log.debug("[{}-{}] End of queue. self consumed = [{}]",
            threadId.toString, id.toString, selfCounter.toString)) >>
            IO.pure(selfCounter)
      }
    } yield cnt

    def pendingItemsCount: Int = queue.size()
  }
