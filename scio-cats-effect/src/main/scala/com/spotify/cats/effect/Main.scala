package com.spotify.cats.effect

import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue, TimeUnit}

import cats._
import cats.implicits._
import cats.effect.implicits._
import cats.effect.{IO, Timer}
import cats.effect._
import cats.effect.concurrent.{Deferred, Ref, Semaphore}
import com.google.common.util.concurrent.MoreExecutors
import com.spotify.cats.effect.queue.{MultiConsumerBoundedQueue, SimpleMultiConsumerBoundedQueue}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, Setup, StartBundle}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory

import scala.concurrent
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import collection.immutable.Queue
import scala.concurrent.duration.{Duration, FiniteDuration}
import ExecutionContext.Implicits.global
import scala.collection.JavaConverters._



object Main {

  private val logger = LoggerFactory.getLogger("main")
  //private val queue = new LinkedBlockingQueue[Int](3)

  def main(args: Array[String]): Unit = {

    implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

//    val deferedD = Deferred[IO, Int].unsafeRunSync()
//
//    val app = for {
//      x <- IO.race(IO.never, deferedD.get)
//      _ <- IO.delay(logger.info("Deferred: {}", x))
//    } yield x
//
//    Future {
//      Thread.sleep(3000)
//      deferedD.complete(1).unsafeRunSync()
//    }
//
//    app.unsafeRunSync()

//    val executor = Executors.newCachedThreadPool()
//    implicit val executorService =
//      ExecutionContext.fromExecutor(global)

//    val producer = Future {
//      var i = 0
//      while(true) {
//        Thread.sleep(100)
//        queue.put(Some(i))
//
//        println(s"Produced new element [$i]. Queue size=${queue.size()} thread id = ${Thread.currentThread().getId}")
//        i += 1
//      }
//    }

//    val endOfWorld = Future {
//      Thread.sleep(10000)
//      queue.put(None)
//    }

    logger.info("Main program thread id = {}", Thread.currentThread().getId)

//    implicit val contextShift = IO.contextShift(global)
    val scheduledExecutor = Executors.newScheduledThreadPool(1)
    implicit val myTimer = IO.timer(ExecutionContext.fromExecutor(scheduledExecutor))


//    val parallelismLevel = 11
//    val cancelPromise = Promise[Unit]()

    //val blocker = Blocker[IO]

//    def consumer(id: Int,
//                 selfCounter: Long,
//                 counterR: Ref[IO, Int],
//                 cancel: IO[Unit]): IO[Long] = for {
//      _       <- contextShift.shift
//      nextStep <- IO.race(IO(queue.take()), cancel)
//      cnt     <- nextStep match {
//        case Left(elem) =>
//          for {
//            _       <- IO.sleep(FiniteDuration.apply(500, TimeUnit.MILLISECONDS))
//            consumedCount <- counterR.updateAndGet(_ + 1)
//            _       <- contextShift.shift
//            _        = logger.info(
//              "[{}] Consumed [{}]. thread id = {}. total consumed={} self consumed={}",
//              id.toString, Thread.currentThread().getId.toString,
//              consumedCount.toString, selfCounter.toString)
//            updCnt  <- consumer(id, selfCounter + 1, counterR, cancel)
//          } yield updCnt
//        case Right(_) =>
//          IO(logger.info("[{}] End of queue. self consumed = [{}]", id, selfCounter)) >>
//            IO.pure(selfCounter)
//      }
//    } yield cnt
//
//    val consumerFuture = {
//      for {
//        counterR <- Ref.of[IO, Int](0)
//        //cancelSemaphore <- Semaphore[IO](1)
//        cancel    = IO.fromFuture(IO(cancelPromise.future))
//        consumedCounts <- (0 until parallelismLevel).toList
//          .map(id => consumer(id, 0, counterR, cancel))
//          .parSequence
//        consumedCount <- counterR.get
//        _         = logger.info("Total number of consumed elements: {} == sum({})",
//          consumedCount, consumedCounts)
//      } yield consumedCount
//    }.unsafeToFuture()

    val queue = new SimpleMultiConsumerBoundedQueue[Int](3, 7)

    def producerFuture = Future {
      (0 until 5).foreach { k =>
        (0 until 1).foreach { i =>
          queue.put(k*5 + i)
          logger.info("Produced new element [{}]. Pending items count={} thread id = {}",
            i.toString, queue.pendingItemsCount.toString, Thread.currentThread().getId.toString)
        }
        Thread.sleep(1000)
      }

      queue.drainAndClose()
    }

    producerFuture

    val consumerFuture = queue
      .consume(_ => IO.sleep(FiniteDuration.apply(500, TimeUnit.MILLISECONDS))).unsafeToFuture()


    //Await.result(producerFuture, Duration.Inf)
    Await.result(consumerFuture, Duration.Inf)
    //endOfWorld.value
    scheduledExecutor.shutdownNow()
  }

}
