package com.spotify.cats.effect

import java.util.UUID

import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO}
import com.spotify.cats.effect.queue.SimpleMultiConsumerBoundedQueue
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, Setup, StartBundle}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

final class IODoFn[A, B](maxPendingItems: Int, parallelismLevel: Int)
                        (action: A => IO[B]) extends DoFn[A, KV[A, B]] {

    private val log = LoggerFactory.getLogger(getClass)
    private val instanceId = UUID.randomUUID().toString

    @volatile private var consumerFuture: Future[Long] = null
    private lazy val queue = new SimpleMultiConsumerBoundedQueue[A](maxPendingItems, parallelismLevel)
    private lazy implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
    private lazy val outputSemaphore = Semaphore[IO](1).unsafeRunSync()

    @Setup
    def setup(): Unit = {
      log.debug("Setup. instance id = {} thread id = {}",
        instanceId, Thread.currentThread().getId)
    }

    @StartBundle
    def startBundle(): Unit = {
      log.debug("Start bundle. instance id = {} thread id = {}",
        instanceId, Thread.currentThread().getId)
    }

    @ProcessElement
    def processElement(c: ProcessContext, window: BoundedWindow): Unit = {
      log.debug("process element [{}], instance id = {} thread id = {}",
         c.element().toString, instanceId, Thread.currentThread().getId.toString)

      queue.put(c.element())

      if (consumerFuture == null) {
        consumerFuture = queue.consume(input => {
          for {
            output <- action(input)
            _      <- outputSemaphore.withPermit {
              IO.delay(c.output(KV.of(input, output)))
            }
          } yield ()
        }).unsafeToFuture()
      }
    }

    @FinishBundle
    def finishBundle(c: FinishBundleContext): Unit = {
      log.debug("Finish bundle. instance id = {} thread id = {}",
        instanceId, Thread.currentThread().getId)

      queue.drainAndClose()
      Await.result(consumerFuture, Duration.Inf)
      consumerFuture = null
    }

}
