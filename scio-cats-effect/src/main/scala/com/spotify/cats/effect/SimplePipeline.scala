package com.spotify.cats.effect

import cats.effect.IO
import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow

object SimplePipeline {

  val doFn: DoFn[Int, String] = new DoFn[Int, String] {
    @ProcessElement
    def processElement(c: DoFn[Int, String]#ProcessContext, window: BoundedWindow): Unit = {
      c.output(c.element().toString)
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = sc.parallelize((0 until 10))

    input
      .applyTransform(ParDo.of(
        new DoFn[Int, String] {
          @ProcessElement
          private[effect] def processElement(c: ProcessContext, window: BoundedWindow): Unit = {
            c.output(c.element().toString)
          }
        }
      ))
      .debug()

    sc.run().waitUntilDone()
  }

}
