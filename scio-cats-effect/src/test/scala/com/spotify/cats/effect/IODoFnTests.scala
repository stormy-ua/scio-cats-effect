package com.spotify.cats.effect

import cats.effect.IO
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.KV

class IODoFnTests extends PipelineSpec {

  it must "work" in {

    val catsIODoFn = new IODoFn[Int, String](1000, 20)(
        input => for {
          output <- IO.delay(input.toString + "+")
        } yield output
      )

    val count = 10000L

    runWithContext { sc =>
      val input = sc.parallelize((0 until count.toInt))

      input.applyTransform(ParDo.of(catsIODoFn)).count should containSingleValue(count)
    }

  }

}
