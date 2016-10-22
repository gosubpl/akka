/**
 * Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.remote

import java.security.SecureRandom
import java.util.concurrent.TimeUnit

import akka.remote.security.provider.AESNewCounterRNG
import org.openjdk.jmh.annotations._
import org.uncommons.maths.random.AESCounterRNG

/*
akka-bench-jmh > jmh:run -wi 15 -i 15 -f 1 .*RandomReplacementBenchmark.*
[...]

[info] RandomReplacementBenchmark.runNew    ss   15  257.697 ± 38.849  us/op
[info] RandomReplacementBenchmark.runOld    ss   15  351.173 ± 88.658  us/op
 */

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SingleShotTime))
@Fork(5)
@Threads(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
class RandomReplacementBenchmark {
  private final val SOURCE = new SecureRandom
  var seed: Array[Byte] = null
  var rngNew: AESNewCounterRNG = null
  var rngOld: AESCounterRNG = null
  var inputNew: Array[Byte] = null
  var inputOld: Array[Byte] = null

  @Setup(Level.Trial)
  def setup(): Unit = {

  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
  }

  @Setup(Level.Iteration)
  def setupIteration(): Unit = {
    seed = SOURCE.generateSeed(32)
    rngNew = new AESNewCounterRNG(seed)
    rngOld = new AESCounterRNG(seed)
    inputNew = Array.fill[Byte](10000)(1)
    inputOld = Array.fill[Byte](10000)(1)
  }

  @TearDown(Level.Iteration)
  def shutdownIteration(): Unit = {
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def runOld(): Unit = {
    rngOld.nextBytes(inputOld)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def runNew(): Unit = {
    rngNew.nextBytes(inputNew)
  }
}
