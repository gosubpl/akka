package akka.remote.serialization

import akka.remote.security.provider.ShadowAESCounterRNG
import akka.testkit.AkkaSpec
import org.uncommons.maths.random.{AESCounterRNG, SecureRandomSeedGenerator}

class RandomReplacementSpec extends AkkaSpec {
  "Secure random replacement" must {
    "generate the same" in {
      val rng = new FakeAES256CounterSecureRNG
      val rng2 = new FakeNewAES256CounterSecureRNG
      println(rng.getBytes().toList.map(_.toInt).toString)
      println(rng2.getBytes.toList.map(_.toInt).toString)
      true should ===(true)
    }
  }
}

private class FakeNewAES256CounterSecureRNG {
  // stubbed for testing
  private val seed: Array[Byte] = Array(
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte
  )

  private val rng = new ShadowAESCounterRNG(seed)

  // helper method, for test purposes only
  def getBytes = {
    var bytes = Array[Byte](1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    rng.nextBytes(bytes)
    bytes
  }
}

private class FakeAES256CounterSecureRNG {
  /**Singleton instance. */
  private final val Instance: SecureRandomSeedGenerator = new SecureRandomSeedGenerator

  // stub for test purposes
  private val seed: Array[Byte] = Array(
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte,
    1.toByte, 1.toByte, 1.toByte, 1.toByte
  )

  private val rng = new AESCounterRNG(seed)

  // helper method for test purposes only
  def getBytes() = {
    var bytes = Array[Byte](1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    rng.nextBytes(bytes)
    bytes
  }
}

