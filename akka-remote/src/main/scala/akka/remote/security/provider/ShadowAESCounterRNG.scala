package akka.remote.security.provider

import java.nio.ByteBuffer
import java.security.{GeneralSecurityException, Key}
import java.util.Random
import javax.crypto.Cipher

class ShadowAESCounterRNG(val seed: Array[Byte]) extends Random {
  private val counter: Array[Byte] = new Array[Byte](16) // beware, mutable state
  private var index : Int = 0
  private var currentBlock : Array[Byte] = null // FIXME: all very bad

  private val cipher = Cipher.getInstance("AES/ECB/NoPadding")
  cipher.init(Cipher.ENCRYPT_MODE, new this.AESKey(seed))

  /**
    * Generates a single 128-bit block (16 bytes).
    *
    * @throws GeneralSecurityException If there is a problem with the cipher
    *                                  that generates the random data.
    * @return A 16-byte block of random data.
    */
  @throws[GeneralSecurityException]
  private def nextBlock: Array[Byte] = { // why not handle exceptions in this function...
  var i: Int = 0
    if (i < counter.length) {
      do {
        counter(i) = (counter(i) + 1.toByte).toByte
        i += 1
      } while ((i < counter.length ) && (counter(i-1) == 0))
    }
    cipher.doFinal(counter)
  }

  @Override
  override protected def next(bits: Int): Int = {
    var result: Int = 0
    try {
      if (currentBlock == null || currentBlock.length - index < 4) {
        try {
          currentBlock = nextBlock
          index = 0
        }
        catch { // is this possible at all - check - oh no, even more exceptions are possible...
          case ex: GeneralSecurityException => {
            throw new IllegalStateException("Failed creating next random block.", ex)
          }
        }
      }
      result = ByteBuffer.wrap(currentBlock).getInt(index)
      index += 4
    } finally {
    }
    result >>> (32 - bits)
  }

  /**
    * Trivial key implementation for use with AES cipher.
    */
  final private class AESKey(val keyData: Array[Byte]) extends Key {
    def getAlgorithm: String = "AES"
    def getFormat: String = "RAW"
    def getEncoded: Array[Byte] = keyData
  }
}
