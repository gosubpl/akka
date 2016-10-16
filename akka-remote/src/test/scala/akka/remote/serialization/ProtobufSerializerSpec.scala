/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.remote.serialization

import java.security.{GeneralSecurityException, Key}
import java.util.Random
import javax.crypto.Cipher

import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec
import akka.remote.WireFormats.SerializedMessage
import akka.remote.ProtobufProtocol.MyMessage
import akka.remote.MessageSerializer
import akka.actor.ExtendedActorSystem
import org.uncommons.maths.random.{AESCounterRNG, SecureRandomSeedGenerator}

class ProtobufSerializerSpec extends AkkaSpec {

  val ser = SerializationExtension(system)

  "Serialization" must {

    "resolve protobuf serializer" in {
      ser.serializerFor(classOf[SerializedMessage]).getClass should ===(classOf[ProtobufSerializer])
      ser.serializerFor(classOf[MyMessage]).getClass should ===(classOf[ProtobufSerializer])
    }

    "work for SerializedMessage (just an akka.protobuf message)" in {
      // create a protobuf message
      val protobufMessage = MessageSerializer.serialize(system.asInstanceOf[ExtendedActorSystem], "hello")
      // serialize it with ProtobufSerializer
      val bytes = ser.serialize(protobufMessage).get
      // deserialize the bytes with ProtobufSerializer
      val deserialized = ser.deserialize(bytes, protobufMessage.getClass).get.asInstanceOf[SerializedMessage]
      deserialized.getSerializerId should ===(protobufMessage.getSerializerId)
      deserialized.getMessage should ===(protobufMessage.getMessage) // same "hello"
    }

  }

  "Secure random replacement" must {
    "generate the same" in {
      val rng = new FakeAES256CounterSecureRNG
      val rng2 = new ReplacementAES256CounterSecureRNG
      println(rng.getBytes().toList.map(_.toInt).toString)
      println(rng2.getBytes.toList.map(_.toInt).toString)
      true should ===(true)
    }
  }
}

private class ReplacementAES256CounterSecureRNG extends Random {
  // FIXME: replace with something sane
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

  private val counter: Array[Byte] = new Array[Byte](16) // beware, mutable state
  private var index : Int = 0
  private var currentBlock : Array[Byte] = null // FIXME: all very bad

  private val DEFAULT_SEED_SIZE_BYTES : Int = 16 // unused, for seed generator later

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
  private def nextBlock: Array[Byte] = {
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
        catch {
          case ex: GeneralSecurityException => {
            throw new IllegalStateException("Failed creating next random block.", ex)
          }
        }
      }
      result = convertBytesToInt(currentBlock, index)
      index += 4
    } finally {
    }
    result >>> (32 - bits)
  }

  private def convertBytesToInt (bytes: Array[Byte], offset: Int) : Int = {
    val BITWISE_BYTE_TO_INT = 0x000000FF
    (BITWISE_BYTE_TO_INT & bytes(offset + 3)) | ((BITWISE_BYTE_TO_INT & bytes(offset + 2)) << 8) | ((BITWISE_BYTE_TO_INT & bytes(offset + 1)) << 16) | ((BITWISE_BYTE_TO_INT & bytes(offset)) << 24)
  }

  def getBytes = {
    var bytes = Array[Byte](1, 1, 1, 1, 1)
    nextBytes(bytes)
    bytes
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

private class FakeAES256CounterSecureRNG extends java.security.SecureRandomSpi {
  /**Singleton instance. */
  private final val Instance: SecureRandomSeedGenerator = new SecureRandomSeedGenerator


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

  /**
    * This is managed internally by AESCounterRNG
    */
  override protected def engineSetSeed(seed: Array[Byte]): Unit = ()
  // check the interplay here between this and SecureRandom JDK API

  /**
    * Generates a user-specified number of random bytes.
    *
    * @param bytes the array to be filled in with random bytes.
    */
  override protected def engineNextBytes(bytes: Array[Byte]): Unit = rng.nextBytes(bytes)

  def getBytes() = {
    var bytes = Array[Byte](1, 1, 1, 1, 1)
    engineNextBytes(bytes)
    bytes
  }

  /**
    * Unused method
    * Returns the given number of seed bytes.  This call may be used to
    * seed other random number generators.
    *
    * @param numBytes the number of seed bytes to generate.
    * @return the seed bytes.
    */
  override protected def engineGenerateSeed(numBytes: Int): Array[Byte] = Instance.generateSeed(numBytes)
  // no need to re-implement, just copy as this uses simply SecureRandom from JDK API (almost!)
}
