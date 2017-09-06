/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.cluster.ddata

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._
import akka.cluster.{ Cluster, ddata }
import akka.cluster.ddata.Replicator._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._
import com.typesafe.config.ConfigFactory
import akka.event.Logging.Error

object ReplicatorMapDeltaSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    akka.actor {
      serialize-messages = off
      allow-java-serialization = off
    }
    #akka.remote.artery.enabled = on
    """))

  testTransport(on = true)

  sealed trait Op
  final case class Delay(n: Int) extends Op
  final case class Incr(ki: (PNCounterMapKey[String], String), n: Int, consistency: WriteConsistency) extends Op
  final case class Decr(ki: (PNCounterMapKey[String], String), n: Int, consistency: WriteConsistency) extends Op
  // AddVD and RemoveVD for variant of ORMultiMap with Value Deltas, NoVD - for the vanilla ORMultiMap
  final case class AddVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency) extends Op
  final case class RemoveVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency) extends Op
  final case class AddNoVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency) extends Op
  final case class RemoveNoVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency) extends Op
  // AddOM and RemoveOM for Vanilla ORMap holding ORSet inside
  final case class AddOM(ki: (ORMapKey[String, ORSet[String]], String), elem: String, consistency: WriteConsistency) extends Op
  final case class RemoveOM(ki: (ORMapKey[String, ORSet[String]], String), elem: String, consistency: WriteConsistency) extends Op

  val timeout = 5.seconds
  val writeTwo = WriteTo(2, timeout)
  val writeMajority = WriteMajority(timeout)

  val KeyPN = PNCounterMapKey[String]("A")
  // VD and NoVD as above
  val KeyMMVD = ORMultiMapKey[String, String]("D")
  val KeyMMNoVD = ORMultiMapKey[String, String]("G")
  // OM as above
  val KeyOM = ORMapKey[String, ORSet[String]]("J")

  val KeyA: (PNCounterMapKey[String], String) = (KeyPN, "a")
  val KeyB: (PNCounterMapKey[String], String) = (KeyPN, "b")
  val KeyC: (PNCounterMapKey[String], String) = (KeyPN, "c")
  val KeyD: (ORMultiMapKey[String, String], String) = (KeyMMVD, "d")
  val KeyE: (ORMultiMapKey[String, String], String) = (KeyMMVD, "e")
  val KeyF: (ORMultiMapKey[String, String], String) = (KeyMMVD, "f")
  val KeyG: (ORMultiMapKey[String, String], String) = (KeyMMNoVD, "g")
  val KeyH: (ORMultiMapKey[String, String], String) = (KeyMMNoVD, "h")
  val KeyI: (ORMultiMapKey[String, String], String) = (KeyMMNoVD, "i")
  val KeyJ: (ORMapKey[String, ORSet[String]], String) = (KeyOM, "j")
  val KeyK: (ORMapKey[String, ORSet[String]], String) = (KeyOM, "k")
  val KeyL: (ORMapKey[String, ORSet[String]], String) = (KeyOM, "l")

  def generateOperations(onNode: RoleName): Vector[Op] = {
    //    onNode match {
    //      case `first` ⇒
    //        Vector(Incr(KeyC, 1, WriteLocal), AddNoVD(KeyH, "b", WriteLocal), AddNoVD(KeyG, "i", WriteLocal), AddOM(KeyK, "a", WriteLocal), Delay(161), Delay(212), RemoveVD(KeyF, "a", WriteLocal), RemoveOM(KeyL, "i", WriteLocal), Delay(155), AddOM(KeyK, "i", WriteLocal), Decr(KeyA, 3, WriteLocal), Decr(KeyC, 5, WriteMajority(5 seconds, 0)), Decr(KeyA, 0, WriteLocal), AddNoVD(KeyI, "b", WriteLocal), Incr(KeyB, 62, WriteLocal), AddVD(KeyE, "e", WriteMajority(5 seconds, 0)), Decr(KeyB, 0, WriteLocal), Decr(KeyA, 9, WriteLocal), Incr(KeyA, 49, WriteLocal), AddOM(KeyK, "j", WriteLocal), AddVD(KeyE, "g", WriteLocal), AddNoVD(KeyH, "b", WriteLocal), AddOM(KeyJ, "d", WriteLocal), Decr(KeyB, 6, WriteLocal), RemoveVD(KeyF, "j", WriteLocal), AddNoVD(KeyG, "j", WriteLocal), Delay(395), Delay(163), Delay(267), AddVD(KeyE, "a", WriteLocal), AddVD(KeyE, "g", WriteLocal), AddNoVD(KeyG, "e", WriteLocal), Incr(KeyC, 0, WriteLocal), RemoveVD(KeyF, "e", WriteLocal), Delay(463), Incr(KeyC, 62, WriteLocal), Delay(73), Delay(341), Delay(456), AddVD(KeyE, "f", WriteLocal), AddVD(KeyD, "a", WriteMajority(5 seconds, 0)), AddNoVD(KeyI, "f", WriteLocal), AddNoVD(KeyH, "d", WriteLocal), Decr(KeyC, 1, WriteLocal), AddVD(KeyD, "d", WriteLocal), AddVD(KeyE, "i", WriteLocal), AddOM(KeyJ, "g", WriteLocal), AddVD(KeyD, "b", WriteLocal), Delay(396), Decr(KeyC, 6, WriteLocal), Decr(KeyA, 9, WriteLocal))
    //      case `second` ⇒
    //        Vector(AddNoVD(KeyG, "a", WriteLocal), AddOM(KeyJ, "a", WriteLocal), AddOM(KeyL, "i", WriteLocal), Incr(KeyB, 57, WriteMajority(5 seconds, 0)), AddVD(KeyE, "f", WriteLocal), Delay(418), AddOM(KeyK, "h", WriteLocal), AddOM(KeyL, "h", WriteLocal), AddNoVD(KeyG, "h", WriteLocal), AddVD(KeyF, "c", WriteLocal), Decr(KeyC, 5, WriteLocal), Delay(316), AddOM(KeyJ, "j", WriteLocal), AddVD(KeyF, "g", WriteLocal), Delay(25), Decr(KeyA, 8, WriteLocal), AddOM(KeyJ, "c", WriteLocal), AddVD(KeyF, "h", WriteLocal), Decr(KeyA, 5, WriteLocal), Decr(KeyC, 6, WriteLocal), Delay(230), Decr(KeyB, 0, WriteLocal), Incr(KeyA, 20, WriteLocal), AddVD(KeyF, "b", WriteLocal), Decr(KeyC, 8, WriteLocal), AddOM(KeyK, "g", WriteLocal), AddVD(KeyD, "c", WriteLocal), Incr(KeyC, 92, WriteLocal), Decr(KeyA, 6, WriteMajority(5 seconds, 0)), Incr(KeyB, 79, WriteLocal), Decr(KeyB, 1, WriteLocal), AddVD(KeyF, "d", WriteLocal), Incr(KeyA, 57, WriteLocal), Decr(KeyA, 8, WriteLocal), AddNoVD(KeyG, "b", WriteLocal), AddVD(KeyF, "a", WriteLocal), Delay(409), AddOM(KeyL, "j", WriteLocal), Decr(KeyA, 4, WriteLocal), AddNoVD(KeyH, "e", WriteLocal), AddNoVD(KeyG, "e", WriteLocal), Decr(KeyA, 5, WriteLocal), Decr(KeyC, 5, WriteLocal), AddOM(KeyJ, "i", WriteLocal), AddOM(KeyJ, "b", WriteLocal), AddVD(KeyE, "i", WriteLocal), Delay(163), Decr(KeyA, 4, WriteLocal), Incr(KeyC, 67, WriteLocal), Delay(403), Incr(KeyA, 24, WriteLocal), AddNoVD(KeyI, "a", WriteLocal), AddVD(KeyD, "f", WriteLocal), AddNoVD(KeyH, "f", WriteLocal), Decr(KeyA, 6, WriteLocal), Incr(KeyB, 89, WriteLocal), Delay(348))
    //      case `third` ⇒
    //        Vector(Decr(KeyB, 3, WriteLocal), Decr(KeyA, 8, WriteLocal), AddNoVD(KeyG, "a", WriteLocal), Incr(KeyC, 42, WriteLocal), Decr(KeyA, 9, WriteLocal), Delay(321), AddVD(KeyD, "g", WriteLocal), AddNoVD(KeyG, "h", WriteLocal), Decr(KeyC, 3, WriteLocal), AddVD(KeyD, "d", WriteLocal), Incr(KeyA, 6, WriteLocal), Delay(365), AddVD(KeyF, "g", WriteLocal), AddOM(KeyK, "h", WriteLocal), AddVD(KeyF, "c", WriteLocal), AddOM(KeyK, "d", WriteTo(2, 5 seconds)), Incr(KeyB, 54, WriteLocal), Decr(KeyC, 4, WriteLocal), Decr(KeyC, 1, WriteLocal), Delay(457), AddVD(KeyD, "b", WriteLocal), Delay(339), Decr(KeyB, 8, WriteLocal), AddOM(KeyL, "a", WriteLocal), Incr(KeyC, 2, WriteLocal), Decr(KeyB, 1, WriteLocal), Decr(KeyA, 1, WriteLocal), AddOM(KeyK, "a", WriteLocal), Decr(KeyC, 0, WriteLocal), AddVD(KeyF, "e", WriteLocal), Delay(232), AddNoVD(KeyH, "b", WriteLocal), Decr(KeyB, 7, WriteLocal), Delay(211), AddOM(KeyL, "f", WriteLocal), Decr(KeyC, 0, WriteLocal), Delay(458), AddNoVD(KeyH, "b", WriteLocal), AddOM(KeyL, "j", WriteMajority(5 seconds, 0)), AddNoVD(KeyG, "a", WriteMajority(5 seconds, 0)), Delay(469), Decr(KeyB, 3, WriteLocal), AddNoVD(KeyI, "f", WriteLocal), AddVD(KeyE, "d", WriteLocal), Delay(292), AddOM(KeyK, "i", WriteLocal), AddOM(KeyJ, "h", WriteLocal), Delay(65), AddOM(KeyK, "f", WriteLocal), AddNoVD(KeyI, "c", WriteLocal), AddOM(KeyL, "c", WriteMajority(5 seconds, 0)))
    //      case `fourth` ⇒
    //        Vector(AddNoVD(KeyH, "b", WriteLocal), AddOM(KeyL, "d", WriteLocal), Incr(KeyC, 90, WriteLocal), AddVD(KeyE, "f", WriteLocal), Decr(KeyC, 4, WriteLocal), Delay(14), AddVD(KeyF, "h", WriteLocal), Delay(470), AddVD(KeyD, "g", WriteMajority(5 seconds, 0)), Delay(193), AddNoVD(KeyI, "e", WriteLocal), Incr(KeyB, 78, WriteLocal), Delay(397), AddOM(KeyK, "a", WriteLocal), AddVD(KeyD, "f", WriteLocal), AddVD(KeyD, "c", WriteLocal), Delay(179), AddNoVD(KeyH, "b", WriteLocal), AddVD(KeyE, "a", WriteLocal), Incr(KeyC, 67, WriteLocal), AddVD(KeyD, "a", WriteLocal), Decr(KeyA, 8, WriteLocal), Delay(165), AddOM(KeyK, "b", WriteLocal), Delay(212), Incr(KeyA, 24, WriteMajority(5 seconds, 0)), AddNoVD(KeyH, "e", WriteLocal), AddVD(KeyD, "h", WriteLocal), Decr(KeyC, 1, WriteLocal), Decr(KeyC, 9, WriteLocal), Delay(277), AddNoVD(KeyG, "f", WriteLocal), Delay(381), Delay(197), Decr(KeyA, 8, WriteLocal), AddNoVD(KeyG, "h", WriteLocal), Delay(93), Decr(KeyB, 7, WriteLocal), AddNoVD(KeyH, "i", WriteLocal), Decr(KeyB, 2, WriteLocal), AddVD(KeyF, "e", WriteLocal), Decr(KeyA, 8, WriteLocal), AddOM(KeyL, "h", WriteLocal), Decr(KeyC, 6, WriteLocal), AddOM(KeyL, "g", WriteLocal), Incr(KeyB, 4, WriteLocal), Incr(KeyA, 51, WriteLocal), AddVD(KeyF, "g", WriteLocal), AddOM(KeyL, "j", WriteLocal), Incr(KeyC, 22, WriteLocal), AddVD(KeyD, "i", WriteMajority(5 seconds, 0)), Decr(KeyA, 6, WriteLocal), Delay(495), Delay(223), Decr(KeyA, 5, WriteLocal))
    //    }

    val rnd = ThreadLocalRandom.current()

    def consistency(): WriteConsistency = {
      rnd.nextInt(100) match {
        case n if n < 90  ⇒ WriteLocal
        case n if n < 95  ⇒ writeTwo
        case n if n < 100 ⇒ writeMajority
      }
    }

    def rndPnCounterkey(): (PNCounterMapKey[String], String) = {
      rnd.nextInt(3) match {
        case 0 ⇒ KeyA
        case 1 ⇒ KeyB
        case 2 ⇒ KeyC
      }
    }

    def rndOrSetkeyVD(): (ORMultiMapKey[String, String], String) = {
      rnd.nextInt(3) match {
        case 0 ⇒ KeyD
        case 1 ⇒ KeyE
        case 2 ⇒ KeyF
      }
    }

    def rndOrSetkeyNoVD(): (ORMultiMapKey[String, String], String) = {
      rnd.nextInt(3) match {
        case 0 ⇒ KeyG
        case 1 ⇒ KeyH
        case 2 ⇒ KeyI
      }
    }

    def rndOrSetkeyOM(): (ORMapKey[String, ORSet[String]], String) = {
      rnd.nextInt(3) match {
        case 0 ⇒ KeyJ
        case 1 ⇒ KeyK
        case 2 ⇒ KeyL
      }
    }

    var availableForRemove = Set.empty[String]

    def rndAddElement(): String = {
      // lower case a - j
      val s = (97 + rnd.nextInt(10)).toChar.toString
      availableForRemove += s
      s
    }

    def rndRemoveElement(): String = {
      if (availableForRemove.isEmpty)
        "a"
      else
        availableForRemove.toVector(rnd.nextInt(availableForRemove.size))
    }

    (0 to (50 + rnd.nextInt(10))).map { _ ⇒
      rnd.nextInt(6) match {
        case 0 ⇒ Delay(rnd.nextInt(500))
        case 1 ⇒ Incr(rndPnCounterkey(), rnd.nextInt(100), consistency())
        case 2 ⇒ Decr(rndPnCounterkey(), rnd.nextInt(10), consistency())
        case 3 ⇒
          // ORMultiMap.withValueDeltas
          val key = rndOrSetkeyVD()
          // only removals for KeyF on node first
          if (key == KeyF && onNode == first && rnd.nextBoolean())
            RemoveVD(key, rndRemoveElement(), consistency())
          else
            AddVD(key, rndAddElement(), consistency())
        case 4 ⇒
          // ORMultiMap - vanilla variant - without Value Deltas
          val key = rndOrSetkeyNoVD()
          // only removals for KeyI on node first
          if (key == KeyI && onNode == first && rnd.nextBoolean())
            RemoveNoVD(key, rndRemoveElement(), consistency())
          else
            AddNoVD(key, rndAddElement(), consistency())
        case 5 ⇒
          // Vanilla ORMap - with ORSet inside
          val key = rndOrSetkeyOM()
          // only removals for KeyL on node first
          if (key == KeyL && onNode == first && rnd.nextBoolean())
            RemoveOM(key, rndRemoveElement(), consistency())
          else
            AddOM(key, rndAddElement(), consistency())
      }
    }.toVector
  }

  def addElementToORMap(om: ORMap[String, ORSet[String]], key: String, element: String)(implicit node: Cluster) =
    om.updated(node, key, ORSet.empty[String])(_.add(node, element))

  def removeElementFromORMap(om: ORMap[String, ORSet[String]], key: String, element: String)(implicit node: Cluster) =
    om.updated(node, key, ORSet.empty[String])(_.remove(node, element))
}

class ReplicatorMapDeltaSpecMultiJvmNode1 extends ReplicatorMapDeltaSpec
class ReplicatorMapDeltaSpecMultiJvmNode2 extends ReplicatorMapDeltaSpec
class ReplicatorMapDeltaSpecMultiJvmNode3 extends ReplicatorMapDeltaSpec
class ReplicatorMapDeltaSpecMultiJvmNode4 extends ReplicatorMapDeltaSpec

class ReplicatorMapDeltaSpec extends MultiNodeSpec(ReplicatorMapDeltaSpec) with STMultiNodeSpec with ImplicitSender {
  import Replicator._
  import ReplicatorMapDeltaSpec._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val fullStateReplicator = system.actorOf(Replicator.props(
    ReplicatorSettings(system).withGossipInterval(1.second).withDeltaCrdtEnabled(false)), "fullStateReplicator")
  val deltaReplicator = {
    val r = system.actorOf(Replicator.props(ReplicatorSettings(system)), "deltaReplicator")
    r ! Replicator.Internal.TestFullStateGossip(enabled = false)
    r
  }
  // both deltas and full state
  val ordinaryReplicator = system.actorOf(Replicator.props(
    ReplicatorSettings(system).withGossipInterval(1.second)), "ordinaryReplicator")

  var afterCounter = 0
  def enterBarrierAfterTestStep(): Unit = {
    afterCounter += 1
    enterBarrier("after-" + afterCounter)
  }

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "delta-CRDT" must {
    "join cluster" in {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)

      within(15.seconds) {
        awaitAssert {
          fullStateReplicator ! GetReplicaCount
          expectMsg(ReplicaCount(4))
        }
      }

      enterBarrierAfterTestStep()
    }

    "propagate delta" in {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)

      within(15.seconds) {
        awaitAssert {
          fullStateReplicator ! GetReplicaCount
          expectMsg(ReplicaCount(4))
        }
      }
      enterBarrier("ready")

      runOn(first) {
        // by setting something for each key we don't have to worry about NotFound
        List(KeyA, KeyB, KeyC).foreach { key ⇒
          fullStateReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_ increment key._2)
          deltaReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_ increment key._2)
        }
        List(KeyD, KeyE, KeyF).foreach { key ⇒
          fullStateReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(_ + (key._2 → Set("a")))
          deltaReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(_ + (key._2 → Set("a")))
        }
        List(KeyG, KeyH, KeyI).foreach { key ⇒
          fullStateReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ + (key._2 → Set("a")))
          deltaReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ + (key._2 → Set("a")))
        }
        List(KeyJ, KeyK, KeyL).foreach { key ⇒
          fullStateReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(_ + (key._2 → (ORSet.empty + "a")))
          deltaReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(_ + (key._2 → (ORSet.empty + "a")))
        }
      }
      enterBarrier("updated-1")

      within(5.seconds) {
        awaitAssert {
          val p = TestProbe()
          List(KeyA, KeyB, KeyC).foreach { key ⇒
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue should be(1)
          }
        }
        awaitAssert {
          val p = TestProbe()
          List(KeyD, KeyE, KeyF).foreach { key ⇒
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            val res = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2) should ===(Some(Set("a")))
          }
        }
        awaitAssert {
          val p = TestProbe()
          List(KeyG, KeyH, KeyI).foreach { key ⇒
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2) should ===(Some(Set("a")))
          }
        }
        awaitAssert {
          val p = TestProbe()
          List(KeyJ, KeyK, KeyL).foreach { key ⇒
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            val res = p.expectMsgType[GetSuccess[ORMap[String, ORSet[String]]]].dataValue.get(key._2)
            res.map(_.elements) should ===(Some(Set("a")))
          }
        }
      }

      enterBarrierAfterTestStep()
    }

    "replicate high throughput changes without OversizedPayloadException" in {
      val N = 1000
      val errorLogProbe = TestProbe()
      system.eventStream.subscribe(errorLogProbe.ref, classOf[Error])
      runOn(first) {
        for (_ ← 1 to N; key ← List(KeyA, KeyB)) {
          ordinaryReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_ increment key._2)
        }
      }
      enterBarrier("updated-2")

      within(5.seconds) {
        awaitAssert {
          val p = TestProbe()
          List(KeyA, KeyB).foreach { key ⇒
            ordinaryReplicator.tell(Get(key._1, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue should be(N)
          }
        }
      }

      enterBarrier("replicated-2")
      // no OversizedPayloadException logging
      errorLogProbe.expectNoMsg(100.millis)

      enterBarrierAfterTestStep()
    }

    "be eventually consistent" in {
      val operations = generateOperations(onNode = myself)
      log.debug(s"random operations on [${myself.name}]: ${operations.mkString(", ")}")
      try {
        // perform random operations with both delta and full-state replicators
        // and compare that the end result is the same

        for (op ← operations) {
          log.debug("operation: {}", op)
          op match {
            case Delay(d) ⇒ Thread.sleep(d)
            case Incr(key, n, consistency) ⇒
              fullStateReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_ increment (key._2, n))
              deltaReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_ increment (key._2, n))
            case Decr(key, n, consistency) ⇒
              fullStateReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_ decrement (key._2, n))
              deltaReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_ decrement (key._2, n))
            case AddVD(key, elem, consistency) ⇒
              // to have an deterministic result when mixing add/remove we can only perform
              // the ORSet operations from one node
              runOn((if (key == KeyF) List(first) else List(first, second, third)): _*) {
                fullStateReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(_ addBinding (key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(_ addBinding (key._2, elem))
              }
            case RemoveVD(key, elem, consistency) ⇒
              runOn(first) {
                fullStateReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(_ removeBinding (key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(_ removeBinding (key._2, elem))
              }
            case AddNoVD(key, elem, consistency) ⇒
              // to have an deterministic result when mixing add/remove we can only perform
              // the ORSet operations from one node
              runOn((if (key == KeyI) List(first) else List(first, second, third)): _*) {
                fullStateReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ addBinding (key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ addBinding (key._2, elem))
              }
            case RemoveNoVD(key, elem, consistency) ⇒
              runOn(first) {
                fullStateReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ removeBinding (key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ removeBinding (key._2, elem))
              }
            case AddOM(key, elem, consistency) ⇒
              // to have an deterministic result when mixing add/remove we can only perform
              // the ORSet operations from one node
              runOn((if (key == KeyL) List(first) else List(first, second, third)): _*) {
                fullStateReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om ⇒ addElementToORMap(om, key._2, elem))
                deltaReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om ⇒ addElementToORMap(om, key._2, elem))
              }
            case RemoveOM(key, elem, consistency) ⇒
              runOn(first) {
                fullStateReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om ⇒ removeElementFromORMap(om, key._2, elem))
                deltaReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om ⇒ removeElementFromORMap(om, key._2, elem))
              }
          }
        }

        enterBarrier("updated-3")

        List(KeyA, KeyB, KeyC).foreach { key ⇒
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue
              deltaValue should ===(fullStateValue)
            }
          }
        }

        List(KeyD, KeyE, KeyF).foreach { key ⇒
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              println(s"\n\n[FOO] Checking replication correctness on $key values: $deltaValue / $fullStateValue")
              deltaValue should ===(fullStateValue)
            }
          }
        }

        List(KeyG, KeyH, KeyI).foreach { key ⇒
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              deltaValue should ===(fullStateValue)
            }
          }
        }

        List(KeyJ, KeyK, KeyL).foreach { key ⇒
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[ORMap[String, ORSet[String]]]].dataValue.get(key._2)
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[ORMap[String, ORSet[String]]]].dataValue.get(key._2)
              deltaValue.map(_.elements) should ===(fullStateValue.map(_.elements))
            }
          }
        }

        enterBarrierAfterTestStep()
      } catch {
        case e: Throwable ⇒
          info(s"random operations on [${myself.name}]: ${operations.mkString(", ")}")
          throw e
      }
    }
  }

}

