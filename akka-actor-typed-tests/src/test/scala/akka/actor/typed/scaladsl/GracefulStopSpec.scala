/**
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.actor.typed
package scaladsl

import akka.Done
import akka.NotUsed
import akka.testkit.typed.TestKit
import akka.testkit.typed.scaladsl.TestProbe

sealed trait TestMessage
final case object Child1 extends TestMessage
final case object Child2 extends TestMessage
final case object Parent extends TestMessage

final class GracefulStopSpec extends TestKit with TypedAkkaSpecWithShutdown {

  "Graceful stop" must {

    "properly stop the children and perform the cleanup" in {
      val probe = TestProbe[TestMessage]("probe")

      val behavior =
        Behaviors.deferred[akka.NotUsed] { context ⇒
          val c1 = context.spawn[NotUsed](Behaviors.onSignal {
            case (_, PostStop) ⇒
              probe.ref ! Child1
              Behaviors.stopped
          }, "child1")

          val c2 = context.spawn[NotUsed](Behaviors.onSignal {
            case (_, PostStop) ⇒
              probe.ref ! Child2
              Behaviors.stopped
          }, "child2")

          Behaviors.stopped {
            Behaviors.onSignal {
              case (ctx, PostStop) ⇒
                // cleanup function body
                probe.ref ! Parent
                Behaviors.same
            }
          }
        }

      spawn(behavior)
      probe.expectMsg(Child1)
      probe.expectMsg(Child2)
      probe.expectMsg(Parent)
    }

    "properly perform the cleanup and stop itself for no children case" in {
      val probe = TestProbe[Done]("probe")

      val behavior =
        Behaviors.deferred[akka.NotUsed] { context ⇒
          // do not spawn any children
          Behaviors.stopped {
            Behaviors.onSignal {
              case (ctx, PostStop) ⇒
                // cleanup function body
                probe.ref ! Done
                Behaviors.same
            }
          }
        }

      spawn(behavior)
      probe.expectMsg(Done)
    }
  }

}
