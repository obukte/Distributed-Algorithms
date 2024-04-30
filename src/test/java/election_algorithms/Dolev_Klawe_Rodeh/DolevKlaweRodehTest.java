package election_algorithms.Dolev_Klawe_Rodeh;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor;



public class DolevKlaweRodehTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testElectionProcess() {
        TestProbe<DolevKlaweRodehActor.Message> neighbor1 = testKit.createTestProbe();
        TestProbe<DolevKlaweRodehActor.Message> neighbor2 = testKit.createTestProbe();

        ActorRef<DolevKlaweRodehActor.Message> actor1 = testKit.spawn(DolevKlaweRodehActor.create(1), "actor1");

        Map<Boolean, ActorRef<DolevKlaweRodehActor.Message>> neighbors = new HashMap<>();
        neighbors.put(true, neighbor1.getRef());
        neighbors.put(false, neighbor2.getRef());

        actor1.tell(new DolevKlaweRodehActor.InitializeRing(neighbors));
        actor1.tell(new DolevKlaweRodehActor.StartElection());

        // Simulate the return of the election message to itself
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, actor1, false));

        // Use probes to listen for any leadership announcement
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, neighbor1.getRef(), false));
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, neighbor2.getRef(), false));
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, actor1, true));  // message circulates back to the initiator

        // Wait and verify no more messages are sent after leader is elected
        TestProbe<DolevKlaweRodehActor.Message> probe = testKit.createTestProbe();
        probe.expectNoMessage(java.time.Duration.ofSeconds(1));

        testKit.stop(actor1);
    }

}