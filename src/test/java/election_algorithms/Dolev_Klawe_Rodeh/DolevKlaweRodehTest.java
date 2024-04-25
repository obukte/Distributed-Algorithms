package election_algorithms.Dolev_Klawe_Rodeh;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor.ElectionMessage;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor.InitializeRing;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor.StartElection;

public class DolevKlaweRodehTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testElectionProcess() {
        TestProbe<DolevKlaweRodehActor.Message> neighbor1 = testKit.createTestProbe();
        TestProbe<DolevKlaweRodehActor.Message> neighbor2 = testKit.createTestProbe();

        ActorRef<DolevKlaweRodehActor.Message> actor1 = testKit.spawn(DolevKlaweRodehActor.create(1));
        System.out.println("Test started: Actor 1 spawned");

        Map<Boolean, ActorRef<DolevKlaweRodehActor.Message>> neighbors = new HashMap<>();
        neighbors.put(true, neighbor1.ref());
        neighbors.put(false, neighbor2.ref());

        actor1.tell(new InitializeRing(neighbors));
        System.out.println("InitializeRing message sent");

        actor1.tell(new StartElection());
        System.out.println("StartElection message sent");

        ElectionMessage message1 = (ElectionMessage) neighbor1.receiveMessage();
        ElectionMessage message2 = (ElectionMessage) neighbor2.receiveMessage();

        assertEquals(1, message1.electionId);
        assertEquals(actor1, message1.sender);
        System.out.println("Initial messages received and verified");

        actor1.tell(new ElectionMessage(2, neighbor1.ref(), !message1.parity));
        System.out.println("Higher election ID message sent");

        ElectionMessage forwardedMessage = (ElectionMessage) neighbor2.receiveMessage();
        assertEquals(2, forwardedMessage.electionId);
        assertEquals(!message1.parity, forwardedMessage.parity);
        System.out.println("Forwarded message received and verified");

        actor1.tell(new ElectionMessage(forwardedMessage.electionId, actor1, forwardedMessage.parity));
        ElectionMessage leaderAck = (ElectionMessage) neighbor1.receiveMessage();
        assertEquals(forwardedMessage.electionId, leaderAck.electionId);
        System.out.println("Leader message acknowledged");

        testKit.stop(actor1);
        System.out.println("Actor system stopped");
    }
}
