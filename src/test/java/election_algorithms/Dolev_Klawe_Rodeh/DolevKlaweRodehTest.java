package election_algorithms.Dolev_Klawe_Rodeh;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor.InitializeRing;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor.StartElection;
import util.GraphParser;
import util.GraphParser.Edge;


public class DolevKlaweRodehTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testElectionWithHighestIdBecomingLeader() {
        // Create actors
        ActorRef<DolevKlaweRodehActor.Message> actor1 = testKit.spawn(DolevKlaweRodehActor.create(1), "actor1");
        ActorRef<DolevKlaweRodehActor.Message> actor2 = testKit.spawn(DolevKlaweRodehActor.create(2), "actor2");
        ActorRef<DolevKlaweRodehActor.Message> actor3 = testKit.spawn(DolevKlaweRodehActor.create(3), "actor3");

        // Setup ring topology
        Map<Boolean, ActorRef<DolevKlaweRodehActor.Message>> neighbors1 = new HashMap<>();
        neighbors1.put(true, actor2);
        neighbors1.put(false, actor3);

        Map<Boolean, ActorRef<DolevKlaweRodehActor.Message>> neighbors2 = new HashMap<>();
        neighbors2.put(true, actor3);
        neighbors2.put(false, actor1);

        Map<Boolean, ActorRef<DolevKlaweRodehActor.Message>> neighbors3 = new HashMap<>();
        neighbors3.put(true, actor1);
        neighbors3.put(false, actor2);

        actor1.tell(new DolevKlaweRodehActor.InitializeRing(neighbors1));
        actor2.tell(new DolevKlaweRodehActor.InitializeRing(neighbors2));
        actor3.tell(new DolevKlaweRodehActor.InitializeRing(neighbors3));

        // Start the election from actor3 (the highest ID)
        actor3.tell(new DolevKlaweRodehActor.StartElection());

        // Use probes to listen for any leadership announcement
        TestProbe<DolevKlaweRodehActor.Message> probe = testKit.createTestProbe();
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(3, actor3, false));
        actor2.tell(new DolevKlaweRodehActor.ElectionMessage(3, actor3, false));
        actor3.tell(new DolevKlaweRodehActor.ElectionMessage(3, actor3, true));  // message circulates back to the initiator

        // Wait and verify no more messages are sent after leader is elected
        probe.expectNoMessage(java.time.Duration.ofSeconds(1));
    }


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


    @Test
    public void testElectionProcessGraph() {
        Map<Integer, ActorRef<DolevKlaweRodehActor.Message>> actors = new HashMap<>();
        Map<Integer, Integer> ringTopology = new HashMap<>();

        // Define the ring topology based on your .dot file
        ringTopology.put(0, 1);
        ringTopology.put(1, 2);
        ringTopology.put(2, 3);
        ringTopology.put(3, 0); // Closing the ring

        // Create actors for each node
        ringTopology.keySet().forEach(nodeId -> {
            actors.put(nodeId, testKit.spawn(DolevKlaweRodehActor.create(nodeId), "actor" + nodeId));
        });

        // Initialize neighbors for each actor based on the ring topology
        ringTopology.forEach((sourceId, destId) -> {
            ActorRef<DolevKlaweRodehActor.Message> sourceActor = actors.get(sourceId);
            ActorRef<DolevKlaweRodehActor.Message> destActor = actors.get(destId);
            Map<Boolean, ActorRef<DolevKlaweRodehActor.Message>> neighbors = new HashMap<>();
            neighbors.put(true, destActor); // 'true' for the single forward direction
            sourceActor.tell(new DolevKlaweRodehActor.InitializeRing(neighbors));
        });

        // Start the election with the actor that has the smallest ID
        Integer startNodeId = 0; // Starting from node 0
        System.out.println("Actor " + startNodeId + " initiates the election");
        actors.get(startNodeId).tell(new DolevKlaweRodehActor.StartElection());
    }

}