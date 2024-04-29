package election_algorithms.Franklin_algorithms;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.List;
import util.GraphParser;
import util.GraphParser.Edge;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import election_algorithms.Franklins_algorithm.FranklinActor;

import static org.junit.Assert.assertEquals;

public class FranklinActorTest {

    private static ActorTestKit testKit;
    private static final AtomicInteger actorCounter = new AtomicInteger();

    @BeforeClass
    public static void setup() {
        testKit = ActorTestKit.create();
    }

    @AfterClass
    public static void teardown() {
        testKit.shutdownTestKit();
    }

    private static String generateUniqueActorName(String baseName) {
        return baseName + actorCounter.getAndIncrement();
    }

//    @Test
//    public void testFranklinElectionAlgorithm() {
//        String filePath = "target/test-classes/graph/Electiongraph2.dot";
//        List<Edge> edges = GraphParser.parseDotFile(filePath);
//
//        Map<String, ActorRef<FranklinActor.Message>> actors = edges.stream().collect(
//                Collectors.toMap(
//                        Edge::getSource,
//                        edge -> {
//                            String uniqueName = generateUniqueActorName("actor" + edge.getSource());
//                            return testKit.spawn(FranklinActor.create(Integer.parseInt(edge.getSource())), uniqueName);
//                        },
//                        (a1, a2) -> a1
//                )
//        );
//
//        actors.forEach((id, actor) -> {
//            int currentId = Integer.parseInt(id);
//            String leftNeighborId = String.valueOf((currentId + actors.size() - 1) % actors.size());
//            String rightNeighborId = String.valueOf((currentId + 1) % actors.size());
//            ActorRef<FranklinActor.Message> leftNeighbor = actors.get(leftNeighborId);
//            ActorRef<FranklinActor.Message> rightNeighbor = actors.get(rightNeighborId);
//            actor.tell(new FranklinActor.InitializeNeighbors(leftNeighbor, rightNeighbor));
//            System.out.println("Node " + currentId + " initialized with neighbors: " + leftNeighborId + ", " + rightNeighborId);
//        });
//
//        // Find the highest ID actor
//        int highestId = actors.keySet().stream().mapToInt(Integer::parseInt).max().orElseThrow();
//        ActorRef<FranklinActor.Message> highestIdActor = actors.get(String.valueOf(highestId));
//
//        // Start the election from the highest ID node
//        highestIdActor.tell(new FranklinActor.ElectionMessage(highestId, 0, "right"));
//        System.out.println("Node " + highestId + " started ElectionMessage");
//
//        TestProbe<FranklinActor.LeaderElected> probe = testKit.createTestProbe();
//
//        try {
//            // Wait for the LeaderElected message and assert the correct leader is elected
//            FranklinActor.LeaderElected elected = probe.expectMessageClass(FranklinActor.LeaderElected.class, Duration.ofSeconds(30));
//            assertEquals("The highest ID should be the leader", highestId, elected.leaderId);
//        } catch (AssertionError e) {
//            System.out.println("Failed to receive LeaderElected message within the timeout period.");
//            throw e;
//        }
//    }

    @Test
    public void testFranklinElectionAlgorithm() {
        // Create same topology in Distributed Algorithms by Wan Fokkink Example 9.3
        Map<Integer, Integer[]> neighborsMap = new HashMap<>();
        neighborsMap.put(0, new Integer[]{5, 4});
        neighborsMap.put(1, new Integer[]{3, 5});
        neighborsMap.put(2, new Integer[]{4, 3});
        neighborsMap.put(3, new Integer[]{2, 1});
        neighborsMap.put(4, new Integer[]{0, 2});
        neighborsMap.put(5, new Integer[]{1, 0});


        // Create a map of node IDs to actor references
        Map<Integer, ActorRef<FranklinActor.Message>> actors = new HashMap<>();

        // Instantiate all actors first
        neighborsMap.forEach((nodeId, neighborIds) -> {
            String uniqueName = generateUniqueActorName("actor" + nodeId);
            // Create each actor with its node ID and neighbor IDs
            actors.put(nodeId, testKit.spawn(FranklinActor.create(nodeId, neighborIds[0], neighborIds[1]), uniqueName));
        });

        // Initialize neighbors for each actor
        actors.forEach((nodeId, actor) -> {
            Integer[] neighborIds = neighborsMap.get(nodeId);
            ActorRef<FranklinActor.Message> leftNeighbor = actors.get(neighborIds[0]);
            ActorRef<FranklinActor.Message> rightNeighbor = actors.get(neighborIds[1]);
            // Tell each actor who their neighbors are
            actor.tell(new FranklinActor.InitializeNeighbors(leftNeighbor, rightNeighbor));
            System.out.println("Node " + nodeId + " initialized with neighbors: " + neighborIds[0] + ", " + neighborIds[1]);
        });

    }

//    @Test
//    public void testElectionProcess() {
//        TestProbe<FranklinActor.Message> leftProbe = testKit.createTestProbe();
//        TestProbe<FranklinActor.Message> rightProbe = testKit.createTestProbe();
//
////        ActorRef<FranklinActor.Message> franklinActor = testKit.spawn(FranklinActor.create(1));
//
//        // Initialize neighbors
//        franklinActor.tell(new FranklinActor.InitializeNeighbors(leftProbe.getRef(), rightProbe.getRef()));
//
//        // Expect initialization and send election messages
//        FranklinActor.ElectionMessage leftMessage = leftProbe.expectMessageClass(FranklinActor.ElectionMessage.class);
//        assertEquals("The left neighbor should receive the correct ID and round", 1, leftMessage.id);
//        assertEquals("The round should match", 0, leftMessage.round);
//        assertEquals("Direction should be 'left'", "left", leftMessage.direction);
//
//        FranklinActor.ElectionMessage rightMessage = rightProbe.expectMessageClass(FranklinActor.ElectionMessage.class);
//        assertEquals("The right neighbor should receive the correct ID and round", 1, rightMessage.id);
//        assertEquals("The round should match", 0, rightMessage.round);
//        assertEquals("Direction should be 'right'", "right", rightMessage.direction);
//
//        // Simulate further election process with correct parameters
//        franklinActor.tell(new FranklinActor.ElectionMessage(2, 0, "right"));
//
//        // Check if the leader election message is received correctly
//        FranklinActor.LeaderElected leaderMessage = rightProbe.expectMessageClass(FranklinActor.LeaderElected.class);
//        assertEquals("The leader should be recognized with the correct ID", 2, leaderMessage.leaderId);
//    }

}
