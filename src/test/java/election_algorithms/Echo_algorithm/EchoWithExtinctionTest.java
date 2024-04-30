package election_algorithms.Echo_algorithm;

import election_algorithms.echo_algorithm.EchoWithExtinctionActor;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import util.GraphParser;
import util.GraphParser.Edge;
import java.util.List;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;


public class EchoWithExtinctionTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @BeforeClass
    public static void setup() {
        // setup if necessary
    }

    @AfterClass
    public static void teardown() {
        testKit.shutdownTestKit();
    }


    @Test
    public void testEchoWithExtinctionElectionAlgorithm() {
        // Create a TestProbe to listen for the LeaderElected message
        TestProbe<EchoWithExtinctionActor.LeaderElected> probe = testKit.createTestProbe();

        // Parse the .dot file to create the network structure
        List<Edge> edges = GraphParser.parseDotFile("target/test-classes/graph/Electiongraph.dot");
        Map<Integer, ActorRef<EchoWithExtinctionActor.Message>> actors = new HashMap<>();
        edges.forEach(edge -> {
            int source = Integer.parseInt(edge.getSource());
            int destination = Integer.parseInt(edge.getDestination());
            String sourceName = "actor_" + source + "_to_" + destination + "_source"; // Descriptive actor name for source node
            String destinationName = "actor_" + source + "_to_" + destination + "_destination"; // Descriptive actor name for destination node

            // Log actor creation
            System.out.println("Creating actor: " + sourceName);
            System.out.println("Creating actor: " + destinationName);

            actors.putIfAbsent(source, testKit.spawn(EchoWithExtinctionActor.create(source, new HashMap<>(), probe), sourceName));
            actors.putIfAbsent(destination, testKit.spawn(EchoWithExtinctionActor.create(destination, new HashMap<>(), probe), destinationName));
        });

        // Initialize each actor with its neighbors from the parsed edges
        edges.forEach(edge -> {
            int source = Integer.parseInt(edge.getSource());
            int destination = Integer.parseInt(edge.getDestination());
            Map<Integer, ActorRef<EchoWithExtinctionActor.Message>> neighbors = new HashMap<>();
            neighbors.put(destination, actors.get(destination));

            // Log neighbor initialization
            System.out.println("Initializing neighbors for actor " + source);

            // Check if the source and destination IDs are going through the EchoWithExtinctionActor
            if (actors.containsKey(source) && actors.containsKey(destination)) {
                actors.get(source).tell(new EchoWithExtinctionActor.InitializeNeighbors(neighbors));
            } else {
                System.out.println("Invalid source or destination actor ID: " + source + ", " + destination);
            }
        });

        // Start the election from a node (for simplicity, we choose the first node in the list)
        if (!actors.isEmpty()) {
            // Find the node with the highest ID
            int highestId = actors.keySet().stream().max(Integer::compareTo).orElse(-1);

            // Log election start
            System.out.println("Starting election from node " + highestId);

            if (actors.containsKey(highestId)) {
                actors.get(highestId).tell(new EchoWithExtinctionActor.StartElection(highestId));
            } else {
                System.out.println("Invalid start node ID: " + highestId);
            }

            // Wait for the LeaderElected message using the test probe
            EchoWithExtinctionActor.LeaderElected elected = probe.receiveMessage(Duration.ofSeconds(10));

            // Assert that the node with the highest ID was elected as the leader
            assertEquals("The node with the highest ID should be elected as leader.", highestId, elected.leaderId);
        }
    }

}

