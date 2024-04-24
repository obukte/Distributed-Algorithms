package election_algorithms.Chang_Roberts;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import election_algorithms.Chang_roberts.ChangRobertActor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import util.GraphParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChangRobertsActorTest {

    static ActorTestKit testKit;

    @BeforeClass
    public static void setup() {
        testKit = ActorTestKit.create();
    }

    @AfterClass
    public static void teardown() {
        testKit.shutdownTestKit();
    }

    @Test
    public void testElectionProcess() {
        String filePath = "target/test-classes/graph/electionGraph.dot";
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(filePath);


        Map<Integer, ActorRef<ChangRobertActor.Message>> actors = new HashMap<>();

        // Create actors for each unique node ID found in the .ngs.dot file
        edges.forEach(edge -> {
            if (!edge.getSource().isEmpty() && !edge.getDestination().isEmpty()) {
                try {
                    int sourceId = Integer.parseInt(edge.getSource());
                    int destId = Integer.parseInt(edge.getDestination());
                    actors.computeIfAbsent(sourceId, id -> testKit.spawn(ChangRobertActor.create(id), "actor" + id));
                    actors.computeIfAbsent(destId, id -> testKit.spawn(ChangRobertActor.create(id), "actor" + id));
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing node ID: " + e.getMessage());
                }
            } else {
                System.err.println("Found an empty node ID in the edge: " + edge);
            }
        });

        // Assume a simple ring where each node points to the next
        actors.keySet().stream().sorted().forEach(id -> {
            int nextId = (id + 1) % actors.size(); // Creates a circular ring
            actors.get(id).tell(new ChangRobertActor.SetNextActor(actors.get(nextId)));
            System.out.println("Actor " + id + " set next actor to " + nextId);
        });

        // Initiate the election process from the smallest node ID
        if (!actors.isEmpty()) {
            Integer startNodeId = actors.keySet().stream().min(Integer::compareTo).orElse(null);
            if (startNodeId != null) {
                System.out.println("Actor " + startNodeId + " initiates the election");
                actors.get(startNodeId).tell(new ChangRobertActor.StartElection(startNodeId));
            } else {
                System.err.println("No valid nodes found to initiate the election.");
            }
        } else {
            System.err.println("No actors were created due to input errors.");
        }

       // Wait to observe the election process
        try {
            Thread.sleep(5000); // Simple delay to allow messages to be processed
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }

    }
}