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
import java.util.ArrayList;


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


    @Test
    public void testSimpleTwoNodeElection() {
        String filePath = "target/test-classes/graph/Electiongraph0.dot";
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(filePath);

        Map<Integer, ActorRef<ChangRobertActor.Message>> actors = new HashMap<>();

        // Create actors for the two nodes defined in the simple graph
        edges.forEach(edge -> {
            try {
                int sourceId = Integer.parseInt(edge.getSource());
                int destId = Integer.parseInt(edge.getDestination());
                actors.computeIfAbsent(sourceId, id -> testKit.spawn(ChangRobertActor.create(id), "actor" + id));
                actors.computeIfAbsent(destId, id -> testKit.spawn(ChangRobertActor.create(id), "actor" + id));
            } catch (NumberFormatException e) {
                System.err.println("Error parsing node ID: " + e.getMessage());
            }
        });

        // Set each node's next actor to simulate a two-node ring
        if (actors.size() == 2) {
            List<Integer> nodeIds = new ArrayList<>(actors.keySet());
            Integer firstNodeId = nodeIds.get(0);
            Integer secondNodeId = nodeIds.get(1);
            actors.get(firstNodeId).tell(new ChangRobertActor.SetNextActor(actors.get(secondNodeId)));
            actors.get(secondNodeId).tell(new ChangRobertActor.SetNextActor(actors.get(firstNodeId)));
            System.out.println("Two-node ring setup complete between " + firstNodeId + " and " + secondNodeId);

            // Start the election from the first node
            System.out.println("Actor " + firstNodeId + " initiates the election");
            actors.get(firstNodeId).tell(new ChangRobertActor.StartElection(firstNodeId));
        } else {
            System.err.println("Test setup error: Incorrect number of actors created.");
        }

        // Wait to observe the election process
        try {
            Thread.sleep(3000); // Delay to allow the election message to circulate between the two nodes
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }

    @Test
    public void testDirectThreeNodeElection() {
        ActorTestKit testKit = ActorTestKit.create();
        try {
            // Manually create three actors
            ActorRef<ChangRobertActor.Message> actor1 = testKit.spawn(ChangRobertActor.create(1), "actor1");
            ActorRef<ChangRobertActor.Message> actor2 = testKit.spawn(ChangRobertActor.create(2), "actor2");
            ActorRef<ChangRobertActor.Message> actor3 = testKit.spawn(ChangRobertActor.create(3), "actor3");

            // Manually set up a ring topology among actors
            actor1.tell(new ChangRobertActor.SetNextActor(actor2));
            actor2.tell(new ChangRobertActor.SetNextActor(actor3));
            actor3.tell(new ChangRobertActor.SetNextActor(actor1));

            // Start the election from the first node
            System.out.println("Actor 1 initiates the election");
            actor1.tell(new ChangRobertActor.StartElection(1));

            // Wait to observe the election process
            try {
                Thread.sleep(5000); // Delay to allow the election message to circulate among the three nodes
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        } finally {
            testKit.shutdownTestKit();
        }
    }

}