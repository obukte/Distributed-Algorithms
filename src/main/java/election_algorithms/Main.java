package election_algorithms;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import election_algorithms.Chang_roberts.ChangRobertActor;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor;
import election_algorithms.echo_algorithm.EchoWithExtinctionActor;
import util.GraphParser;

import java.time.Duration;
import java.util.*;



public class Main {

    private static final String TEST_FILE_PATH = "src/main/resources/graph/NetGraph_17-03-24-12-50-04.ngs.dot";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        ActorTestKit testKit = ActorTestKit.create();

        try {
            while (true) {
                System.out.println("Choose the election algorithm to simulate:");
                System.out.println("1: Dolev-Klawe-Rodeh");
                System.out.println("2: Chang-Roberts ");
                System.out.println("3: Echo with Extinction");
                System.out.println("4: Exit");

                String choice = scanner.nextLine();
                switch (choice) {
                    case "1":
                        runDolevKlaweRodeh(testKit);
                        break;
                    case "2":
                        runChangRoberts(testKit);
                        break;
                    case "3":
                        runEchoWithExtinction(testKit);
                        break;
                    case "4":
                        System.out.println("Exiting...");
                        testKit.shutdownTestKit();
                        System.exit(0);
                    default:
                        System.out.println("Invalid option. Please enter 1, 2, 3, 4 or 5");
                }
            }
        } finally {
            scanner.close();
            testKit.shutdownTestKit();
        }
    }

    private static void runChangRoberts(ActorTestKit testKit) {
        String filePath = "src/main/resources/graph/NetGraph_17-03-24-12-50-04.ngs.dot";
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

    private static void runDolevKlaweRodeh(ActorTestKit testKit) {
        TestProbe<DolevKlaweRodehActor.Message> neighbor1 = testKit.createTestProbe();
        TestProbe<DolevKlaweRodehActor.Message> neighbor2 = testKit.createTestProbe();
        TestProbe<DolevKlaweRodehActor.Message> neighbor3 = testKit.createTestProbe();

        ActorRef<DolevKlaweRodehActor.Message> actor1 = testKit.spawn(DolevKlaweRodehActor.create(1), "actor1");

        // Create a ring topology with three neighbors
        Map<Boolean, ActorRef<DolevKlaweRodehActor.Message>> neighbors = new HashMap<>();
        neighbors.put(true, neighbor1.getRef());
        neighbors.put(false, neighbor2.getRef());
        neighbors.put(false, neighbor3.getRef());

        // Initialize the ring with the neighbors and start the election process
        actor1.tell(new DolevKlaweRodehActor.InitializeRing(neighbors));
        actor1.tell(new DolevKlaweRodehActor.StartElection());

        // Simulate the return of the election message to itself
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, actor1, false));

        // Use probes to listen for any leadership announcement
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, neighbor1.getRef(), false));
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, neighbor2.getRef(), false));
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, neighbor3.getRef(), false));
        actor1.tell(new DolevKlaweRodehActor.ElectionMessage(1, actor1, true));  // message circulates back to the initiator

        // Wait and verify no more messages are sent after leader is elected
        TestProbe<DolevKlaweRodehActor.Message> probe = testKit.createTestProbe();
        probe.expectNoMessage(java.time.Duration.ofSeconds(1));

        // Stop the actor after the test
        testKit.stop(actor1);

    }

    private static void runFranklinsAlgorithm(ActorTestKit testKit) {
        // Load the graph, create actors, and run Franklin's algorithm simulation
        // ... Add your code here
    }

    private static void runEchoWithExtinction(ActorTestKit testKit) {
        // Create a TestProbe to listen for the LeaderElected message
        TestProbe<EchoWithExtinctionActor.LeaderElected> probe = testKit.createTestProbe();

        // Parse the .dot file to create the network structure
        List<GraphParser.Edge> edges = GraphParser.parseDotFile("target/test-classes/graph/Electiongraph.dot");
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
        }
    }
}