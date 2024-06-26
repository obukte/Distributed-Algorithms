package snapshot_algorithms.lai_yang;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import snapshot_algorithms.Message;
import util.GraphParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;
import static util.GraphParser.clearSnapshotsDirectory;

public class LaiYangActorTest {

    static ActorTestKit testKit;

    @BeforeClass
    public static void setup() throws IOException {

        testKit = ActorTestKit.create();
        clearSnapshotsDirectory();
    }

    @AfterClass
    public static void teardown() throws InterruptedException {

        testKit.shutdownTestKit();
        Thread.sleep(2000);
    }

    public Map<String, ActorRef<Message>> buildNetworkFromDotFile(String dotFilePath) throws Exception {
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(dotFilePath);

        Map<String, ActorRef<Message>> nodes = new HashMap<>();

        edges.forEach(edge -> {
            nodes.computeIfAbsent(edge.getSource(), sourceId -> testKit.spawn(LaiYangActor.create(new HashSet<>()), sourceId));
            nodes.computeIfAbsent(edge.getDestination(), destId -> testKit.spawn(LaiYangActor.create(new HashSet<>()), destId));
        });

        edges.forEach(edge -> {
            ActorRef<Message> sourceNode = nodes.get(edge.getSource());
            ActorRef<Message> destinationNode = nodes.get(edge.getDestination());
            sourceNode.tell(new LaiYangActor.AddNeighbor(destinationNode));
        });

        return nodes;
    }

    // Method to parse the snapshot file and return the state value
    private int parseSnapshotAndGetState(String filename) throws Exception {
        String content = new String(Files.readAllBytes(Paths.get("snapshots", filename)));
        String searchKey = "\"State\":";
        int startIndex = content.indexOf(searchKey) + searchKey.length();
        if (startIndex == -1) {
            throw new RuntimeException("State not found in snapshot file: " + filename);
        }
        int endIndex = content.indexOf(",", startIndex);
        endIndex = endIndex == -1 ? content.indexOf("}", startIndex) : endIndex; // Handle if "State" is the last element
        String stateStr = content.substring(startIndex, endIndex).trim();

        return Integer.parseInt(stateStr);
    }

    @Test
    public void testGraphCreationAndSnapshotInitiation() throws Exception {

        // Create nodes
        ActorRef<Message> nodeA = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeA");
        ActorRef<Message> nodeB = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeB");
        ActorRef<Message> nodeC = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeC");

        // Dynamically add neighbors using AddNeighbor message
        nodeA.tell(new LaiYangActor.AddNeighbor(nodeB));
        nodeB.tell(new LaiYangActor.AddNeighbor(nodeC));
        nodeC.tell(new LaiYangActor.AddNeighbor(nodeA)); // Creating a cyclic graph for demonstration

        // Example action: Initiate a snapshot from nodeA
        nodeA.tell(new LaiYangActor.InitiateSnapshot());

        // Allow some time for the snapshot initiation to propagate
        Thread.sleep(2000);

        File snapshotsDir = new File("snapshots");
        File[] files = snapshotsDir.listFiles((dir, name) -> name.startsWith("snapshot_NodeA") && name.endsWith(".json"));
        assertTrue("Snapshot file for NodeA should exist", files != null && files.length > 0);

        int stateNodeA = parseSnapshotAndGetState(files[files.length - 1].getName());

        Thread.sleep(2000);

        assertTrue("The state of NodeA after snapshot should be 20", stateNodeA == 0);

        Thread.sleep(2000);
    }

    @Test
    public void testSnapshotCreationAndContent() throws Exception {
        clearSnapshotsDirectory();

        ActorRef<Message> nodeA = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeA");
        ActorRef<Message> nodeB = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeB");
        ActorRef<Message> nodeC = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeC");
        ActorRef<Message> nodeD = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeD");

        // Connect nodes
        nodeA.tell(new LaiYangActor.AddNeighbor(nodeB));
        nodeB.tell(new LaiYangActor.AddNeighbor(nodeC));
        nodeC.tell(new LaiYangActor.AddNeighbor(nodeD));
        nodeD.tell(new LaiYangActor.AddNeighbor(nodeA));

        // Perform a calculation to change state
        nodeA.tell(new LaiYangActor.PerformCalculation(10));

        // Initiate snapshot from Node A
        nodeA.tell(new LaiYangActor.InitiateSnapshot());

        // Give some time for snapshot to be taken
        Thread.sleep(1000);

        File snapshotsDir = new File("snapshots");
        File[] files = snapshotsDir.listFiles((dir, name) -> name.startsWith("snapshot_NodeA") && name.endsWith(".json"));
        assertTrue("Snapshot file for NodeA should exist", files != null && files.length > 0);

        int stateNodeA = parseSnapshotAndGetState(files[files.length - 1].getName());

        assertTrue("The state of NodeA after snapshot should be 20", stateNodeA == 20);

        Thread.sleep(2000);
    }

    @Test
    public void testGraphSnapshotFromDotFile() throws Exception {
        clearSnapshotsDirectory();


        Map<String, ActorRef<Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph.dot");

        ActorRef<Message> initNode = network.get("0");
        initNode.tell(new LaiYangActor.InitiateSnapshot());

        initNode.tell(new LaiYangActor.PerformCalculation(10));

        Thread.sleep(2000);
    }

    @Test
    public void testGraphSnapshotFromDotFileTwo() throws Exception {
        clearSnapshotsDirectory();
        Thread.sleep(2000);

        Map<String, ActorRef<Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph2.dot");

        ActorRef<Message> initNode = network.get("0");

        initNode.tell(new LaiYangActor.PerformCalculation(10));

        initNode.tell(new LaiYangActor.InitiateSnapshot());


        Thread.sleep(5000);
    }

    @Test
    public void testGraphSnapshotFromDotFileThree() throws Exception {
        clearSnapshotsDirectory();
        Thread.sleep(2000);
        // Build the network from the dot file
        Map<String, ActorRef<Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph3.dot");

        // Assuming the initial snapshot trigger is from node "0"
        ActorRef<Message> initNode = network.get("0");

        initNode.tell(new LaiYangActor.PerformCalculation(10));

        initNode.tell(new LaiYangActor.InitiateSnapshot());

        // Allow some time for the snapshot process to complete
        Thread.sleep(5000);
    }

    @Test
    public void testGraphSnapshotFromDotFileThreeScenarioTwo() throws Exception {
        clearSnapshotsDirectory();
        // Build the network from the dot file
        Map<String, ActorRef<Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph3.dot");

        // Assuming the initial snapshot trigger is from node "0"
        ActorRef<Message> initNode = network.get("0");
        ActorRef<Message> nodeSeven = network.get("7");

        initNode.tell(new LaiYangActor.PerformCalculation(10));
        nodeSeven.tell(new LaiYangActor.PerformCalculation(2));

        initNode.tell(new LaiYangActor.InitiateSnapshot());
        nodeSeven.tell(new LaiYangActor.PerformCalculation(3));

        // Allow some time for the snapshot process to complete
        Thread.sleep(5000);

        File snapshotDir = new File("snapshots");
        File[] snapshotFiles = snapshotDir.listFiles((dir, name) -> name.startsWith("snapshot_7") && name.endsWith(".json"));
        assertTrue("Snapshot file for Node 7 should exist", snapshotFiles != null && snapshotFiles.length > 0);


        Thread.sleep(2000);
    }

}
