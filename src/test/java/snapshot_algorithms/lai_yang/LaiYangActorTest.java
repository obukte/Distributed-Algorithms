package snapshot_algorithms.lai_yang;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import util.GraphParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

public class LaiYangActorTest {

    static ActorTestKit testKit;

    @BeforeClass
    public static void setup() throws IOException {

        testKit = ActorTestKit.create();
        clearSnapshotsDirectory();
    }

    @AfterClass
    public static void teardown() {
        testKit.shutdownTestKit();
    }

    public Map<String, ActorRef<LaiYangActor.Message>> buildNetworkFromDotFile(String dotFilePath) throws Exception {
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(dotFilePath);

        Map<String, ActorRef<LaiYangActor.Message>> nodes = new HashMap<>();

        edges.forEach(edge -> {
            nodes.computeIfAbsent(edge.getSource(), sourceId -> testKit.spawn(LaiYangActor.create(new HashSet<>()), sourceId));
            nodes.computeIfAbsent(edge.getDestination(), destId -> testKit.spawn(LaiYangActor.create(new HashSet<>()), destId));
        });

        edges.forEach(edge -> {
            ActorRef<LaiYangActor.Message> sourceNode = nodes.get(edge.getSource());
            ActorRef<LaiYangActor.Message> destinationNode = nodes.get(edge.getDestination());
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

    private static void clearSnapshotsDirectory() throws IOException {
        Path snapshotsDir = Paths.get("snapshots");
        if (Files.exists(snapshotsDir)) {
            Files.walk(snapshotsDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

            Files.createDirectories(snapshotsDir);
        } else {
            Files.createDirectories(snapshotsDir);
        }
    }

    @Test
    public void testNetworkFromDotFile() throws Exception {
        clearSnapshotsDirectory();
        ActorTestKit test_kit = ActorTestKit.create();
        // File path relative to the project or module where the test is located
        String dotFilePath = "src/test/resources/graph/testGraph2.dot";

        // Call the method and get the network nodes
        Map<String, ActorRef<LaiYangActor.Message>> nodes = buildNetworkFromDotFile(dotFilePath);

        // Example: Test the neighbors of node "0"
        TestProbe<LaiYangActor.NeighborsResponse> probe = test_kit.createTestProbe();
        nodes.get("0").tell(new LaiYangActor.QueryNeighbors(probe.ref()));

        LaiYangActor.NeighborsResponse response = probe.receiveMessage();

        assertTrue("Node 0 should have Node 1 as a neighbor", response.neighbors.contains("1"));
        assertTrue("Node 0 should have Node 2 as a neighbor", response.neighbors.contains("2"));

        Thread.sleep(2000);
    }


    @Test
    public void testGraphCreationAndSnapshotInitiation() throws Exception {
        clearSnapshotsDirectory();
        ActorTestKit testKit = ActorTestKit.create();

        // Create nodes
        ActorRef<LaiYangActor.Message> nodeA = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeA");
        ActorRef<LaiYangActor.Message> nodeB = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeB");
        ActorRef<LaiYangActor.Message> nodeC = testKit.spawn(LaiYangActor.create(new HashSet<>()), "NodeC");

        // Dynamically add neighbors using AddNeighbor message
        nodeA.tell(new LaiYangActor.AddNeighbor(nodeB));
        nodeB.tell(new LaiYangActor.AddNeighbor(nodeC));
        nodeC.tell(new LaiYangActor.AddNeighbor(nodeA)); // Creating a cyclic graph for demonstration

        // Example action: Initiate a snapshot from nodeA
        nodeA.tell(new LaiYangActor.InitiateSnapshot());

        // Allow some time for the snapshot initiation to propagate
        Thread.sleep(1000); // Adjust sleep time as needed

        File snapshotsDir = new File("snapshots");
        File[] files = snapshotsDir.listFiles((dir, name) -> name.startsWith("snapshot_NodeA") && name.endsWith(".json"));
        assertTrue("Snapshot file for NodeA should exist", files != null && files.length > 0);

        int stateNodeA = parseSnapshotAndGetState(files[files.length - 1].getName());

        assertTrue("The state of NodeA after snapshot should be 20", stateNodeA == 0);


        // Additional assertions can be added to verify the snapshot state of other nodes

        testKit.shutdownTestKit();
        Thread.sleep(2000);
    }

    @Test
    public void testSnapshotCreationAndContent() throws Exception {
        clearSnapshotsDirectory();
        ActorTestKit testKit2 = ActorTestKit.create();

        ActorRef<LaiYangActor.Message> nodeA = testKit2.spawn(LaiYangActor.create(new HashSet<>()), "NodeA");
        ActorRef<LaiYangActor.Message> nodeB = testKit2.spawn(LaiYangActor.create(new HashSet<>()), "NodeB");
        ActorRef<LaiYangActor.Message> nodeC = testKit2.spawn(LaiYangActor.create(new HashSet<>()), "NodeC");
        ActorRef<LaiYangActor.Message> nodeD = testKit2.spawn(LaiYangActor.create(new HashSet<>()), "NodeD");

        // Connect nodes
        nodeA.tell(new LaiYangActor.AddNeighbor(nodeB));
        nodeB.tell(new LaiYangActor.AddNeighbor(nodeC));
        nodeC.tell(new LaiYangActor.AddNeighbor(nodeD));
        nodeD.tell(new LaiYangActor.AddNeighbor(nodeA)); // Making it a circle for example

        // Perform a calculation to change state
        nodeA.tell(new LaiYangActor.PerformCalculation(10));

        // Initiate snapshot from Node A
        nodeA.tell(new LaiYangActor.InitiateSnapshot());

        // Give some time for snapshot to be taken
        Thread.sleep(1000); // Consider using a more robust synchronization method in production

        File snapshotsDir = new File("snapshots");
        File[] files = snapshotsDir.listFiles((dir, name) -> name.startsWith("snapshot_NodeA") && name.endsWith(".json"));
        assertTrue("Snapshot file for NodeA should exist", files != null && files.length > 0);

        int stateNodeA = parseSnapshotAndGetState(files[files.length - 1].getName());

        assertTrue("The state of NodeA after snapshot should be 20", stateNodeA == 20);

        testKit2.shutdownTestKit();
        Thread.sleep(2000);
    }

    @Test
    public void testGraphSnapshotFromDotFile() throws Exception {
        clearSnapshotsDirectory();
        ActorTestKit testKit3 = ActorTestKit.create();
        Map<String, ActorRef<LaiYangActor.Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph.dot");

        ActorRef<LaiYangActor.Message> initNode = network.get("0");
        initNode.tell(new LaiYangActor.InitiateSnapshot());

        initNode.tell(new LaiYangActor.PerformCalculation(10));

        Thread.sleep(2000);

        File snapshotDir = new File("snapshots");
        File[] snapshotFiles = snapshotDir.listFiles((dir, name) -> name.startsWith("snapshot_0") && name.endsWith(".json"));
        assertTrue("Snapshot file for Node 0 should exist", snapshotFiles != null && snapshotFiles.length > 0);

        int stateNode0 = parseSnapshotAndGetState(snapshotFiles[0].getName());

        int expectedState = 0; // Should be zero since we initiated the snapshot before any calculation
        assertTrue("The state of Node 0 after snapshot should match the expected value", stateNode0 == expectedState);
        testKit3.shutdownTestKit();
    }

    @Test
    public void testGraphSnapshotFromDotFileTwo() throws Exception {
        clearSnapshotsDirectory();
        Thread.sleep(2000);
        ActorTestKit testKit4 = ActorTestKit.create();

        Map<String, ActorRef<LaiYangActor.Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph2.dot");

        ActorRef<LaiYangActor.Message> initNode = network.get("0");

        initNode.tell(new LaiYangActor.PerformCalculation(10));

        initNode.tell(new LaiYangActor.InitiateSnapshot());

        Thread.sleep(2000);

        File snapshotDir = new File("snapshots");
        File[] snapshotFiles = snapshotDir.listFiles((dir, name) -> name.startsWith("snapshot_0") && name.endsWith(".json"));
        assertTrue("Snapshot file for Node 0 should exist", snapshotFiles != null && snapshotFiles.length > 0);

        testKit4.shutdownTestKit();
        Thread.sleep(5000);
    }

    @Test
    public void testGraphSnapshotFromDotFileThree() throws Exception {
        clearSnapshotsDirectory();
        Thread.sleep(2000);
        ActorTestKit testKit5 = ActorTestKit.create();
        // Build the network from the dot file
        Map<String, ActorRef<LaiYangActor.Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph3.dot");

        // Assuming the initial snapshot trigger is from node "0"
        ActorRef<LaiYangActor.Message> initNode = network.get("0");

        initNode.tell(new LaiYangActor.PerformCalculation(10));

        initNode.tell(new LaiYangActor.InitiateSnapshot());

        // Allow some time for the snapshot process to complete
        Thread.sleep(2000);

        File snapshotDir = new File("snapshots");
        File[] snapshotFiles = snapshotDir.listFiles((dir, name) -> name.startsWith("snapshot_0") && name.endsWith(".json"));
        assertTrue("Snapshot file for Node 0 should exist", snapshotFiles != null && snapshotFiles.length > 0);

        testKit5.shutdownTestKit();
        Thread.sleep(5000);
    }

    @Test
    public void testGraphSnapshotFromDotFileThreeScenarioTwo() throws Exception {
        clearSnapshotsDirectory();
        ActorTestKit testKit6 = ActorTestKit.create();
        // Build the network from the dot file
        Map<String, ActorRef<LaiYangActor.Message>> network = buildNetworkFromDotFile("src/test/resources/graph/testGraph3.dot");

        // Assuming the initial snapshot trigger is from node "0"
        ActorRef<LaiYangActor.Message> initNode = network.get("0");
        ActorRef<LaiYangActor.Message> nodeSeven = network.get("7");

        initNode.tell(new LaiYangActor.PerformCalculation(10));
        nodeSeven.tell(new LaiYangActor.PerformCalculation(2));

        initNode.tell(new LaiYangActor.InitiateSnapshot());
        nodeSeven.tell(new LaiYangActor.PerformCalculation(3));

        // Allow some time for the snapshot process to complete
        Thread.sleep(2000);

        File snapshotDir = new File("snapshots");
        File[] snapshotFiles = snapshotDir.listFiles((dir, name) -> name.startsWith("snapshot_7") && name.endsWith(".json"));
        assertTrue("Snapshot file for Node 7 should exist", snapshotFiles != null && snapshotFiles.length > 0);


        testKit6.shutdownTestKit();
        Thread.sleep(2000);
    }

}
