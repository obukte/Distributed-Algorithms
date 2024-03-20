package snapshot_algorithms.lai_yang;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import snapshot_algorithms.chandy_lamport.ChandyLamportActor;
import util.GraphParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

public class ChandyLamportActorTest {

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
    public void testSnapshotCreationAndContentChandyLamport() throws Exception {
        ActorTestKit testKit = ActorTestKit.create();

        // Spawn nodes with empty neighbor sets initially
        ActorRef<ChandyLamportActor.Message> nodeA = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeA");
        ActorRef<ChandyLamportActor.Message> nodeB = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeB");

        // Dynamically add each other as neighbors
        nodeA.tell(new ChandyLamportActor.AddNeighbor(nodeB));
        nodeB.tell(new ChandyLamportActor.AddNeighbor(nodeA));

        // Send a BasicMessage to change the state before initiating a snapshot
        nodeA.tell(new ChandyLamportActor.BasicMessage(10, nodeB));

        // Initiate snapshot
        nodeA.tell(new ChandyLamportActor.InitiateSnapshot());

        // Give some time for the snapshot to be taken
        Thread.sleep(1000); // Consider using a more robust synchronization method in production

        // Fetch the snapshot file for NodeA
        File snapshotsDir = new File("snapshots");
        File[] files = snapshotsDir.listFiles((dir, name) -> name.startsWith("snapshot_NodeA") && name.endsWith(".json"));
        assertTrue("Snapshot file for NodeA should exist", files != null && files.length > 0);

        // Parse the state from the first snapshot file
        int stateNodeA = parseSnapshotAndGetState(files[0].getName());
        assertTrue("The state of NodeA after snapshot should reflect the updated value", stateNodeA == 20);

        testKit.shutdownTestKit();
    }

    @Test
    public void testSnapshotCreationAndContentWithMultipleNodes() throws Exception {
        ActorTestKit testKit = ActorTestKit.create();

        // Spawn nodes
        ActorRef<ChandyLamportActor.Message> nodeA = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeA");
        ActorRef<ChandyLamportActor.Message> nodeB = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeB");
        ActorRef<ChandyLamportActor.Message> nodeC = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeC");
        ActorRef<ChandyLamportActor.Message> nodeD = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeD");
        ActorRef<ChandyLamportActor.Message> nodeE = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeE");
        ActorRef<ChandyLamportActor.Message> nodeF = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeF");

        // Establish neighbor relationships
        nodeA.tell(new ChandyLamportActor.AddNeighbor(nodeB));
        nodeB.tell(new ChandyLamportActor.AddNeighbor(nodeA));
        nodeB.tell(new ChandyLamportActor.AddNeighbor(nodeC));
        nodeC.tell(new ChandyLamportActor.AddNeighbor(nodeB));
        nodeC.tell(new ChandyLamportActor.AddNeighbor(nodeD));
        nodeD.tell(new ChandyLamportActor.AddNeighbor(nodeC));
        nodeD.tell(new ChandyLamportActor.AddNeighbor(nodeE));
        nodeE.tell(new ChandyLamportActor.AddNeighbor(nodeD));
        nodeE.tell(new ChandyLamportActor.AddNeighbor(nodeF));
        nodeF.tell(new ChandyLamportActor.AddNeighbor(nodeE));
        nodeF.tell(new ChandyLamportActor.AddNeighbor(nodeA));
        nodeA.tell(new ChandyLamportActor.AddNeighbor(nodeF));

        // Change states before initiating a snapshot
        nodeA.tell(new ChandyLamportActor.BasicMessage(10, nodeB));
        nodeB.tell(new ChandyLamportActor.BasicMessage(5, nodeC));
        nodeC.tell(new ChandyLamportActor.BasicMessage(15, nodeD));

        // Initiate snapshot from Node A
        nodeA.tell(new ChandyLamportActor.InitiateSnapshot());

        // Allow some time for the snapshot process to complete across the network
        Thread.sleep(3000);

        // Define the directory where snapshots are saved
        File snapshotsDir = new File("snapshots");


        // Verify snapshot files for all nodes
        String[] nodeNames = {"NodeA", "NodeB", "NodeC", "NodeD", "NodeE", "NodeF"};
        for (String nodeName : nodeNames) {
            File[] snapshotFiles = snapshotsDir.listFiles(
                    (dir, name) -> name.startsWith("snapshot_" + nodeName) && name.endsWith(".json"));
            assertTrue(nodeName + " should have a snapshot file", snapshotFiles != null && snapshotFiles.length > 0);
            // Optionally parse and verify the state of each node from its snapshot file
        }

        testKit.shutdownTestKit();
    }

    @Test
    public void testSnapshotCreationAndContentWithMultipleNodesMultipleSnapshot() throws Exception {
        ActorTestKit testKit = ActorTestKit.create();

        // Spawn nodes
        ActorRef<ChandyLamportActor.Message> nodeA = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeA");
        ActorRef<ChandyLamportActor.Message> nodeB = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeB");
        ActorRef<ChandyLamportActor.Message> nodeC = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeC");
        ActorRef<ChandyLamportActor.Message> nodeD = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeD");
        ActorRef<ChandyLamportActor.Message> nodeE = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeE");
        ActorRef<ChandyLamportActor.Message> nodeF = testKit.spawn(ChandyLamportActor.create(new HashSet<>()), "NodeF");

        // Establish neighbor relationships
        nodeA.tell(new ChandyLamportActor.AddNeighbor(nodeB));
        nodeB.tell(new ChandyLamportActor.AddNeighbor(nodeA));
        nodeB.tell(new ChandyLamportActor.AddNeighbor(nodeC));
        nodeC.tell(new ChandyLamportActor.AddNeighbor(nodeB));
        nodeC.tell(new ChandyLamportActor.AddNeighbor(nodeD));
        nodeD.tell(new ChandyLamportActor.AddNeighbor(nodeC));
        nodeD.tell(new ChandyLamportActor.AddNeighbor(nodeE));
        nodeE.tell(new ChandyLamportActor.AddNeighbor(nodeD));
        nodeE.tell(new ChandyLamportActor.AddNeighbor(nodeF));
        nodeF.tell(new ChandyLamportActor.AddNeighbor(nodeE));
        nodeF.tell(new ChandyLamportActor.AddNeighbor(nodeA));
        nodeA.tell(new ChandyLamportActor.AddNeighbor(nodeF));

        // Change states before initiating a snapshot
        nodeA.tell(new ChandyLamportActor.BasicMessage(10, nodeB));
        nodeB.tell(new ChandyLamportActor.BasicMessage(5, nodeC));
        nodeC.tell(new ChandyLamportActor.BasicMessage(15, nodeD));

        // Initiate snapshot from Node A
        nodeA.tell(new ChandyLamportActor.InitiateSnapshot());

        // Change states after first snapshot
        nodeD.tell(new ChandyLamportActor.BasicMessage(7, nodeF));
        nodeE.tell(new ChandyLamportActor.BasicMessage(3, nodeF));

        nodeA.tell(new ChandyLamportActor.InitiateSnapshot());
        // Initiate snapshot from Node A
        nodeF.tell(new ChandyLamportActor.InitiateSnapshot());

        // Allow some time for the snapshot process to complete across the network
        Thread.sleep(3000);

        // Define the directory where snapshots are saved
        File snapshotsDir = new File("snapshots");


        // Verify snapshot files for all nodes
        String[] nodeNames = {"NodeA", "NodeB", "NodeC", "NodeD", "NodeE", "NodeF"};
        for (String nodeName : nodeNames) {
            File[] snapshotFiles = snapshotsDir.listFiles(
                    (dir, name) -> name.startsWith("snapshot_" + nodeName) && name.endsWith(".json"));
            assertTrue(nodeName + " should have a snapshot file", snapshotFiles != null && snapshotFiles.length > 0);
            // Optionally parse and verify the state of each node from its snapshot file
        }

        testKit.shutdownTestKit();
    }

}
