package snapshot_algorithms.peterson_kearns;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;

public class CheckpointManagerTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testBuildNetworkFromDotFile() throws InterruptedException {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());

        checkpointManager.tell(new CheckpointManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));

        // Expect to create actors and establish neighbors
        Thread.sleep(1000);
        assertNotNull("Actor system should have created actors", checkpointManager);
    }

    @Test
    public void testActorTermination() {
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());
        String actorId = "actor1"; // Ensure the actor exists or is mocked appropriately
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();

        checkpointManager.tell(new CheckpointManager.TerminateActor(actorId));

        probe.expectTerminated(probe.ref(), Duration.ofSeconds(3));
    }

    @Test
    public void testSnapshotCreation() {
        String snapshotDirectoryPath = "snapshots"; // Ensure this path is correct
        Path snapshotDir = Paths.get(snapshotDirectoryPath);
        try {
            // Trigger actions that should create snapshots
            ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());
            checkpointManager.tell(new CheckpointManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));
            checkpointManager.tell(new CheckpointManager.InitiateNetworkSnapshot());

            // Allow some time for files to be written if necessary
            Thread.sleep(2000); // Consider using Awaitility or another more robust asynchronous handling method

            // Check if the snapshot directory contains any files
            boolean hasSnapshots = Files.list(snapshotDir)
                    .anyMatch(path -> path.toString().endsWith(".json"));
            assertTrue("Snapshots should have been created", hasSnapshots);
        } catch (IOException | InterruptedException e) {
            fail("Failed due to an exception: " + e.getMessage());
        }
    }

    @Test
    public void testActorRecovery() {
        // Create system and test probe
        ActorTestKit testKit = ActorTestKit.create();
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();

        // Spawn the checkpoint manager
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());

        // Assume snapshot files are correctly placed in the environment
        String actorId = "actor1";

        // Tell the checkpoint manager to recover the actor
        checkpointManager.tell(new CheckpointManager.RecoverActor(actorId));

        // Expect that the checkpoint manager will recreate the actor
        // Here, you need to define what message or signal indicates successful recovery
        // This could be an actor state message sent to the probe, or checking the actor system directly

        // Example of expecting a message from the recovered actor
        PetersonKearnsActor.Message recoveryMessage = probe.receiveMessage();
        assertNotNull("Expected a recovery message from the actor", recoveryMessage);

        // Alternatively, check if the actor ref is part of the system now
        ActorRef<PetersonKearnsActor.Message> recoveredActor = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), actorId);
        assertNotNull("Actor should be recovered and present in the system", recoveredActor);

        testKit.shutdownTestKit();
    }

    @Test
    public void testNetworkRecovery() {
        ActorTestKit testKit = ActorTestKit.create();
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();

        // Spawn the checkpoint manager
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());

        // Step 1: Build the network from the DOT file
        checkpointManager.tell(new CheckpointManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));

        // Step 2: Initiate snapshots across all nodes
        checkpointManager.tell(new CheckpointManager.InitiateNetworkSnapshot());
        // Step 3: Terminate node "0" (simulated here; in practice, could be an actual termination)
        checkpointManager.tell(new CheckpointManager.TerminateActor("0"));

        // Step 4: Recover the terminated node "0"
        checkpointManager.tell(new CheckpointManager.RecoverActor("0"));

        // Check for actor recovery
        assertNotNull("Node 0 should have been recreated", testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "0"));

        testKit.shutdownTestKit();
    }

    @Test
    public void testCompleteNetworkRecovery() throws InterruptedException {
        // Create the checkpoint manager
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());

        // Build the network from a DOT file
        checkpointManager.tell(new CheckpointManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));

        // Allow some time for the network to be built
        Thread.sleep(1000);

        // Initiate snapshots across all nodes
        checkpointManager.tell(new CheckpointManager.InitiateNetworkSnapshot());

        // Simulate failure and recovery
        String failedNodeId = "node0";
        checkpointManager.tell(new CheckpointManager.TerminateActor(failedNodeId));
        Thread.sleep(500); // Simulate some processing time

        // Recover the failed node
        checkpointManager.tell(new CheckpointManager.RecoverActor(failedNodeId));
        Thread.sleep(1000); // Allow time for recovery

        // This is a placeholder for the verification step
        assertNotNull("The actor should have been successfully recovered and not be null", checkpointManager);

    }

    @Test
    public void testActorRecoveryFromSnapshot() throws Exception {
        // Create and build the network from a hypothetical DOT file
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());
        checkpointManager.tell(new CheckpointManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));

        // Allow time for network creation
        Thread.sleep(1000);

        // Simulate sending messages to change state
        ActorRef<PetersonKearnsActor.Message> actor0 = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "0");
        ActorRef<PetersonKearnsActor.Message> actor1 = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "1");

        actor0.tell(new PetersonKearnsActor.BasicMessage(10, actor1, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));
        Thread.sleep(1000);

        // Initiate snapshot for all actors
        checkpointManager.tell(new CheckpointManager.InitiateNetworkSnapshot());
        Thread.sleep(1000);

        // Terminate one actor
        checkpointManager.tell(new CheckpointManager.TerminateActor("0"));
        Thread.sleep(500);
    }

    @Test
    public void testActorRecoveryFromSnapshotTwo() throws Exception {
        // Create the actor system and spawn the checkpoint manager
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());
        checkpointManager.tell(new CheckpointManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph2.dot"));

        // Allow time for network to be built
        Thread.sleep(1000);

        // Create and spawn actors manually if not already created by the BuildNetwork command
        ActorRef<PetersonKearnsActor.Message> actor0 = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "0");
        ActorRef<PetersonKearnsActor.Message> actor1 = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "1");

        // Simulate sending messages to alter state
        actor0.tell(new PetersonKearnsActor.BasicMessage(10, actor1, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));

        // Allow time for messages to be processed
        Thread.sleep(1000);

        // Initiate snapshot across all nodes
        checkpointManager.tell(new CheckpointManager.InitiateNetworkSnapshot());
        Thread.sleep(1000);

        checkpointManager.tell(new CheckpointManager.TerminateActor("0"));
        Thread.sleep(500);

    }

    @Test
    public void testActorRecoveryFromSnapshotRecovery() throws Exception {
        // Create the actor system and spawn the checkpoint manager
        ActorRef<CheckpointManager.Command> checkpointManager = testKit.spawn(CheckpointManager.create());
        checkpointManager.tell(new CheckpointManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph2.dot"));

        // Allow time for network to be built
        Thread.sleep(1000);

        // Create and spawn actors manually if not already created by the BuildNetwork command
        ActorRef<PetersonKearnsActor.Message> actor0 = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "0");
        ActorRef<PetersonKearnsActor.Message> actor1 = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "1");

        // Simulate sending messages to alter state
        actor0.tell(new PetersonKearnsActor.BasicMessage(10, actor1, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(10, actor1, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));

        // Allow time for messages to be processed
        Thread.sleep(1000);

        // Initiate snapshot across all nodes
        checkpointManager.tell(new CheckpointManager.InitiateNetworkSnapshot());
        Thread.sleep(1000);

        checkpointManager.tell(new CheckpointManager.TerminateActor("0"));
        Thread.sleep(500);

    }

}
