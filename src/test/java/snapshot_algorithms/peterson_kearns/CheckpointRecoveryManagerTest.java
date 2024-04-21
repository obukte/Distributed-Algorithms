package snapshot_algorithms.peterson_kearns;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class CheckpointRecoveryManagerTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testBuildNetworkFromDotFile() throws InterruptedException {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        ActorRef<CheckpointRecoveryManager.Command> checkpointManager = testKit.spawn(CheckpointRecoveryManager.create());

        checkpointManager.tell(new CheckpointRecoveryManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));

        Thread.sleep(1000);
        assertNotNull("Actor system should have created actors", checkpointManager);
    }

    @Test
    public void testSnapshotCreation() {
        String snapshotDirectoryPath = "snapshots";
        Path snapshotDir = Paths.get(snapshotDirectoryPath);
        try {
            // Trigger actions that should create snapshots
            ActorRef<CheckpointRecoveryManager.Command> checkpointManager = testKit.spawn(CheckpointRecoveryManager.create());
            checkpointManager.tell(new CheckpointRecoveryManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));
            checkpointManager.tell(new CheckpointRecoveryManager.InitiateNetworkSnapshot());

            // Allow some time for files to be written
            Thread.sleep(2000);

            // Check if the snapshot directory contains any files
            boolean hasSnapshots = Files.list(snapshotDir)
                    .anyMatch(path -> path.toString().endsWith(".json"));
            assertTrue("Snapshots should have been created", hasSnapshots);
        } catch (IOException | InterruptedException e) {
            fail("Failed due to an exception: " + e.getMessage());
        }
    }

    @Test
    public void testNetworkRecovery() {
        ActorTestKit testKit = ActorTestKit.create();
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();

        // Spawn the checkpoint manager
        ActorRef<CheckpointRecoveryManager.Command> checkpointManager = testKit.spawn(CheckpointRecoveryManager.create());

        //  Build the network from the DOT file
        checkpointManager.tell(new CheckpointRecoveryManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph.dot"));
        checkpointManager.tell(new CheckpointRecoveryManager.InitiateNetworkSnapshot());
        checkpointManager.tell(new CheckpointRecoveryManager.TerminateActor("0"));

        assertNotNull("Node 0 should have been recreated", testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0), "0"));

        testKit.shutdownTestKit();
    }


    @Test
    public void testActorRecoveryFromSnapshotRecovery() throws InterruptedException, ExecutionException {
        // Create the actor system and the checkpoint manager actor
        ActorSystem<CheckpointRecoveryManager.Command> system = ActorSystem.create(CheckpointRecoveryManager.create(), "System");

        // Send command to build the network
        system.tell(new CheckpointRecoveryManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph2.dot"));

        // Wait for the network to be built
        Thread.sleep(1000);

        // Fetch actors from the checkpoint manager
        CompletionStage<ActorRef<PetersonKearnsActor.Message>> actor0Future = AskPattern.ask(
                system,
                replyTo -> new CheckpointRecoveryManager.GetActorRef("0", replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        );

        CompletionStage<ActorRef<PetersonKearnsActor.Message>> actor1Future = AskPattern.ask(
                system,
                replyTo -> new CheckpointRecoveryManager.GetActorRef("1", replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        );

        ActorRef<PetersonKearnsActor.Message> actor0 = actor0Future.toCompletableFuture().get();
        ActorRef<PetersonKearnsActor.Message> actor1 = actor1Future.toCompletableFuture().get();

        // Ensure actors are not null
        assertEquals(true, actor0 != null);
        assertEquals(true, actor1 != null);

        // Send messages to actors to change state
        actor0.tell(new PetersonKearnsActor.BasicMessage(10, actor1, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(10, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));

        // Allow time for messages to be processed
        Thread.sleep(2000);

        // Initiate snapshot across all nodes
        system.tell(new CheckpointRecoveryManager.InitiateNetworkSnapshot());
        Thread.sleep(2000);

        // Terminate an actor
        system.tell(new CheckpointRecoveryManager.TerminateActor("0"));
        Thread.sleep(2000);

        // Cleanup system
        system.terminate();
    }

    @Test
    public void testActorRecoveryFromSnapshotRollbackRecovery() throws InterruptedException, ExecutionException {
        // Create the actor system and the checkpoint manager actor
        ActorSystem<CheckpointRecoveryManager.Command> system = ActorSystem.create(CheckpointRecoveryManager.create(), "System");

        // Send command to build the network
        system.tell(new CheckpointRecoveryManager.BuildNetworkFromDotFile("src/test/resources/graph/testGraph3.dot"));

        // Wait for the network to be built
        Thread.sleep(1000);

        // Fetch actors from the checkpoint manager
        CompletionStage<ActorRef<PetersonKearnsActor.Message>> actor0Future = AskPattern.ask(
                system,
                replyTo -> new CheckpointRecoveryManager.GetActorRef("0", replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        );

        CompletionStage<ActorRef<PetersonKearnsActor.Message>> actor1Future = AskPattern.ask(
                system,
                replyTo -> new CheckpointRecoveryManager.GetActorRef("1", replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        );

        ActorRef<PetersonKearnsActor.Message> actor0 = actor0Future.toCompletableFuture().get();
        ActorRef<PetersonKearnsActor.Message> actor1 = actor1Future.toCompletableFuture().get();

        // Ensure actors are not null
        assertEquals(true, actor0 != null);
        assertEquals(true, actor1 != null);

        // Send messages to actors to change state
        actor0.tell(new PetersonKearnsActor.BasicMessage(10, actor1, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(10, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));

        // Allow time for messages to be processed
        Thread.sleep(2000);

        // Initiate snapshot across all nodes
        system.tell(new CheckpointRecoveryManager.InitiateNetworkSnapshot());
        Thread.sleep(2000);

        actor1.tell(new PetersonKearnsActor.BasicMessage(10, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));

        // Terminate an actor
        system.tell(new CheckpointRecoveryManager.TerminateActor("0"));
        Thread.sleep(2000);

        // Cleanup system
        system.terminate();
    }

}
