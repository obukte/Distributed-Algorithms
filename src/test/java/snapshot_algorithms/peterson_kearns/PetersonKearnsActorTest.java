package snapshot_algorithms.peterson_kearns;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class PetersonKearnsActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @BeforeClass
    public static void setup() {
    }

    @AfterClass
    public static void teardown() {
        testKit.shutdownTestKit();
    }

    @Test
    public void testSnapshotCreation() throws Exception {
        // Set the path where snapshots will be stored
        String snapshotDirectoryPath = "snapshots";
        File snapshotDirectory = new File(snapshotDirectoryPath);
        if (!snapshotDirectory.exists()) {
            snapshotDirectory.mkdir();
        }

        // Clear the snapshot directory before the test
        for (File file : snapshotDirectory.listFiles()) {
            file.delete();
        }

        // IDs for the actors
        int[] ids = {3, 5};
        Set<ActorRef<PetersonKearnsActor.Message>> actors = new HashSet<>();

        // Create actors with IDs 3 and 5
        for (int id : ids) {
            ActorRef<PetersonKearnsActor.Message> actor = testKit.spawn(PetersonKearnsActor.create(new HashSet<>()), "actor_" + id);
            actors.add(actor);
            actor.tell(new PetersonKearnsActor.InitiateSnapshot());
        }

        // Allow some time for files to be written
        Thread.sleep(1000); // This is generally not recommended but may be necessary in this context

        // Check if snapshot files exist for each actor
        for (int id : ids) {
            Path directoryPath = Paths.get(snapshotDirectoryPath);
            try (Stream<Path> paths = Files.walk(directoryPath)) {
                boolean found = paths
                        .map(Path::toFile)
                        .anyMatch(file -> file.getName().contains("actor_" + id) && file.getName().endsWith(".json"));
                assertTrue("Snapshot file for actor " + id + " does not exist", found);
            } catch (IOException e) {
                e.printStackTrace();
                fail("Failed to walk through the snapshot directory: " + e.getMessage());
            }
        }
    }

    @Test
    public void testMessageHandlingAndForwarding() {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        Set<ActorRef<PetersonKearnsActor.Message>> neighbors = new HashSet<>();
        neighbors.add(probe.ref());

        ActorRef<PetersonKearnsActor.Message> actor = testKit.spawn(PetersonKearnsActor.create(neighbors));

        Map<String, Integer> initialVectorClock = new HashMap<>();
        actor.tell(new PetersonKearnsActor.BasicMessage(100, actor, initialVectorClock));

        // Checking if the message was forwarded
        PetersonKearnsActor.BasicMessage receivedMessage = probe.expectMessageClass(PetersonKearnsActor.BasicMessage.class);
        assertEquals(100, receivedMessage.value);

        // Verify that the vector clock or other state changes as expected
    }

    @Test
    public void testActorTermination() {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        ActorRef<PetersonKearnsActor.Message> actor = testKit.spawn(PetersonKearnsActor.create(new HashSet<>()));

        actor.tell(new PetersonKearnsActor.TerminateActor());
        probe.expectTerminated(actor, probe.getRemainingOrDefault());
    }
}
