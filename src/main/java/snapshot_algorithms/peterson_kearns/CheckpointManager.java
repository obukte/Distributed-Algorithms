package snapshot_algorithms.peterson_kearns;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.internal.SystemMessage;
import akka.actor.typed.javadsl.Behaviors;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CheckpointManager {
    private ActorSystem<PetersonKearnsActor.Message> system;
    private List<ActorRef<PetersonKearnsActor.Message>> actorRefs; // List of all managed actor references

    public CheckpointManager(ActorSystem<PetersonKearnsActor.Message> system, List<ActorRef<PetersonKearnsActor.Message>> actorRefs) {
        this.system = system;
        this.actorRefs = actorRefs;
    }

    public void triggerRollback(int nodeId) {
        System.out.println("Rollback requested for node ID: " + nodeId);
        String snapshotData = loadLatestSnapshot(nodeId);
        if (snapshotData != null) {
//            ActorRef<PetersonKearnsActor.Message> newActor = createActorWithSnapshot(nodeId, snapshotData);
//            actorRefs.add(newActor);
            System.out.println("New actor created and initialized for node ID: " + nodeId);
        } else {
            System.out.println("No snapshot found for node ID: " + nodeId);
        }
    }

    public void createActorFromSnapshot(int nodeId) {
        String snapshotData = loadLatestSnapshot(nodeId);
        if (snapshotData != null) {
            try {
                int personalState = parsePersonalState(snapshotData);
                Set<ActorRef<PetersonKearnsActor.Message>> initialNeighbors = new HashSet<>(); // Assume resolved elsewhere
                ActorRef<PetersonKearnsActor.Message> newActor = system.spawn(
                        PetersonKearnsActor.create(initialNeighbors, personalState),
                        "actor_" + nodeId
                );
                actorRefs.add(newActor);
                System.out.println("Actor recreated from snapshot with node ID: " + nodeId);
            } catch (Exception e) {
                System.err.println("Error parsing snapshot data: " + e.getMessage());
            }
        } else {
            System.out.println("No valid snapshot found for node ID: " + nodeId);
        }
    }

    private String loadLatestSnapshot(int nodeId) {
        Path dirPath = Paths.get("snapshots");
        try (Stream<Path> paths = Files.walk(dirPath)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("snapshot_actor_" + nodeId + "_.*\\.json"))
                    .max(Comparator.comparingLong(path -> path.toFile().lastModified()))
                    .map(path -> {
                        try {
                            return new String(Files.readAllBytes(path));
                        } catch (IOException e) {
                            System.err.println("Failed to read snapshot: " + e.getMessage());
                            return null;
                        }
                    })
                    .orElse(null);
        } catch (IOException e) {
            System.err.println("Failed to load snapshots: " + e.getMessage());
            return null;
        }
    }

    private int parsePersonalState(String jsonData) {
        String searchKey = "\"PersonalState\":";
        int index = jsonData.indexOf(searchKey);
        if (index == -1) {
            throw new IllegalArgumentException("PersonalState not found in the snapshot.");
        }
        int startIndex = index + searchKey.length();
        int endIndex = jsonData.indexOf(",", startIndex);
        if (endIndex == -1) { // Might not find a comma if it's the last item
            endIndex = jsonData.indexOf("}", startIndex);
        }
        return Integer.parseInt(jsonData.substring(startIndex, endIndex).trim());
    }



}

