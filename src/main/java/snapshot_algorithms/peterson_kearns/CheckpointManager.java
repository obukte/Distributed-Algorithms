package snapshot_algorithms.peterson_kearns;

import akka.actor.typed.Terminated;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.AbstractBehavior;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import util.GraphParser;

public class CheckpointManager extends AbstractBehavior<CheckpointManager.Command> {

    public interface Command {}

    public class SnapshotData {
        private int personalState;
        private Map<String, Integer> vectorClock;

        public SnapshotData(int personalState, Map<String, Integer> vectorClock) {
            this.personalState = personalState;
            this.vectorClock = vectorClock;
        }

        public int getPersonalState() {
            return personalState;
        }

        public Map<String, Integer> getVectorClock() {
            return vectorClock;
        }
    }

    public static class BuildNetworkFromDotFile implements Command {
        final String dotFilePath;
        public BuildNetworkFromDotFile(String dotFilePath) {
            this.dotFilePath = dotFilePath;
        }
    }

    public static class RecoverActor implements Command {
        final String actorId;
        public RecoverActor(String actorId) {
            this.actorId = actorId;
        }
    }

    public static class TerminateActor implements Command {
        final String actorId;

        public TerminateActor(String actorId) {
            this.actorId = actorId;
        }
    }

    public static class InitiateNetworkSnapshot implements Command {}

    private Map<String, Set<String>> nodeNeighbors;
    private Map<String, ActorRef<PetersonKearnsActor.Message>> nodes;
    private ActorContext<Command> context;

    private CheckpointManager(ActorContext<Command> context) {
        super(context);
        this.nodeNeighbors = new HashMap<>();
        this.nodes = new HashMap<>();
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(CheckpointManager::new);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(BuildNetworkFromDotFile.class, this::onBuildNetworkFromDotFile)
                .onMessage(RecoverActor.class, this::onRecoverActor)
                .onMessage(TerminateActor.class, this::onTerminateActorCommand)
                .onMessage(InitiateNetworkSnapshot.class, this::onInitiateNetworkSnapshot)
                .onSignal(Terminated.class, this::onTerminated)
                .build();
    }

    private Behavior<Command> onBuildNetworkFromDotFile(BuildNetworkFromDotFile command) {
        getContext().getLog().info("Building network from DOT file: {}", command.dotFilePath);
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(command.dotFilePath);
        edges.forEach(edge -> {
            nodes.computeIfAbsent(edge.getSource(), sourceId -> getContext().spawn(PetersonKearnsActor.create(new HashSet<>(), 0), sourceId));
            nodes.computeIfAbsent(edge.getDestination(), destId -> getContext().spawn(PetersonKearnsActor.create(new HashSet<>(), 0), destId));
        });

        edges.forEach(edge -> {
            ActorRef<PetersonKearnsActor.Message> sourceNode = nodes.get(edge.getSource());
            ActorRef<PetersonKearnsActor.Message> destinationNode = nodes.get(edge.getDestination());
            sourceNode.tell(new PetersonKearnsActor.AddNeighbor(destinationNode));
            getContext().watch(sourceNode);
            getContext().watch(destinationNode);
        });
        return this;
    }

    private Behavior<Command> onRecoverActor(RecoverActor command) {
        recoverActorFromSnapshot(command.actorId);
        return this;
    }

    private Behavior<Command> onTerminateActorCommand(TerminateActor command) {
        ActorRef<PetersonKearnsActor.Message> actorToTerminate = nodes.get(command.actorId);
        if (actorToTerminate != null) {
            actorToTerminate.tell(new PetersonKearnsActor.TerminateActor());
            getContext().getLog().info("Sent termination request to actor with ID {}", command.actorId);
        } else {
            getContext().getLog().info("No actor found with ID {}", command.actorId);
        }
        return this;
    }

    private Behavior<Command> onTerminated(Terminated t) {
        String terminatedActorId = t.getRef().path().name();
        getContext().getLog().info("Actor {} has terminated.", terminatedActorId);
        recoverActorFromSnapshot(terminatedActorId);
        nodes.remove(terminatedActorId);
        return this;
    }

    private Behavior<Command> onInitiateNetworkSnapshot(InitiateNetworkSnapshot command) {
        nodes.values().forEach(actorRef -> {
            actorRef.tell(new PetersonKearnsActor.InitiateSnapshot());
            getContext().getLog().info("Initiated snapshot for actor {}", actorRef.path().name());
        });
        return this;
    }

    private void recoverActorFromSnapshot(String actorId) {
        // load the snapshot, determine the initial state and recreate the actor
        getContext().getLog().info("Trying to recover Actor {}, loading latest snapshot file.", actorId);
        try {
            String snapshotData = loadLatestSnapshot(actorId);
            if (snapshotData == null) {
                System.err.println("No snapshot data available to recover for actor ID: " + actorId);
                return;
            }
            // Parse the snapshot data to get both state and vector clock
            SnapshotData data = parseSnapshotData(snapshotData);

            // Determine the neighbors of the actor from the snapshot
            Set<ActorRef<PetersonKearnsActor.Message>> neighbors = determineNeighbors(actorId);

            // Create a new actor instance with the recovered state and vector clock
            ActorRef<PetersonKearnsActor.Message> newActor = getContext().spawn(
                    PetersonKearnsActor.create(neighbors, data.getPersonalState()),
                    actorId + "_recovered"
            );

            getContext().watch(newActor);
            nodes.put(actorId, newActor);
            getContext().getLog().info("Actor {} has been successfully recovered with id {}. ", actorId, actorId + "_recovered");

            // Handle rollback or state update for connected nodes based on `channelStates`
            data.getVectorClock().forEach((neighborId, clockValue) -> {
                ActorRef<PetersonKearnsActor.Message> neighbor = nodes.get(neighborId);
                if (neighbor != null) {
                    neighbor.tell(new PetersonKearnsActor.SetState(data.getPersonalState(), data.getVectorClock()));
                }
            });
        } catch (Exception e) {
            getContext().getLog().error("Failed to recover actor {}: {}", actorId, e.getMessage());
        }
    }

    private SnapshotData parseSnapshotData(String jsonData) throws IllegalArgumentException {
        if (jsonData == null || jsonData.isEmpty()) {
            throw new IllegalArgumentException("Snapshot data is null or empty");
        }

        getContext().getLog().info("Parsing snapshot data: {}", jsonData);

        // Parsing personal state
        String personalStateKey = "\"PersonalState\":";
        int personalStateIndex = jsonData.indexOf(personalStateKey);
        if (personalStateIndex == -1) {
            throw new IllegalArgumentException("PersonalState not found in the snapshot.");
        }
        int personalStateStart = personalStateIndex + personalStateKey.length();
        int personalStateEnd = jsonData.indexOf(",", personalStateStart);
        if (personalStateEnd == -1) personalStateEnd = jsonData.indexOf("}", personalStateStart);
        int personalState = Integer.parseInt(jsonData.substring(personalStateStart, personalStateEnd).trim());

        // Parsing vector clock
        String vectorClockKey = "\"VectorClock\":";
        int vectorClockIndex = jsonData.indexOf(vectorClockKey);
        if (vectorClockIndex == -1) {
            getContext().getLog().error("VectorClock field missing in the snapshot.");
            throw new IllegalArgumentException("VectorClock not found in the snapshot.");
        }
        int vectorClockStart = vectorClockIndex + vectorClockKey.length();
        int vectorClockEnd = jsonData.indexOf("}", vectorClockStart) + 1; // Include the closing brace '}'
        String vectorClockData = jsonData.substring(vectorClockStart, vectorClockEnd);

        Map<String, Integer> vectorClock = parseJsonMap(vectorClockData);

        return new SnapshotData(personalState, vectorClock);
    }

    private Map<String, Integer> parseJsonMap(String jsonData) throws IllegalArgumentException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(jsonData, new TypeReference<Map<String, Integer>>() {});
        } catch (JsonProcessingException e) {
            getContext().getLog().error("Error parsing JSON map: {}", e.getMessage());
            throw new IllegalArgumentException("Error parsing JSON map.", e);
        }
    }

    private String loadLatestSnapshot(String nodeId) {
        Path dirPath = Paths.get("snapshots");
        if (!Files.exists(dirPath)) {
            System.err.println("Snapshot directory does not exist: " + dirPath.toAbsolutePath());
            return null;
        }

        try (Stream<Path> paths = Files.walk(dirPath)) {
            Optional<Path> latestSnapshot = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("snapshot_" + nodeId + "_.*\\.json"))
                    .max(Comparator.comparingLong(path -> path.toFile().lastModified()));

            if (!latestSnapshot.isPresent()) {
                System.err.println("No snapshot found for node ID: " + nodeId);
                return null;
            }

            try {
                return new String(Files.readAllBytes(latestSnapshot.get()));
            } catch (IOException e) {
                System.err.println("Failed to read snapshot: " + e.getMessage());
                return null;
            }
        } catch (IOException e) {
            System.err.println("Failed to load snapshots: " + e.getMessage());
            return null;
        }
    }

    private Set<ActorRef<PetersonKearnsActor.Message>> determineNeighbors(String actorId) {
        Set<String> neighborIds = nodeNeighbors.getOrDefault(actorId, new HashSet<>());
        Set<ActorRef<PetersonKearnsActor.Message>> neighborRefs = new HashSet<>();

        neighborIds.forEach(id -> {
            ActorRef<PetersonKearnsActor.Message> neighborRef = nodes.get(id);
            if (neighborRef != null) {
                neighborRefs.add(neighborRef);
            }
        });

        return neighborRefs;
    }

}

