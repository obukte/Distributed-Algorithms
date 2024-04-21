package snapshot_algorithms.peterson_kearns;

import akka.actor.typed.Terminated;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.AbstractBehavior;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import snapshot_algorithms.Message;
import util.GraphParser;

public class CheckpointRecoveryManager extends AbstractBehavior<CheckpointRecoveryManager.Command> {

    private static final String PERSISTENT_MESSAGE_LOG_PATH = "snapshots/message.log";

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

    public static final class GetActorRef implements Command {
        public final String actorId;
        public final ActorRef<ActorRef<Message>> replyTo;

        public GetActorRef(String actorId, ActorRef<ActorRef<Message>> replyTo) {
            this.actorId = actorId;
            this.replyTo = replyTo;
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
    private Map<String, ActorRef<Message>> nodes;

    private CheckpointRecoveryManager(ActorContext<Command> context) {
        super(context);
        this.nodeNeighbors = new HashMap<>();
        this.nodes = new HashMap<>();
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(CheckpointRecoveryManager::new);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(BuildNetworkFromDotFile.class, this::onBuildNetworkFromDotFile)
                .onMessage(RecoverActor.class, this::onRecoverActor)
                .onMessage(TerminateActor.class, this::onTerminateActorCommand)
                .onMessage(InitiateNetworkSnapshot.class, this::onInitiateNetworkSnapshot)
                .onSignal(Terminated.class, this::onTerminated)
                .onMessage(GetActorRef.class, this::onGetActorRef)
                .build();
    }

    // Handling GetActorRef inside createReceive method
    private Behavior<Command> onGetActorRef(GetActorRef command) {
        ActorRef<Message> actorRef = nodes.get(command.actorId); // Assuming actorMap holds the references
        command.replyTo.tell(actorRef);
        return this;
    }

    private Behavior<Command> onBuildNetworkFromDotFile(BuildNetworkFromDotFile command) {
        getContext().getLog().info("Building network from DOT file: {}", command.dotFilePath);
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(command.dotFilePath);
        edges.forEach(edge -> {
            nodes.computeIfAbsent(edge.getSource(), sourceId -> getContext().spawn(PetersonKearnsActor.create(new HashSet<>(), 0), sourceId));
            nodes.computeIfAbsent(edge.getDestination(), destId -> getContext().spawn(PetersonKearnsActor.create(new HashSet<>(), 0), destId));
        });

        edges.forEach(edge -> {
            ActorRef<Message> sourceNode = nodes.get(edge.getSource());
            ActorRef<Message> destinationNode = nodes.get(edge.getDestination());
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
        ActorRef<Message> actorToTerminate = nodes.get(command.actorId);
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
            Set<ActorRef<Message>> neighbors = determineNeighbors(actorId);

            // Create a new actor instance with the recovered state and vector clock
            ActorRef<Message> newActor = getContext().spawn(
                    PetersonKearnsActor.create(neighbors, data.getPersonalState()),
                    actorId + "_recovered"
            );

            getContext().watch(newActor);
            nodes.put(actorId, newActor);
            getContext().getLog().info("Actor {} has been successfully recovered with id {}. ", actorId, actorId + "_recovered");

            // Update neighbors based on messages sent after the snapshot
            updateNeighborsBasedOnMessagesLog("snapshots/message.log", actorId, data.getVectorClock());
            // Replay messages post recovery state from the message log
            replayMessages(PERSISTENT_MESSAGE_LOG_PATH, data.getVectorClock());
        } catch (Exception e) {
            getContext().getLog().error("Failed to recover actor {}: {}", actorId, e.getMessage());
        }
    }

    private void updateNeighborsBasedOnMessagesLog(String logFilePath, String actorId, Map<String, Integer> recoveryVC) {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // This makes sure we're only looking at lines with messages sent from the recovered actor
                if (line.startsWith("From: " + actorId)) {
                    // Parse the message log
                    Pattern pattern = Pattern.compile("From: (\\d+), To: (\\d+), Value: (\\d+), VectorClock: (\\{[^\\}]+\\})");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        String toId = matcher.group(2);
                        Map<String, Integer> msgVectorClock = parseVectorClock(matcher.group(4));

                        getContext().getLog().info("Analyzing message from {} to {}: {}", actorId, toId, line);

                        // Only trigger recovery if the message vector clock indicates a need for update
                        if (shouldReplay(msgVectorClock, recoveryVC)) {
                            ActorRef<Message> neighbor = nodes.get(toId);
                            if (neighbor == null) { // Check if the neighbor exists in the nodes map
                                getContext().getLog().info("Neighbor {} not active. Triggering recovery.", toId);
                                recoverActorFromSnapshot(toId);
                            } else {
                                getContext().getLog().info("Neighbor {} is already active, skipping recovery.", toId);
                            }
                        } else {
                            getContext().getLog().info("Message to {} does not require replay. Vector clock check passed.", toId);
                        }
                    }
                }
            }
        } catch (IOException e) {
            getContext().getLog().error("Error reading log file for neighbor updates: " + e.getMessage());
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

    private Set<ActorRef<Message>> determineNeighbors(String actorId) {
        Set<String> neighborIds = nodeNeighbors.getOrDefault(actorId, new HashSet<>());
        Set<ActorRef<Message>> neighborRefs = new HashSet<>();

        neighborIds.forEach(id -> {
            ActorRef<Message> neighborRef = nodes.get(id);
            if (neighborRef != null) {
                neighborRefs.add(neighborRef);
            }
        });

        return neighborRefs;
    }

    private Map<String, Integer> parseVectorClock(String vcString) {
        Map<String, Integer> vc = new HashMap<>();
        vcString = vcString.replace("{", "").replace("}", "");
        String[] entries = vcString.split(",");
        for (String entry : entries) {
            String[] kv = entry.split(":");
            vc.put(kv[0].trim().replace("\"", ""), Integer.parseInt(kv[1].trim()));
        }
        return vc;
    }

    private boolean shouldReplay(Map<String, Integer> messageVC, Map<String, Integer> snapshotVC) {
        // Replay if message vector clock is greater than snapshot vector clock for any key
        return messageVC.entrySet().stream()
                .anyMatch(e -> e.getValue() > snapshotVC.getOrDefault(e.getKey(), 0));
    }

    public void replayMessages(String logFilePath, Map<String, Integer> recoveryVC) {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Pattern pattern = Pattern.compile("From: (\\d+), To: (\\d+), Value: (\\d+), VectorClock: (\\{[^\\}]+\\})");
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    String fromId = matcher.group(1);
                    String toId = matcher.group(2);
                    int value = Integer.parseInt(matcher.group(3));
                    Map<String, Integer> messageVectorClock = parseVectorClock(matcher.group(4));

                    if (shouldReplay(messageVectorClock, recoveryVC)) {
                        ActorRef<Message> toActor = nodes.get(toId);
                        if (toActor != null) {
                            // Create and send a BasicMessage
                            PetersonKearnsActor.BasicMessage message = new PetersonKearnsActor.BasicMessage(value, nodes.get(fromId), messageVectorClock);
                            toActor.tell(message);
                            getContext().getLog().info("Replayed message from {} to {} with value {}", fromId, toId, value);
                        } else {
                            getContext().getLog().error("No actor found with ID {}", toId);
                        }
                    }
                }
            }
        } catch (IOException e) {
            getContext().getLog().error("Error reading log file: " + e.getMessage());
        }
    }


}

