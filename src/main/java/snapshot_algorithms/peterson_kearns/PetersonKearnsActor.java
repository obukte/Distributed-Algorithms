package snapshot_algorithms.peterson_kearns;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class PetersonKearnsActor extends AbstractBehavior<PetersonKearnsActor.Message> {

    public interface Message {}

    public static final class InitiateSnapshot implements Message {}

    public static final class TerminateActor implements Message {}

    private final Map<ActorRef<Message>, List<Message>> state;
    private Map<String, Integer> vectorClock;
    private int personalState;
    private static final String PERSISTENT_MESSAGE_LOG_PATH = "snapshots/message.log";

    public static final class BasicMessage implements Message {
        public final int value;
        public final ActorRef<Message> from;
        public final Map<String, Integer> vectorClock;

        public BasicMessage(int value, ActorRef<Message> from, Map<String, Integer> vectorClock) {
            this.value = value;
            this.from = from;
            this.vectorClock = new HashMap<>(vectorClock);
        }
    }

    public static final class AddNeighbor implements Message {
        public final ActorRef<Message> neighbor;

        public AddNeighbor(ActorRef<Message> neighbor) {
            this.neighbor = neighbor;
        }
    }

    public static final class SetState implements Message {
        public final int newState;
        public final Map<String, Integer> newVectorClock;

        public SetState(int newState, Map<String, Integer> newVectorClock) {
            this.newState = newState;
            this.newVectorClock = newVectorClock;
        }
    }

    public static class SentMessageInfo {
        final BasicMessage message;
        final Map<String, Integer> vectorClockAtSend;

        public SentMessageInfo(BasicMessage message, Map<String, Integer> vectorClockAtSend) {
            this.message = message;
            this.vectorClockAtSend = new HashMap<>(vectorClockAtSend);
        }
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitiateSnapshot.class, this::onInitiateSnapshot)
                .onMessage(BasicMessage.class, this::onBasicMessage)
                .onMessage(AddNeighbor.class, this::onAddNeighbor)
                .onMessage(TerminateActor.class, this::onTerminateActor)
                .onMessage(SetState.class, this::onSetState)
                .build();
    }

    public PetersonKearnsActor(ActorContext<Message> context, Set<ActorRef<Message>> neighbors, int initialState) {
        super(context);
        this.personalState = initialState;
        this.state = new HashMap<>();
        this.vectorClock = new HashMap<>();
        // Initialize the vector clock with zero for each neighbor and the actor itself
        this.vectorClock.put(context.getSelf().path().name(), 0);
        neighbors.forEach(neighbor -> {
            state.put(neighbor, new ArrayList<>());
            vectorClock.put(neighbor.path().name(), 0); // Initialize vector clock for each neighbor
        });
    }
    public static Behavior<Message> create(Set<ActorRef<Message>> initialNeighbors, int initialState) {
        return Behaviors.setup(context -> new PetersonKearnsActor(context, initialNeighbors, initialState));
    }

    private Behavior<Message> onInitiateSnapshot(InitiateSnapshot message) {
            getContext().getLog().info("Initiating snapshot process.");
            takeSnapshot();
            return this;
    }


    private Behavior<Message> onBasicMessage(BasicMessage message) {
        getContext().getLog().info("{} received BasicMessage with value: {} from {}", getContext().getSelf().path().name(), message.value, message.from.path().name());

        logMessage(message);
        // Ensure that the sender is known in the vector clock
        this.vectorClock.putIfAbsent(message.from.path().name(), 0);

        // Increment the vector clock for the sender to reflect the reception of their message
        this.vectorClock.compute(message.from.path().name(), (key, val) -> val == null ? 1 : val + 1);

//        getContext().getLog().info("Old Personal {} , new personal: {}", personalState, personalState + message.value);

        mergeVectorClocks(message.vectorClock);
        this.personalState += message.value;

        // Increment local vector clock to reflect local change
        incrementVectorClock(getContext().getSelf().path().name());
        return this;
    }

    private void logMessage(BasicMessage message) {
        // Convert vector clock to a string representation
        String vectorClockStr = this.vectorClock.entrySet().stream()
                .map(entry -> "\"" + entry.getKey() + "\":" + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));

        // Format log entry
        String logEntry = String.format("From: %s, To: %s, Value: %d, VectorClock: %s\n",
                getContext().getSelf().path().name(), message.from.path().name(), message.value, vectorClockStr);

        // Write to log file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(PERSISTENT_MESSAGE_LOG_PATH, true))) {
            writer.write(logEntry);
        } catch (IOException e) {
            getContext().getLog().error("Failed to write to log file", e);
        }
    }

    private Behavior<Message> onTerminateActor(TerminateActor message) {
        getContext().getLog().info("Terminating {} actor.", getContext().getSelf().path().name());
        return Behaviors.stopped(() -> getContext().getLog().info("Actor has been stopped."));
    }

    private Behavior<Message> onSetState(SetState message) {
        getContext().getLog().info("State and vector clock updated at {} by checkpoint manager. New State: {} New VectorClock: {} - Old State {}, Old VectorClock: {}", getContext().getSelf().path().name(), message.newState, message.newVectorClock, this.personalState, this.vectorClock );
        this.personalState = message.newState;
        this.vectorClock = new HashMap<>(message.newVectorClock);
        return this;
    }

    private void incrementVectorClock(String actorId) {
        this.vectorClock.put(actorId, vectorClock.getOrDefault(actorId, 0) + 1);  // Increment by 1
    }

    private void mergeVectorClocks(Map<String, Integer> receivedClock) {
        receivedClock.forEach((key, value) ->
                this.vectorClock.merge(key, value, Math::max));  // Merge received clock with local clock
    }


    private Behavior<Message> onAddNeighbor(AddNeighbor message) {
        if (!state.containsKey(message.neighbor)) {
            state.put(message.neighbor, new LinkedList<>());  // Initialize message queue for the new neighbor
            vectorClock.putIfAbsent(message.neighbor.path().name(), 0);  // Initialize vector clock entry if not present
            getContext().getLog().info("{} added as neighbor added to {}", message.neighbor.path().name(), getContext().getSelf().path().name());
        }
        return this;
    }

    private Behavior<Message> terminate() {
        // Implement termination logic. This could involve cleanup or preparing for shutdown.
        getContext().getLog().info("Snapshot protocol complete. Terminating {} actor.", getContext().getSelf().path().name());

        return Behaviors.stopped();
    }

    private void takeSnapshot() {
        getContext().getLog().info("Compiling snapshot data. Personal state: {}", this.personalState);

        // Serialize the vector clock
        String vectorClockJson = this.vectorClock.entrySet().stream()
                .map(entry -> "\"" + entry.getKey() + "\": " + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));

        // Serialize the channel states with messages
        String channelStatesJson = this.state.entrySet().stream()
                .map(entry -> {
                    String neighborName = entry.getKey().path().name();
                    List<Integer> messages = entry.getValue().stream()
                            .filter(message -> message instanceof BasicMessage)
                            .map(message -> ((BasicMessage) message).value)
                            .collect(Collectors.toList());
                    String channelEntry = "\"" + neighborName + "\": " + messages.toString();// Log each channel state entry
                    return channelEntry;
                })
                .collect(Collectors.joining(", ", "{", "}"));// Log the complete channel states JSON

        // Combine all serialized data into one snapshot string
        String formattedTimestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String snapshotContent = String.format(
                "{\"Timestamp\": \"%s\", \"PersonalState\": %d, \"VectorClock\": %s, \"ChannelStates\": %s}",
                formattedTimestamp,
                this.personalState,
                vectorClockJson,
                channelStatesJson
        );

        // Log the final snapshot content
        getContext().getLog().info("Snapshot Content: {}", snapshotContent);

        // Write the snapshot to a file
        writeSnapshotToFile(formattedTimestamp, snapshotContent);
    }

    private void writeSnapshotToFile(String formattedTimestamp, String snapshotContent) {
        getContext().getLog().info("Writing snapshot to file. Timestamp: {}", formattedTimestamp);
        String directoryPath = "snapshots";
        File directory = new File(directoryPath);
        if (!directory.exists() && !directory.mkdir()) {
            getContext().getLog().error("Failed to create snapshot directory");
            return;
        }

        String nodeName = getContext().getSelf().path().name();
        String safeTimestamp = formattedTimestamp.replace(":", "-").replace("T", "_");
        String filePath = directoryPath + "/snapshot_" + nodeName + "_" + safeTimestamp + ".json";

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(snapshotContent);
            getContext().getLog().info("Snapshot saved to " + filePath);
        } catch (IOException e) {
            getContext().getLog().error("Failed to save snapshot", e);
        }
    }

}
