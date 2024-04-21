package snapshot_algorithms.lai_yang;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import snapshot_algorithms.Message;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class LaiYangActor extends AbstractBehavior<Message> {

    public static final class PerformCalculation implements Message {
        public final int value;

        public PerformCalculation(int value) {
            this.value = value;
        }
    }

    public static final class PrintState implements Message {
    }

    public static final class MarkerMessage implements Message {
    }

    public static final class InitiateSnapshot implements Message {
    }

    public static final class StateMessage implements Message {
        public final int value;
        public final ActorRef<Message> from;
        public final boolean isRecorded;

        public StateMessage(int value, ActorRef<Message> from, boolean isRecorded) {
            this.value = value;
            this.from = from;
            this.isRecorded = isRecorded;
        }
    }

    public static final class PresnapMessage implements Message {
        public final int count;
        public final ActorRef<Message> from;

        public PresnapMessage(int count, ActorRef<Message> from) {
            this.count = count;
            this.from = from;
        }
    }

    public static final class QueryState implements Message {
        public final ActorRef<Message> replyTo;

        public QueryState(ActorRef<Message> replyTo) {
            this.replyTo = replyTo;
        }
    }

    public static final class StateResponse implements Message {
        public final int state;
        public final boolean recorded;
        public final Set<StateMessage> inTransitMessages;

        public StateResponse(int state, boolean recorded, Set<StateMessage> inTransitMessages) {
            this.state = state;
            this.recorded = recorded;
            this.inTransitMessages = inTransitMessages;
        }
    }

    public static final class AddNeighbor implements Message {
        public final ActorRef<Message> neighbor;

        public AddNeighbor(ActorRef<Message> neighbor) {
            this.neighbor = neighbor;
        }
    }

    public static final class QueryNeighbors implements Message {
        public final ActorRef<NeighborsResponse> replyTo;

        public QueryNeighbors(ActorRef<NeighborsResponse> replyTo) {
            this.replyTo = replyTo;
        }
    }

    public static final class NeighborsResponse implements Message {
        public final Set<String> neighbors;

        public NeighborsResponse(Set<String> neighbors) {
            this.neighbors = neighbors;
        }
    }

    public static final class TriggerSnapshot implements Message { }

    private int state = 0;
    private boolean recorded = false;
    private final Map<ActorRef<Message>, Integer> incomingMessageCounters = new HashMap<>();
    private final Set<ActorRef<Message>> neighbors;
    private final Set<StateMessage> inTransitMessages = new HashSet<>();
    private final Map<ActorRef<Message>, Set<StateMessage>> stateMessagesAfterSnapshot = new HashMap<>();
    private LocalDateTime snapshotTimestamp = null;

    public LaiYangActor(ActorContext<Message> context, Set<ActorRef<Message>> neighbors) {
        super(context);
        this.neighbors = neighbors;
        neighbors.forEach(neighbor -> incomingMessageCounters.put(neighbor, 0));
        String nodeName = context.getSelf().path().name();
        context.getLog().info("NodeActor {} created with neighbors: {}",nodeName,  neighbors);
    }

    public static Behavior<Message> create(Set<ActorRef<Message>> neighbors) {
        return Behaviors.setup(context -> new LaiYangActor(context, neighbors));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(PerformCalculation.class, this::onPerformCalculation)
                .onMessage(PrintState.class, this::onPrintState)
                .onMessage(MarkerMessage.class, this::onMarkerMessage)
                .onMessage(StateMessage.class, this::onStateMessage)
                .onMessage(QueryState.class, this::onQueryState)
                .onMessage(InitiateSnapshot.class, this::initiateSnapshot)
                .onMessage(AddNeighbor.class, this::onAddNeighbor)
                .onMessage(QueryNeighbors.class, this::onQueryNeighbors)
                .onMessage(PresnapMessage.class, this::onPresnapMessage)
                .onMessage(TriggerSnapshot.class, this::onTriggerSnapshot)
                .build();
    }

    private Behavior<Message> onPresnapMessage(PresnapMessage message) {
        incomingMessageCounters.compute(message.from, (k, v) -> {
            int newValue = (v == null ? 0 : v) + message.count;
            getContext().getLog().info("Updated counter for {} from {} to {} upon receiving PresnapMessage.", message.from.path().name(), v, newValue);
            return newValue;
        });
        if (!recorded) {
            getContext().getSelf().tell(new TriggerSnapshot());
        }
        // Check if termination condition is met
        boolean shouldTerminate = incomingMessageCounters.values().stream().allMatch(count -> count <= 0);
        if (shouldTerminate) {
            getContext().getLog().info("Termination condition met after processing PresnapMessage.");
            terminate();
        }
        return this;
    }


    private Behavior<Message> onPerformCalculation(PerformCalculation message) {
        getContext().getLog().info("Performing calculation: {} (Recorded: {})", message.value, recorded);

        state = message.value * 2;
        if (!recorded) {
            // Send StateMessage to neighbors to mimic in-transit messages if recorded
            neighbors.forEach(neighbor -> neighbor.tell(new StateMessage(state, getContext().getSelf(), recorded)));
        }
        return this;
    }

    private Behavior<Message> initiateSnapshot(InitiateSnapshot message) {
        getContext().getLog().info("Initiating snapshot process...");
        if (!recorded) {
            recorded = true;
            snapshotTimestamp = LocalDateTime.now();

            neighbors.forEach(neighbor -> {
                int count = incomingMessageCounters.getOrDefault(neighbor, 0) + 1;
                neighbor.tell(new PresnapMessage(count, getContext().getSelf()));
                incomingMessageCounters.put(neighbor, count);
            });

            getContext().getLog().info("{} is taking snapshot...", getContext().getSelf().path().name());

            // Prepare snapshot content including in-transit messages
            String formattedTimestamp = snapshotTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            String snapshotContent = prepareSnapshotContent(formattedTimestamp);

            // Save snapshot to file
            saveSnapshotToFile(snapshotContent, formattedTimestamp);

            // Clear in-transit messages and check for completion
            inTransitMessages.clear();
            checkSnapshotCompletion();
        }
        return this;
    }

    private Behavior<Message> onTriggerSnapshot(TriggerSnapshot message) {
        getContext().getLog().info("Received TriggerSnapshot (Recorded: {})", recorded);
        if (!recorded) {
            initiateSnapshotProcess();
        }
        return this;
    }

    private void initiateSnapshotProcess() {
        getContext().getLog().info("Initiating snapshot process...");
        recorded = true;
        snapshotTimestamp = LocalDateTime.now();

        neighbors.forEach(neighbor -> {
            int count = incomingMessageCounters.getOrDefault(neighbor, 0) + 1;
            neighbor.tell(new PresnapMessage(count, getContext().getSelf()));
            incomingMessageCounters.put(neighbor, count);
        });

        saveStateAndMessages();
    }

    private Behavior<Message> onPrintState(PrintState message) {
        getContext().getLog().info("Current state: {} (Recorded: {})", state, recorded);
        return this;
    }

    private Behavior<Message> onMarkerMessage(MarkerMessage message) {
        getContext().getLog().info("Received MarkerMessage (Recorded: {})", recorded);
        if (!recorded) {
            getContext().getSelf().tell(new TriggerSnapshot());
        }
        return this;
    }

    private Behavior<Message> onStateMessage(StateMessage message) {
        getContext().getLog().info("Received StateMessage from {} at {} with (value: {}, recorded: {})", message.from.path().name(),getContext().getSelf().path().name(), message.value, message.isRecorded);
        // Update the state and possibly pass the message further if not recorded
        if (!recorded && message.isRecorded) {
            getContext().getSelf().tell(new TriggerSnapshot());
            inTransitMessages.add(message);
        } else if (!recorded) {
            state = message.value * 2;
            neighbors.forEach(neighbor -> neighbor.tell(new StateMessage(state, getContext().getSelf(), false)));
        } else if (recorded) {
            inTransitMessages.add(message);
        }
        return this;
    }
    private Behavior<Message> onQueryState(QueryState message) {
        getContext().getLog().info("Received QueryState from {}", message.replyTo.path().name());
        message.replyTo.tell(new StateResponse(state, recorded, new HashSet<>(inTransitMessages)));
        return this;
    }

    private Behavior<Message> onAddNeighbor(AddNeighbor message) {
        String neighborName = message.neighbor.path().name();
        String nodeName = getContext().getSelf().path().name();
        getContext().getLog().info("Adding neighbor: {} to: {}", neighborName, nodeName);
        neighbors.add(message.neighbor);
        incomingMessageCounters.put(message.neighbor, 0);
        return this;
    }

    private Behavior<Message> onQueryNeighbors(QueryNeighbors message) {
        getContext().getLog().info("Received QueryNeighbors from {}", message.replyTo.path().name());
        Set<String> neighborPaths = neighbors.stream().map(neighbor -> neighbor.path().name()).collect(Collectors.toSet());
        message.replyTo.tell(new NeighborsResponse(neighborPaths));
        return this;
    }
    private void saveStateAndMessages() {
        // Ensure snapshotTimestamp is set
        if (snapshotTimestamp == null) {
            snapshotTimestamp = LocalDateTime.now();
        }
        String formattedTimestamp = snapshotTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        // Prepare the content of the snapshot
        String snapshotContent = prepareSnapshotContent(formattedTimestamp);
        // Save the snapshot to a file
        saveSnapshotToFile(snapshotContent, formattedTimestamp);

        getContext().getLog().info("Snapshot process completed for {}.", getContext().getSelf().path().name());
    }



    private String prepareSnapshotContent(String formattedTimestamp) {
        // Serialize in-transit messages
        List<String> inTransitMessagesSerialized = inTransitMessages.stream()
                .map(msg -> String.format("{\"from\": \"%s\", \"value\": %d, \"recorded\": %b}",
                        msg.from.path().name(), msg.value, msg.isRecorded))
                .collect(Collectors.toList());
        String inTransitMessagesJson = inTransitMessagesSerialized.toString();

        // Serialize counters for each channel
        List<String> countersSerialized = incomingMessageCounters.entrySet().stream()
                .map(entry -> String.format("\"%s\": %d", entry.getKey().path().name(), entry.getValue()))
                .collect(Collectors.toList());
        String countersJson = "{" + String.join(", ", countersSerialized) + "}";

        // Compile the snapshot content
        String snapshotContent = String.format(
                "{\"Timestamp\": \"%s\", \"State\": %d, \"InTransitMessages\": %s, \"Counters\": %s}",
                formattedTimestamp, state, inTransitMessagesJson, countersJson);

        return snapshotContent;
    }

    private void saveSnapshotToFile(String snapshotContent, String formattedTimestamp) {
        String directoryPath = "snapshots";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean wasSuccessful = directory.mkdir();
            if (!wasSuccessful) {
                getContext().getLog().error("Failed to create snapshot directory");
                return;
            }
        }

        String nodeName = getContext().getSelf().path().name();
        String fileName = String.format("snapshot_%s_%s.json", nodeName, formattedTimestamp.replace(":", "-").replace("T", "_"));
        String filePath = directoryPath + "/" + fileName;

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(snapshotContent);
            getContext().getLog().info("Snapshot saved to " + filePath);
        } catch (IOException e) {
            getContext().getLog().error("Failed to save snapshot", e);
        }
    }

    private void checkSnapshotCompletion() {
        boolean isComplete = isSnapshotComplete();
        if (isComplete) {
            terminate();
        }
    }

    private boolean isSnapshotComplete() {
        // Snapshot is complete if all channel counters are zero or negative
        boolean noPendingMessages = incomingMessageCounters.values().stream().allMatch(count -> count <= 0);
        return recorded && noPendingMessages;
    }

    private void terminate() {
        getContext().getLog().info("Terminating NodeActor after completing snapshot.", snapshotTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

}
