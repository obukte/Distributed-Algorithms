package snapshot_algorithms.lai_yang;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class LaiYangActor extends AbstractBehavior<LaiYangActor.Message> {

    public interface Message {}

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
                .onMessage(TriggerSnapshot.class, msg -> {prepareForSnapshot();return Behaviors.same();})
                .build();
    }

    private Behavior<Message> onPresnapMessage(PresnapMessage message) {
        incomingMessageCounters.compute(message.from, (k, v) -> v == null ? message.count : v + message.count);
        if (!recorded) {
            getContext().getSelf().tell(new TriggerSnapshot());
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
            takeSnapshot();
            neighbors.forEach(neighbor -> {
                int count = incomingMessageCounters.getOrDefault(neighbor, 0) + 1;
                neighbor.tell(new PresnapMessage(count, getContext().getSelf()));
            });
        }
        return this;
    }

    private Behavior<Message> onPrintState(PrintState message) {
        getContext().getLog().info("Current state: {} (Recorded: {})", state, recorded);
        return this;
    }

    private Behavior<Message> onMarkerMessage(MarkerMessage message) {
        getContext().getLog().info("Received MarkerMessage (Recorded: {})", recorded);
        if (!recorded) {
            recorded = true;
            takeSnapshot();
            for (ActorRef<Message> neighbor : neighbors) {
                neighbor.tell(new MarkerMessage());
            }
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

    private void prepareForSnapshot() {
        if (!recorded) {
            recorded = true;
            snapshotTimestamp = LocalDateTime.now();
            takeSnapshot();
            neighbors.forEach(neighbor -> {
                int count = incomingMessageCounters.getOrDefault(neighbor, 0) + 1;
                neighbor.tell(new PresnapMessage(count, getContext().getSelf()));
            });
        }
    }

    private void takeSnapshot() {
        getContext().getLog().info("Taking snapshot...");
        snapshotTimestamp = LocalDateTime.now();
        String formattedTimestamp = snapshotTimestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        List<String> inTransitMessagesSerialized = inTransitMessages.stream()
                .map(msg -> String.format("{\"from\": \"%s\", \"value\": %d, \"recorded\": %b}",
                        msg.from.path().name(), msg.value, msg.isRecorded))
                .toList();

        String inTransitMessagesJson = inTransitMessagesSerialized.toString();

        String snapshotContent = String.format(
                "{\"Timestamp\": \"%s\", \"State\": %d, \"InTransitMessages\": %s}",
                formattedTimestamp,
                state,
                inTransitMessagesJson
        );

        String directoryPath = "snapshots";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdir();
        }

        String nodeName = getContext().getSelf().path().name();
        String filePath = directoryPath + "/snapshot_" + nodeName + "_" + formattedTimestamp.replace(":", "-").replace("T", "_") + ".json";

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(snapshotContent);
            getContext().getLog().info("Snapshot saved to " + filePath);
        } catch (IOException e) {
            getContext().getLog().error("Failed to save snapshot", e);
        }

        // Clear in-transit messages after taking the snapshot since they have now been recorded
        inTransitMessages.clear();
        checkSnapshotCompletion();
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
