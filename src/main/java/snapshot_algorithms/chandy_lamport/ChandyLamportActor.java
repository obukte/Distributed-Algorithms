package snapshot_algorithms.chandy_lamport;

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

public class ChandyLamportActor extends AbstractBehavior<Message> {

    public static final class InitiateSnapshot implements Message {}

    public static final class BasicMessage implements Message {
        public final int value;
        public final ActorRef<Message> from;

        public BasicMessage(int value, ActorRef<Message> from) {
            this.value = value;
            this.from = from;
        }
    }

    public static final class MarkerMessage implements Message {
        public final ActorRef<Message> from;

        public MarkerMessage(ActorRef<Message> from) {
            this.from = from;
        }
    }

    public static final class AddNeighbor implements Message {
        public final ActorRef<Message> neighbor;

        public AddNeighbor(ActorRef<Message> neighbor) {
            this.neighbor = neighbor;
        }
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitiateSnapshot.class, this::onInitiateSnapshot)
                .onMessage(BasicMessage.class, this::onBasicMessage)
                .onMessage(MarkerMessage.class, this::onMarkerMessage)
                .onMessage(AddNeighbor.class, this::onAddNeighbor)
                .build();
    }

    private  boolean recorded;
    private final Map<ActorRef<Message>, Boolean> marker;
    private final Map<ActorRef<Message>, List<Message>> state;
    private int personalState;

    public ChandyLamportActor(ActorContext<Message> context, Set<ActorRef<Message>> neighbors) {
        super(context);
        this.recorded = false;
        this.personalState = 0;
        this.marker = new HashMap<>();
        this.state = new HashMap<>();
        for (ActorRef<Message> neighbor : neighbors) {
            this.marker.put(neighbor, false);
            this.state.put(neighbor, new LinkedList<>());
        }
    }
    public static Behavior<Message> create(Set<ActorRef<Message>> initialNeighbors) {
        return Behaviors.setup(context -> new ChandyLamportActor(context, initialNeighbors));
    }

    // Call this method when you want to perform the basic calculation
    private void performCalculationAndForward(int value, ActorRef<Message> receiver) {
        this.personalState = value * 2; // Doubles the value received.
        getContext().getLog().info("Value {} received from {}, doubled new value: {}", value, receiver.path().name(), personalState);
        // Forward the updated value to a neighbor
        ActorRef<Message> neighbor = selectNeighbor(receiver);
        if (neighbor != null) {
            neighbor.tell(new BasicMessage(personalState, getContext().getSelf()));
            getContext().getLog().info("Forwarding new value {} to {}", personalState, neighbor.path().name());
        } else {
            getContext().getLog().info("No neighbor found to forward the value.");
        }
    }

    // Helper method to select a neighbor to which the message will be forwarded
    private ActorRef<Message> selectNeighbor(ActorRef<Message> sender) {
        getContext().getLog().info("Selecting a neighbor to forward the message.");
        // Exclude the sender from the list of neighbors
        List<ActorRef<Message>> otherNeighbors = new ArrayList<>(this.marker.keySet());
        otherNeighbors.remove(sender);
        if (!otherNeighbors.isEmpty()) {
            // Just picking the first neighbor for simplicity, but you could have more complex logic
            return otherNeighbors.get(0);
        }
        return null;
    }

    private Behavior<Message> onInitiateSnapshot(InitiateSnapshot message) {
        if (!recorded) {
            getContext().getLog().info("Initiating snapshot process.");
            takeSnapshot();
        } else {
            getContext().getLog().info("Snapshot initiation ignored, already recorded.");
        }
        return this;
    }

    private Behavior<Message> onBasicMessage(BasicMessage message) {
        getContext().getLog().info("{} received BasicMessage with value: {} from {}",getContext().getSelf().path().name(), message.value, message.from.path().name());
        // If the actor has recorded its state and hasn't received a marker from the sender yet
        if (recorded && !marker.getOrDefault(message.from, false)) {
            getContext().getLog().info("Actor recorded and hasn't received marker from {}. Appending message to state.", message.from.path().name());
            // Append the message to the state queue for the sender's channel
            List<Message> messages = state.get(message.from);
            if (messages == null) {
                messages = new LinkedList<>();
                state.put(message.from, messages);
            }
            messages.add(message);
        }
        if (!recorded || marker.getOrDefault(message.from, true)) {
            getContext().getLog().info("Performing calculation and forwarding for value: {} from {}", message.value, message.from.path().name());
            performCalculationAndForward(message.value, message.from);
        }
        return this;
    }

    private Behavior<Message> onMarkerMessage(MarkerMessage message) {

        if (getContext().getSelf().equals(message.from)) {
            getContext().getLog().info("Received own marker, ignoring.");
            return this;
        }

        // Check if this is the first marker message received, then take a snapshot
        if (!recorded) {
            getContext().getLog().info("{} taking snapshot due to first marker received from {}", getContext().getSelf().path().name(), message.from.path().name());
            takeSnapshot();
        }else {
            getContext().getLog().info("Already in recorded state, marker from {} is ignored for snapshot.", message.from.path().name());
        }

        // Regardless of whether a snapshot was taken, mark that a marker has been received from this sender
        marker.put(message.from, true);

        // If marker messages have been received on all incoming channels, then terminate.
        if (marker.values().stream().allMatch(Boolean::booleanValue)) {
            getContext().getLog().info("All markers received at {}. Preparing to terminate.", getContext().getSelf().path().name());
            return terminate();
        }

        return this;
    }

    private Behavior<Message> onAddNeighbor(AddNeighbor message) {
        // Add the neighbor to the set of neighbors
        this.marker.put(message.neighbor, false); // Initialize marker received status for the new neighbor
        this.state.put(message.neighbor, new LinkedList<>()); // Initialize message queue for the new neighbor

        getContext().getLog().info("{} added as neighbor added to {}", message.neighbor.path().name(), getContext().getSelf().path().name());

        return this;
    }

    private Behavior<Message> terminate() {
        // Implement termination logic. This could involve cleanup or preparing for shutdown.
        getContext().getLog().info("Snapshot protocol complete. Terminating {} actor.", getContext().getSelf().path().name());

        return Behaviors.stopped();
    }

    private void takeSnapshot() {
        if (recorded) {
            // Prevent taking a snapshot again if already recorded
            return;
        }

        getContext().getLog().info("Starting the snapshot process. Current state: {}", personalState);
        // Record the actor's current state
        recorded = true;
        snapshot(); // Take the snapshot

        // Send a marker message to all outgoing channels
        for (ActorRef<Message> neighbor : marker.keySet()) {
            // Reset the state queue for each neighbor, since we are taking a new snapshot
            state.get(neighbor).clear();
            // Set marker received to false for all neighbors since we are initiating the snapshot
            marker.put(neighbor, false);
            // Send the marker message to each neighbor
            neighbor.tell(new MarkerMessage(getContext().getSelf()));
        }

    }

    private void snapshot() {
        getContext().getLog().info("Compiling snapshot data. Personal state: {}", personalState);
        getContext().getLog().info("Taking snapshot...");
        // Capture the local state and in-transit messages
        Map<String, List<Integer>> channelStates = new HashMap<>();
        for (ActorRef<Message> neighbor : state.keySet()) {
            List<Integer> messages = state.get(neighbor).stream()
                    .map(message -> {
                        if (message instanceof BasicMessage) {
                            return ((BasicMessage) message).value;
                        }
                        return null;
                    })
                    .filter(Objects::nonNull) // Filter out any null values
                    .collect(Collectors.toList());
            channelStates.put(neighbor.path().name(), messages);
        }
        // Serialize the channel states for output
        String channelStatesJson = channelStates.entrySet().stream()
                .map(entry -> "\"" + entry.getKey() + "\": " + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
        String formattedTimestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String snapshotContent = String.format(
                "{\"Timestamp\": \"%s\", \"State\": %d, \"ChannelStates\": %s}",
                formattedTimestamp,
                personalState,
                channelStatesJson
        );
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
        String filePath = directoryPath + "/snapshot_" + nodeName + "_" + formattedTimestamp.replace(":", "-").replace("T", "_") + ".json";

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(snapshotContent);
            getContext().getLog().info("Snapshot saved to " + filePath);
        } catch (IOException e) {
            getContext().getLog().error("Failed to save snapshot", e);
        }
    }



}
