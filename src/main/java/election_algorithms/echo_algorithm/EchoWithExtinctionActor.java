package election_algorithms.echo_algorithm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import java.util.Map;
import akka.actor.testkit.typed.javadsl.TestProbe;

public class EchoWithExtinctionActor extends AbstractBehavior<EchoWithExtinctionActor.Message> {
    // Define the interface for all messages that can be handled by this actor.
    public interface Message {}

    // Message to initialize the neighbors of the actor.
    public static final class InitializeNeighbors implements Message {
        final Map<Integer, ActorRef<Message>> neighbors;

        public InitializeNeighbors(Map<Integer, ActorRef<Message>> neighbors) {
            this.neighbors = neighbors;
        }
    }

    // Message to start the election process.
    public static final class StartElection implements Message {
        public final int initiatorId;
        public StartElection(int initiatorId) {
            this.initiatorId = initiatorId;
        }
    }

    // Message to propagate the wave during the election process.
    public static final class WaveMessage implements Message {
        public final int senderId;
        public final int waveId;
        public WaveMessage(int senderId, int waveId) {
            this.senderId = senderId;
            this.waveId = waveId;
        }
    }

    // Message to announce the elected leader.
    public static final class LeaderElected implements Message {
        public final int leaderId;
        public LeaderElected(int leaderId) {
            this.leaderId = leaderId;
        }
    }

    // Member variables
    private final int nodeId; //The ID of the current actor.

    private final Map<Integer, ActorRef<Message>> neighbors; //Mapping of neighbors node IDs to their corresponding actor ref
    private boolean electionInProgress = false; //Flag indicating whether an election process is currently in progress
    private boolean leaderAcknowledged = false; // Flag indicating whether the leader has been acknowledged

    private int maxWaveId = -1; //
    private final TestProbe<LeaderElected> testProbe;

    public EchoWithExtinctionActor(ActorContext<Message> context, int nodeId, Map<Integer, ActorRef<Message>> neighbors, TestProbe<LeaderElected> testProbe) {
        super(context);
        this.nodeId = nodeId;
        this.neighbors = neighbors;
        this.testProbe = testProbe;
    }

    // Factory method to create an instance of the actor
    public static Behavior<Message> create(int nodeId, Map<Integer, ActorRef<Message>> neighbors, TestProbe<LeaderElected> testProbe) {
        return Behaviors.setup(context -> new EchoWithExtinctionActor(context, nodeId, neighbors, testProbe));
    }

    // Define behavior for receiving messages
    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeNeighbors.class, this::onInitializeNeighbors)
                .onMessage(StartElection.class, this::onStartElection)
                .onMessage(WaveMessage.class, this::onWaveMessage)
                .onMessage(LeaderElected.class, this::onLeaderElected)
                .build();
    }

    // Handler for the InitializeNeighbors message
    private Behavior<Message> onInitializeNeighbors(InitializeNeighbors message) {
        if (neighbors.isEmpty()) { // Check if neighbors are already initialized
            this.neighbors.putAll(message.neighbors);
        }
        return this;
    }

    // Handler for the StartElection message
    private Behavior<Message> onStartElection(StartElection Message) {
        if (!electionInProgress) {
            electionInProgress = true; //Initiating a new election
            maxWaveId = Message.initiatorId;
            neighbors.values().forEach(neighbor -> neighbor.tell(new WaveMessage(nodeId, maxWaveId)));
        }
        return this;
    }

    // Handler for the WaveMessage message
    private Behavior<Message> onWaveMessage(WaveMessage message) {
        if (message.waveId > maxWaveId) {
            maxWaveId = message.waveId; // Propagating the wave further
            if (!leaderAcknowledged) {
                neighbors.values().forEach(neighbor -> neighbor.tell(message));
            }
        } else if (message.waveId == maxWaveId && !leaderAcknowledged) {
            // Acknowledge the leader
            getContext().getSelf().tell(new LeaderElected(maxWaveId));
        }
        return this;
    }

    // Handler for the LeaderElected message
    private Behavior<Message> onLeaderElected(LeaderElected message) {
        if (!leaderAcknowledged) {
            leaderAcknowledged = true;
            electionInProgress = false;

            if (message.leaderId != nodeId) {
                neighbors.values().forEach(neighbor -> neighbor.tell(message));
                getContext().getLog().info("Node {} acknowledged leader ID: {}", nodeId, message.leaderId);
            } else {
                System.out.println("verified all the other nodes for the highest ID");
                getContext().getLog().info("Node {} has been elected as leader", nodeId);
                // Inform the test probe that the leader has been elected
                testProbe.getRef().tell(new LeaderElected(nodeId));
            }
        }
        return this;
    }
}

