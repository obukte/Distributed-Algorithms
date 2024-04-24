package election_algorithms.echo_algorithm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import java.util.Map;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;

public class EchoWithExtinctionActor extends AbstractBehavior<EchoWithExtinctionActor.Message> {

    public interface Message {}
    public static final class InitializeNeighbors implements Message {
        final Map<Integer, ActorRef<Message>> neighbors;

        public InitializeNeighbors(Map<Integer, ActorRef<Message>> neighbors) {
            this.neighbors = neighbors;
        }
    }
    public static final class StartElection implements Message {
        public final int initiatorId;
        public StartElection(int initiatorId) {
            this.initiatorId = initiatorId;
        }
    }

    public static final class WaveMessage implements Message {
        public final int senderId;
        public final int waveId;
        public WaveMessage(int senderId, int waveId) {
            this.senderId = senderId;
            this.waveId = waveId;
        }
    }

    public static final class LeaderElected implements Message {
        public final int leaderId;
        public LeaderElected(int leaderId) {
            this.leaderId = leaderId;
        }
    }

    private final int nodeId;
    private final Map<Integer, ActorRef<Message>> neighbors;
    private boolean electionInProgress = false;
    private boolean leaderAcknowledged = false;

    private int maxWaveId = -1;
    private final TestProbe<LeaderElected> testProbe;

    public EchoWithExtinctionActor(ActorContext<Message> context, int nodeId, Map<Integer, ActorRef<Message>> neighbors, TestProbe<LeaderElected> testProbe) {
        super(context);
        this.nodeId = nodeId;
        this.neighbors = neighbors;
        this.testProbe = testProbe;
    }

    public static Behavior<Message> create(int nodeId, Map<Integer, ActorRef<Message>> neighbors, TestProbe<LeaderElected> testProbe) {
        return Behaviors.setup(context -> new EchoWithExtinctionActor(context, nodeId, neighbors, testProbe));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeNeighbors.class, this::onInitializeNeighbors)
                .onMessage(StartElection.class, this::onStartElection)
                .onMessage(WaveMessage.class, this::onWaveMessage)
                .onMessage(LeaderElected.class, this::onLeaderElected)
                .build();
    }

    private Behavior<Message> onInitializeNeighbors(InitializeNeighbors message) {
        if (neighbors.isEmpty()) { // Check if neighbors are already initialized
            this.neighbors.putAll(message.neighbors);
        }
        return this;
    }


    private Behavior<Message> onStartElection(StartElection Message) {
        if (!electionInProgress) {
            electionInProgress = true; //Initiating a new election
            maxWaveId = Message.initiatorId;
            neighbors.values().forEach(neighbor -> neighbor.tell(new WaveMessage(nodeId, maxWaveId)));
        }
        return this;
    }

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


    private Behavior<Message> onLeaderElected(LeaderElected message) {
        if (!leaderAcknowledged) {
            leaderAcknowledged = true;
            electionInProgress = false;

            if (message.leaderId != nodeId) {
                neighbors.values().forEach(neighbor -> neighbor.tell(message));
                getContext().getLog().info("Node {} acknowledged leader ID: {}", nodeId, message.leaderId);
            } else {
                getContext().getLog().info("Node {} has been elected as leader", nodeId);
                // Inform the test probe that the leader has been elected
                testProbe.getRef().tell(new LeaderElected(nodeId));
            }
        }
        return this;
    }
}

