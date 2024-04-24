package election_algorithms.Franklins_algorithm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

public class FranklinActor extends AbstractBehavior<FranklinActor.Message> {

    public interface Message {
    }

    public static final class InitializeNeighbors implements Message {
        public final ActorRef<Message> leftNeighbor;
        public final ActorRef<Message> rightNeighbor;

        public InitializeNeighbors(ActorRef<Message> leftNeighbor, ActorRef<Message> rightNeighbor) {
            this.leftNeighbor = leftNeighbor;
            this.rightNeighbor = rightNeighbor;
        }
    }

    public static final class ElectionMessage implements Message {
        public final int id;
        public final int round;

        public ElectionMessage(int id, int round) {
            this.id = id;
            this.round = round;
        }
    }

    public static final class LeaderElected implements Message {
        public final int leaderId;

        public LeaderElected(int leaderId) {
            this.leaderId = leaderId;
        }
    }

    private final int nodeId;
    private ActorRef<Message> leftNeighbor;
    private ActorRef<Message> rightNeighbor;
    private boolean isActive = true;
    private int currentLeaderId = -1;
    private int roundNumber = 0;

    public FranklinActor(ActorContext<Message> context, int nodeId) {
        super(context);
        this.nodeId = nodeId;
    }

    public static Behavior<Message> create(int nodeId) {
        return Behaviors.setup(context -> new FranklinActor(context, nodeId));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeNeighbors.class, this::onInitializeNeighbors)
                .onMessage(ElectionMessage.class, this::onElectionMessage)
                .onMessage(LeaderElected.class, this::onLeaderElected)
                .build();
    }

    private Behavior<Message> onInitializeNeighbors(InitializeNeighbors Message) {
        this.leftNeighbor = Message.leftNeighbor;
        this.rightNeighbor = Message.rightNeighbor;
        getContext().getLog().info("Node {} initialized with left neighbor {} and right neighbor {}", nodeId, leftNeighbor.path().name(), rightNeighbor.path().name());
        // Each node starts the election by sending its ID to both neighbors.
        startElection();
        return this;
    }

    private void startElection() {
        ElectionMessage message = new ElectionMessage(nodeId, roundNumber);
        leftNeighbor.tell(message);
        rightNeighbor.tell(message);
        getContext().getLog().info("Node {} starting election in round {}", nodeId, roundNumber);
    }

    private Behavior<Message> onElectionMessage(ElectionMessage message) {
        if (message.round == roundNumber) {
            if (message.id > currentLeaderId) {
                currentLeaderId = message.id;
                if (isActive) {
                    isActive = false; // Become passive
                    broadcastLeaderElected(); // Broadcast only if this node is becoming passive
                }
            }
            if (isActive && message.id == nodeId) {
                broadcastLeaderElected(); // This node is the leader
                isActive = false; // The node becomes passive after election
            }
        }
        return this;
    }

    private Behavior<Message> onLeaderElected(LeaderElected Message) {
        getContext().getLog().info("Node {} received LeaderElected(leaderId={})", nodeId, Message.leaderId);
        if (isActive) {
            isActive = false; // Node becomes passive upon learning of the leader.
            leftNeighbor.tell(Message);
            rightNeighbor.tell(Message);
        }
        return this;
    }

    private void broadcastLeaderElected() {
        LeaderElected message = new LeaderElected(currentLeaderId);
        leftNeighbor.tell(message);
        rightNeighbor.tell(message);
    }
}