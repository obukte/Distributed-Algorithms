package election_algorithms.Franklins_algorithm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

public class FranklinActor extends AbstractBehavior<FranklinActor.Message> {

    public interface Message {}
    public static final class InitializeNeighbors implements Message {
        public final ActorRef<Message> leftNeighbor, rightNeighbor;
        public InitializeNeighbors(ActorRef<Message> left, ActorRef<Message> right) {
            this.leftNeighbor = left;
            this.rightNeighbor = right;
        }
    }
    public static final class ElectionMessage implements Message {
        public final int id, round;
        public final String direction;
        public ElectionMessage(int id, int round, String direction) {
            this.id = id;
            this.round = round;
            this.direction = direction;
        }
    }
    public static final class LeaderElected implements Message {
        public final int leaderId;
        public LeaderElected(int leaderId) { this.leaderId = leaderId; }
    }

    private final int nodeId;
    private ActorRef<Message> leftNeighbor, rightNeighbor;
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

    private Behavior<Message> onInitializeNeighbors(InitializeNeighbors msg) {
        leftNeighbor = msg.leftNeighbor;
        rightNeighbor = msg.rightNeighbor;
        startElection();
        return this;
    }

    private void startElection() {
        ElectionMessage leftMsg = new ElectionMessage(nodeId, roundNumber, "left");
        ElectionMessage rightMsg = new ElectionMessage(nodeId, roundNumber, "right");
        leftNeighbor.tell(leftMsg);
        rightNeighbor.tell(rightMsg);
        getContext().getLog().info("Node {} starting election in round {}", nodeId, roundNumber);
        roundNumber++;
    }

    private Behavior<Message> onElectionMessage(ElectionMessage msg) {
        if (msg.round == roundNumber) {
            if (msg.id > currentLeaderId) {
                currentLeaderId = msg.id;
                broadcastLeaderElected();
            }
            forwardElectionMessage(msg);
        }
        return this;
    }

    private void broadcastLeaderElected() {
        LeaderElected message = new LeaderElected(currentLeaderId);
        leftNeighbor.tell(message);
        rightNeighbor.tell(message);
        getContext().getLog().info("Node {} declares {} as leader", nodeId, currentLeaderId);
        isActive = false;
    }

    private void forwardElectionMessage(ElectionMessage msg) {
        if ("left".equals(msg.direction)) {
            rightNeighbor.tell(msg);
        } else {
            leftNeighbor.tell(msg);
        }
    }

    private Behavior<Message> onLeaderElected(LeaderElected msg) {
        if (isActive) {
            isActive = false;
            getContext().getLog().info("Node {} recognizes {} as the leader", nodeId, msg.leaderId);
            leftNeighbor.tell(msg);
            rightNeighbor.tell(msg);
        }
        return this;
    }
}
