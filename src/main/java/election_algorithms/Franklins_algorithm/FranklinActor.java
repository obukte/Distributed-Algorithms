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

    public static final class UpdateStatus implements Message {
        public final boolean isActive;
        public UpdateStatus(boolean isActive) {
            this.isActive = isActive;
        }
    }

    private final int nodeId;
    private ActorRef<Message> leftNeighbor, rightNeighbor;
    private boolean isActive = true;
    private int currentLeaderId = -1;
    private int roundNumber = 0;

    // Track the IDs of the neighbors to compare in the election
    private Integer leftNeighborId = null;
    private Integer rightNeighborId = null;

    public FranklinActor(ActorContext<Message> context, int nodeId, int leftId, int rightId) {
        super(context);
        this.nodeId = nodeId;
        this.leftNeighborId = leftId;
        this.rightNeighborId = rightId;
    }

    public static Behavior<Message> create(int nodeId, int leftNeighborId, int rightNeighborId) {
        return Behaviors.setup(context -> new FranklinActor(context, nodeId, leftNeighborId, rightNeighborId));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeNeighbors.class, this::onInitializeNeighbors)
                .onMessage(ElectionMessage.class, this::onElectionMessage)
                .onMessage(LeaderElected.class, this::onLeaderElected)
                .onMessage(UpdateStatus.class, this::onUpdateStatus)
                .build();
    }

    // Method to update the status of this node when a neighbor declares itself as leader
    private Behavior<Message> onUpdateStatus(UpdateStatus msg) {
        isActive = msg.isActive;
        if (!isActive) {
            getContext().getLog().info("Node {} has become passive", nodeId);
        }
        return this;
    }

    private Behavior<Message> onInitializeNeighbors(InitializeNeighbors msg) {
        leftNeighbor = msg.leftNeighbor;
        rightNeighbor = msg.rightNeighbor;
        startElection();
        return this;
    }

    private void startElection() {
        getContext().getLog().info("Node {} evaluating if it should start an election", nodeId);
        // Determine if the node should be active or passive before sending messages
        if ((leftNeighborId != null && nodeId > leftNeighborId) && (rightNeighborId != null && nodeId > rightNeighborId)) {
            isActive = true; // Remain active if the node's ID is higher than both neighbors'
            ElectionMessage leftMsg = new ElectionMessage(nodeId, roundNumber, "left");
            ElectionMessage rightMsg = new ElectionMessage(nodeId, roundNumber, "right");
            leftNeighbor.tell(leftMsg);
            rightNeighbor.tell(rightMsg);
                getContext().getLog().info("Node {} starting election as active in round {}", nodeId, roundNumber);
        } else {
            isActive = false; // Become passive if either neighbor has a higher ID
            getContext().getLog().info("Node {} becomes passive, among its neighbors {}, {}", nodeId, leftNeighborId, rightNeighborId);
        }
    }

    private Behavior<Message> onElectionMessage(ElectionMessage msg) {
        getContext().getLog().info("Node {} received ElectionMessage from {}, round {}", nodeId, msg.id, msg.round);
        if (isActive && msg.round == roundNumber) {

            if (msg.id == nodeId) {
                // If the node receives its own ID, it's the leader
                currentLeaderId = nodeId;
                getContext().getLog().info("Node {} recognizes itself as the leader and broadcasts this info", nodeId);
                broadcastLeaderElected();
            } else if (msg.id > nodeId) {
                // If a neighbor's ID is greater, this node becomes passive
                isActive = false;
                getContext().getLog().info("Node {} becomes passive due to higher ID from {}", nodeId, msg.id);
                roundNumber++;
                ElectionMessage leftMsg = new ElectionMessage(msg.id, roundNumber, "left");
                ElectionMessage rightMsg = new ElectionMessage(msg.id, roundNumber, "right");
                leftNeighbor.tell(leftMsg);
                rightNeighbor.tell(rightMsg);
                getContext().getLog().info("Node {} starting election as active in round {}", nodeId, roundNumber);
            } else {
                startElection();
                getContext().getLog().info("Node {} continues election with higher ID than {}", nodeId, msg.id);
            }
        }
        // If a node is passive, it simply forwards the message
        else if (!isActive) {
            getContext().getLog().info("Node {} is passive and forwards the received message", nodeId);
            forwardElectionMessage(msg);
        }
        return this;
    }

    private void broadcastLeaderElected() {
        getContext().getLog().info("Node {} is broadcasting LeaderElected message as it declares itself leader", nodeId);
        LeaderElected message = new LeaderElected(currentLeaderId);
        leftNeighbor.tell(message);
        rightNeighbor.tell(message);
        getContext().getLog().info("Node {} has notified neighbors about leadership", nodeId);
        isActive = false;
    }

    private void forwardElectionMessage(ElectionMessage msg) {
        getContext().getLog().info("Node {} forwarding ElectionMessage from {} to the next neighbor", nodeId, msg.id);
        if ("left".equals(msg.direction)) {
            rightNeighbor.tell(msg);
        } else {
            leftNeighbor.tell(msg);
        }
    }

    private Behavior<Message> onLeaderElected(LeaderElected msg) {
        getContext().getLog().info("Node {} received LeaderElected message, leader is {}", nodeId, msg.leaderId);
        if (isActive || currentLeaderId != msg.leaderId) {  // Check to avoid message looping
            isActive = false;
            currentLeaderId = msg.leaderId;
            leftNeighbor.tell(msg);
            rightNeighbor.tell(msg);
            getContext().getLog().info("Node {} forwarding LeaderElected message", nodeId);
        }
        return this;
    }
}
