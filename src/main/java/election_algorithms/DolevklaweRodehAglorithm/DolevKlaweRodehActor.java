package election_algorithms.DolevklaweRodehAglorithm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.util.HashMap;
import java.util.Map;

public class DolevKlaweRodehActor extends AbstractBehavior<DolevKlaweRodehActor.Message> {

    public interface Message {
    }

    public static final class StartElection implements Message {
    }

    public static final class ElectionMessage implements Message {
        public final int electionId;
        public final ActorRef<Message> sender;
        public final boolean parity;

        public ElectionMessage(int electionId, ActorRef<Message> sender, boolean parity) {
            this.electionId = electionId;
            this.sender = sender;
            this.parity = parity;
        }
    }

    public static final class LeaderElectedMessage implements Message {
        public final int leaderId;

        public LeaderElectedMessage(int leaderId) {
            this.leaderId = leaderId;
        }
    }

    private final int id;
    private boolean isActive;
    private boolean parity;
    private boolean isLeader;
    private int electionId;
    private Map<Boolean, ActorRef<Message>> neighbors = new HashMap<>();

    private DolevKlaweRodehActor(ActorContext<Message> context, int id) {
        super(context);
        this.id = id;
        this.isActive = false;
        this.parity = false;
        this.electionId = id;
        context.getLog().info("Actor {} initialized, isActive: {}", id, isActive);
    }

    public static Behavior<Message> create(int id) {
        return Behaviors.setup(context -> new DolevKlaweRodehActor(context, id));
    }

    public static final class InitializeRing implements Message {
        final Map<Boolean, ActorRef<Message>> actorRing;

        public InitializeRing(Map<Boolean, ActorRef<Message>> actorRing) {
            this.actorRing = actorRing;
        }
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeRing.class, this::onInitializeRing)
                .onMessage(StartElection.class, this::onStartElection)
                .onMessage(ElectionMessage.class, this::onElectionMessage)
                .onMessage(LeaderElectedMessage.class, this::onLeaderElected)
                .build();
    }

    private Behavior<Message> onStartElection(StartElection message) {
        isActive = true;
        getContext().getLog().info("Actor {} starting election, isActive set to true", id);
        neighbors.values().forEach(neighbor -> {
            getContext().getLog().info("Actor {} sending initial election message to neighbors", id);
            neighbor.tell(new ElectionMessage(id, getContext().getSelf(), parity));
        });
        return this;
    }

    private Behavior<Message> onInitializeRing(InitializeRing message) {
        this.neighbors = message.actorRing;
        getContext().getLog().info("Ring initialized for Actor {}", id);
        return this;
    }

    private Behavior<Message> onElectionMessage(ElectionMessage message) {
        getContext().getLog().info("Actor {} received ElectionMessage from Actor {}, electionId: {}, parity: {}", id, message.sender.path().name(), message.electionId, message.parity);

        if (this.isLeader) {
            getContext().getLog().info("Actor {} is the leader and will not forward messages", id);
            return this;  // If already leader, ignore all messages.
        }

        // Check if the message should change the actor's state
        if (message.electionId > this.electionId) {
            this.isActive = false;
            this.electionId = message.electionId;
            this.parity = message.parity;  // Adopt the message's parity directly
            getContext().getLog().info("Actor {} recognizes a higher ID: {}, becoming passive, adopts parity", id, this.electionId);
        } else if (message.electionId == this.electionId && message.parity == this.parity && message.sender.equals(getContext().getSelf())) {
            this.isLeader = true;
            this.isActive = false;
            getContext().getLog().info("Actor {} has received its own ID and is now the leader", id);
            broadcastLeaderElection();
            return this; // Stop processing as the actor is now the leader
        }
        // Always forward the message using the current state
        forwardElectionMessage(new ElectionMessage(this.electionId, getContext().getSelf(), this.parity));

        return this;
    }


    private Behavior<Message> onLeaderElected(LeaderElectedMessage message) {
        if (message.leaderId != this.id) {
            this.isLeader = false;
            this.isActive = false;
            getContext().getLog().info("Actor {} recognizes Actor {} as the leader and becomes passive.", id, message.leaderId);
        }
        return this;
    }


    private void forwardElectionMessage(ElectionMessage message) {
        if (!this.isLeader && (this.isActive || this.parity == message.parity)) {
            ActorRef<Message> nextNeighbor = this.neighbors.get(true);
            getContext().getLog().info("Actor {} forwarding message with ID {} and parity {} to the next neighbor", id, message.electionId, message.parity);
            nextNeighbor.tell(new ElectionMessage(message.electionId, getContext().getSelf(), message.parity));
        }
    }

    private void broadcastLeaderElection() {
        LeaderElectedMessage leaderElectedMessage = new LeaderElectedMessage(this.id);
        for (ActorRef<Message> neighbor : this.neighbors.values()) {
            neighbor.tell(leaderElectedMessage);
            getContext().getLog().info("Actor {} has broadcasted its election as leader to neighbor {}", this.id, neighbor.path().name());
        }
    }
}