package election_algorithms.DolevklaweRodehAglorithm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.util.HashMap;
import java.util.Map;

public class DolevKlaweRodehActor extends AbstractBehavior<DolevKlaweRodehActor.Message> {

    public interface Message {}

    public static final class StartElection implements Message {}

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

    private final int id;
    private boolean isActive;
    private boolean isLeader;
    private boolean parity;
    private int electionId;
    private Map<Boolean, ActorRef<Message>> neighbors = new HashMap<>();

    private DolevKlaweRodehActor(ActorContext<Message> context, int id) {
        super(context);
        this.id = id;
        this.isActive = false; // Assuming non-initiator by default
        this.isLeader = false;
        this.parity = false; // Assume parity starts as false (even rounds)
        this.electionId = id;
        context.getLog().info("Actor {} initialized, isActive: {}", id, isActive);

    }

    public static final class InitializeRing implements Message {
        final Map<Boolean, ActorRef<Message>> actorRing;
        public InitializeRing(Map<Boolean, ActorRef<Message>> actorRing) {
            this.actorRing = actorRing;
        }
    }

    public static Behavior<Message> create(int id) {
        return Behaviors.setup(context -> new DolevKlaweRodehActor(context, id));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeRing.class, this::onInitializeRing)
                .onMessage(StartElection.class, this::onStartElection)
                .onMessage(ElectionMessage.class, this::onElectionMessage)
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
        this.neighbors = message.actorRing;  // Assuming `actorRing` is correctly typed
        getContext().getLog().info("Ring initialized for Actor {}", id);
        return this;
    }

    private Behavior<Message> onElectionMessage(ElectionMessage message) {
        getContext().getLog().info("Actor {} received ElectionMessage from Actor {}, electionId: {}", id, message.sender.path().name(), message.electionId);
        if (this.isActive) {
            // Update the electionId if the incoming ID is larger
            if (message.electionId > this.electionId) {
                getContext().getLog().info("Actor {} updating electionId from {} to {}", id, electionId, message.electionId);
                this.electionId = message.electionId;
                // Toggle parity after sending message to both neighbors
                this.parity = !this.parity;
                neighbors.values().forEach(neighbor -> neighbor.tell(new ElectionMessage(this.electionId, getContext().getSelf(), this.parity)));
            } else if (message.electionId < this.electionId) {
                // If the incoming ID is smaller, do nothing
            } else {
                // If the incoming ID is equal to this actor's electionId, it might be the leader
                if (message.sender != getContext().getSelf()) {
                    this.isLeader = true;
                    getContext().getLog().info("Actor {} is elected as leader", this.id);
                    // Propagate leadership acknowledgment
                    neighbors.values().forEach(neighbor -> neighbor.tell(message));
                }
            }
        } else {
            // If not active, just forward the message
            getContext().getLog().info("Actor {} forwarding message as it is inactive", id);
            neighbors.values().forEach(neighbor -> neighbor.tell(message));
        }
        return this;
    }

}
