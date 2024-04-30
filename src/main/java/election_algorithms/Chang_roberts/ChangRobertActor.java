package election_algorithms.Chang_roberts;


import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import java.util.Map;

public class ChangRobertActor extends AbstractBehavior<ChangRobertActor.Message> {

    // Define an interface for all messages that can be handled by this actor.
    public interface Message {}

    // Message to initialize the ring topology with references to other actors.
    public static final class InitializeRing implements Message {
        final Map<Integer, ActorRef<Message>> actorRing;
        public InitializeRing(Map<Integer, ActorRef<Message>> actorRing) {
            this.actorRing = actorRing;
        }
    }

    // Message to set the next actor in the ring. This helps in constructing the circular topology.
    public static final class SetNextActor implements Message {
        final ActorRef<Message> nextActor;
        public SetNextActor(ActorRef<Message> nextActor) {
            this.nextActor = nextActor;
        }
    }

    // Message to start the election process.
    public static final class StartElection implements Message {
        final int id;
        public StartElection(int id) {
            this.id = id;
        }
    }

    // Message to pass election information (ID of the contender) around the ring.
    public static final class ElectionMessage implements Message {
        final int id;
        public ElectionMessage(int id) {
            this.id = id;
        }
    }

    // Message to announce the elected leader's ID once the election is concluded.
    public static final class Elected implements Message {
        final int leaderId;
        public Elected(int leaderId) {
            this.leaderId = leaderId;
        }
    }

    // The unique ID for this actor.
    private final int myId;
    // Reference to the next actor in the ring, needed for passing messages.
    private ActorRef<Message> nextActor;

    // Constructor for ChangRobertActor; it is private to enforce the use of the 'create' factory method.
    private ChangRobertActor(ActorContext<Message> context, int myId) {
        super(context);
        this.myId = myId;
    }

    // Static method to create an instance of ChangRobertActor. This encapsulates actor initialization.
    public static Behavior<Message> create(int id) {
        return Behaviors.setup(context -> new ChangRobertActor(context, id));
    }

    // Define how this actor handles the different types of messages it receives.
    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeRing.class, this::onInitializeRing)
                .onMessage(SetNextActor.class, this::onSetNextActor)
                .onMessage(StartElection.class, this::onStartElection)
                .onMessage(ElectionMessage.class, this::onElectionMessage)
                .onMessage(Elected.class, this::onElected)
                .build();
    }

    // Handle the InitializeRing message to set the next actor based on the actorRing mapping.
    private Behavior<Message> onInitializeRing(InitializeRing message) {
        this.nextActor = message.actorRing.get((myId + 1) % message.actorRing.size());
        getContext().getLog().info("Ring initialized. Actor {} set next actor to {}", myId, nextActor.path().name());
        return this;
    }

    // Handle the SetNextActor message to directly set the next actor reference.
    private Behavior<Message> onSetNextActor(SetNextActor message) {
        this.nextActor = message.nextActor;
        getContext().getLog().info("Next actor for {} set to {}", myId, message.nextActor.path().name());
        return this;
    }

    // Handle the StartElection message by sending an ElectionMessage to the next actor.
    private Behavior<Message> onStartElection(StartElection message) {
        nextActor.tell(new ElectionMessage(message.id));
        getContext().getLog().info("Starting election with ID {} from Actor {}", message.id, myId);
        return this;
    }

    // Handle receiving an ElectionMessage. If the incoming ID is higher, pass it on. If lower, send own ID. If the same, declare leadership.
    private Behavior<Message> onElectionMessage(ElectionMessage message) {
        if (message.id > myId) {
            nextActor.tell(message);
        } else if (message.id < myId) {
            nextActor.tell(new ElectionMessage(myId));
        } else {
            nextActor.tell(new Elected(myId));
            getContext().getLog().info("Actor {} is the leader and is sending victory message", myId);
        }
        return this;
    }

    // Handle the Elected message to forward the leader's ID around the ring or acknowledge leadership if the message returns to the leader.
    private Behavior<Message> onElected(Elected message) {
        if (message.leaderId != myId) {
            nextActor.tell(message);
            getContext().getLog().info("Actor {} acknowledged leader ID: {}", myId, message.leaderId);
        } else {
            getContext().getLog().info("Actor {} has received its own leader election message, confirming leadership", myId);
        }
        return this;
    }
}