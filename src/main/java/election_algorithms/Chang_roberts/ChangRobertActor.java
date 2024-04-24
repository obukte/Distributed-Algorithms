package election_algorithms;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import java.util.Map;

public class ChangRobertActor extends AbstractBehavior<ChangRobertActor.Message> {

    public interface Message {}

    public static final class InitializeRing implements Message {
        final Map<Integer, ActorRef<Message>> actorRing;
        public InitializeRing(Map<Integer, ActorRef<Message>> actorRing) {
            this.actorRing = actorRing;
        }
    }

    public static final class SetNextActor implements Message {
        final ActorRef<Message> nextActor;
        public SetNextActor(ActorRef<Message> nextActor) {
            this.nextActor = nextActor;
        }
    }

    public static final class StartElection implements Message {
        final int id;
        public StartElection(int id) {
            this.id = id;
        }
    }

    public static final class ElectionMessage implements Message {
        final int id;
        public ElectionMessage(int id) {
            this.id = id;
        }
    }

    public static final class Elected implements Message {
        final int leaderId;
        public Elected(int leaderId) {
            this.leaderId = leaderId;
        }
    }

    private final int myId;
    private ActorRef<Message> nextActor;

    private ChangRobertActor(ActorContext<Message> context, int myId) {
        super(context);
        this.myId = myId;
    }

    public static Behavior<Message> create(int id) {
        return Behaviors.setup(context -> new ChangRobertActor(context, id));
    }

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

    private Behavior<Message> onInitializeRing(InitializeRing message) {
        this.nextActor = message.actorRing.get((myId + 1) % message.actorRing.size());
        getContext().getLog().info("Ring initialized. Actor {} set next actor to {}", myId, nextActor.path().name());
        return this;
    }

    private Behavior<Message> onSetNextActor(SetNextActor message) {
        this.nextActor = message.nextActor;
        getContext().getLog().info("Next actor for {} set to {}", myId, message.nextActor.path().name());
        return this;
    }

    private Behavior<Message> onStartElection(StartElection message) {
        nextActor.tell(new ElectionMessage(message.id));
        getContext().getLog().info("Starting election with ID {} from Actor {}", message.id, myId);
        return this;
    }

    private Behavior<Message> onElectionMessage(ElectionMessage message) {
        getContext().getLog().info("Received election message with ID: {} at Actor with ID: {}", message.id, this.myId);
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
