package election_algorithms.DolevklaweRodehAglorithm;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import java.util.Map;
public class DolevKlaweRodehActor extends AbstractBehavior<DolevKlaweRodehActor.Message> {

    public interface Message {}

    public static final class InitializeRing implements Message {
        final Map<Integer, ActorRef<Message>> actorRing;
        public InitializeRing(Map<Integer, ActorRef<Message>> actorRing) {
            this.actorRing = actorRing;
        }
    }

    public static final class ElectionMessage implements Message {
        final int id;
        final int round;
        final boolean parity;
        public ElectionMessage(int id, int round, boolean parity) {
            this.id = id;
            this.round = round;
            this.parity = parity;
        }
    }

    private final int myId;
    private ActorRef<Message> nextActor;
    private int maxId;
    private int round = 0;
    private boolean parity = false;

    private DolevKlaweRodehActor(ActorContext<Message> context, int myId) {
        super(context);
        this.myId = myId;
        this.maxId = myId;
    }

    public static Behavior<Message> create(int id) {
        return Behaviors.setup(context -> new DolevKlaweRodehActor(context, id));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(InitializeRing.class, this::onInitializeRing)
                .onMessage(ElectionMessage.class, this::onElectionMessage)
                .build();
    }

    private Behavior<Message> onInitializeRing(InitializeRing message) {
        this.nextActor = message.actorRing.get((myId + 1) % message.actorRing.size());
        getContext().getLog().info("Ring initialized for Actor {}", myId);
        return this;
    }

    private Behavior<Message> onElectionMessage(ElectionMessage message) {
        getContext().getLog().info("Actor {} received election message with ID: {}, Round: {}, Parity: {}", myId, message.id, message.round, message.parity);
        if (message.id > maxId) {
            maxId = message.id;
            round = message.round;
            parity = message.parity;
            sendElectionMessage();
        } else if (message.id == maxId && message.round > round) {
            round = message.round;
            parity = message.parity;
            sendElectionMessage();
        } else if (message.id == maxId && message.round == round && message.parity != parity) {
            decideLeader();
        }
        return this;
    }

    private void decideLeader() {
        if (maxId == myId) {
            getContext().getLog().info("Actor {} is the leader.", myId);
        }
    }

    private void sendElectionMessage() {
        nextActor.tell(new ElectionMessage(maxId, round, !parity));
        getContext().getLog().info("Actor {} sends election message with ID: {}, Round: {}, Parity: {}", myId, maxId, round, !parity);
    }
}