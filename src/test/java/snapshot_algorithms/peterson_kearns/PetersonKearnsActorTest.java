package snapshot_algorithms.peterson_kearns;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class PetersonKearnsActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @BeforeClass
    public static void setup() {
    }

    @AfterClass
    public static void teardown() {
        testKit.shutdownTestKit();
    }

    @Test
    public void testMessageHandlingAndStateUpdate() {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        ActorRef<PetersonKearnsActor.Message> actor = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0));

        actor.tell(new PetersonKearnsActor.BasicMessage(10, probe.ref(), new HashMap<>()));
    }


    @Test
    public void testSnapshotTakingAndRecovery() {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        ActorRef<PetersonKearnsActor.Message> actor = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0));

        actor.tell(new PetersonKearnsActor.InitiateSnapshot());
    }

    @Test
    public void testMessageVectorClockLogging() {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        ActorRef<PetersonKearnsActor.Message> actor = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0));

        Map<String, Integer> vectorClock = new HashMap<>();
        vectorClock.put("someActor", 1);
        actor.tell(new PetersonKearnsActor.BasicMessage(5, probe.ref(), vectorClock));
    }

    @Test
    public void testActorTerminationEffects() {
        TestProbe<PetersonKearnsActor.Message> probe = testKit.createTestProbe();
        ActorRef<PetersonKearnsActor.Message> actor = testKit.spawn(PetersonKearnsActor.create(new HashSet<>(), 0));

        actor.tell(new PetersonKearnsActor.TerminateActor());
    }

}
