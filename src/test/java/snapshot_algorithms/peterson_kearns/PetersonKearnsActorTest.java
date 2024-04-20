package snapshot_algorithms.peterson_kearns;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class PetersonKearnsActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @BeforeClass
    public static void setup() {
    }

    @AfterClass
    public static void teardown() {
        testKit.shutdownTestKit();
    }


}
