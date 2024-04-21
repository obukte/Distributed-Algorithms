package snapshot_algorithms;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import com.typesafe.config.Config;
import snapshot_algorithms.chandy_lamport.ChandyLamportActor;
import snapshot_algorithms.lai_yang.LaiYangActor;
import snapshot_algorithms.peterson_kearns.CheckpointRecoveryManager;
import snapshot_algorithms.peterson_kearns.PetersonKearnsActor;
import util.GraphParser;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final String TEST_FILE_PATH = "src/main/resources/graph/NetGraph_17-03-24-12-50-04.ngs.dot";
    static ActorTestKit testKit = ActorTestKit.create();

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                System.out.println("Choose the snapshot algorithm to simulate:");
                System.out.println("1: Lai-Yang");
                System.out.println("2: Chandy-Lamport");
                System.out.println("3: Peterson-Kearns");
                System.out.println("4: Exit");

                String choice = scanner.nextLine();
                switch (choice) {
                    case "1":
                        runLaiYang();
                        break;
                    case "2":
                        runChandyLamport();
                        break;
                    case "3":
                        runPetersonKearns();
                        break;
                    case "4":
                        System.out.println("Exiting...");
                        System.exit(0);  // This will forcibly terminate the JVM
                    default:
                        System.out.println("Invalid option. Please enter 1, 2, 3, or 4.");
                }
            }
        } finally {
            scanner.close(); // Ensure the scanner is closed to free resources
        }
    }

    private static void runLaiYang() throws InterruptedException {
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(TEST_FILE_PATH);

        Map<String, ActorRef<Message>> network = new HashMap<>();

        edges.forEach(edge -> {
            network.computeIfAbsent(edge.getSource(), sourceId -> testKit.spawn(LaiYangActor.create(new HashSet<>()), sourceId));
            network.computeIfAbsent(edge.getDestination(), destId -> testKit.spawn(LaiYangActor.create(new HashSet<>()), destId));
        });

        edges.forEach(edge -> {
            ActorRef<Message> sourceNode = network.get(edge.getSource());
            ActorRef<Message> destinationNode = network.get(edge.getDestination());
            sourceNode.tell(new LaiYangActor.AddNeighbor(destinationNode));
        });

        // Assuming the initial snapshot trigger is from node "0"
        ActorRef<Message> initNode = network.get("0");
        ActorRef<Message> nodeSeven = network.get("7");

        initNode.tell(new LaiYangActor.PerformCalculation(10));
        nodeSeven.tell(new LaiYangActor.PerformCalculation(2));

        initNode.tell(new LaiYangActor.InitiateSnapshot());
        nodeSeven.tell(new LaiYangActor.PerformCalculation(3));

        // Allow some time for the snapshot process to complete
        Thread.sleep(10000);

        System.out.println("Lai-yang simulation ended...");
    }

    private static void runChandyLamport() throws InterruptedException {
        List<GraphParser.Edge> edges = GraphParser.parseDotFile(TEST_FILE_PATH);

        Map<String, ActorRef<Message>> network = new HashMap<>();

        edges.forEach(edge -> {
            network.computeIfAbsent(edge.getSource(), sourceId -> testKit.spawn(ChandyLamportActor.create(new HashSet<>()), sourceId));
            network.computeIfAbsent(edge.getDestination(), destId -> testKit.spawn(ChandyLamportActor.create(new HashSet<>()), destId));
        });

        edges.forEach(edge -> {
            ActorRef<Message> sourceNode = network.get(edge.getSource());
            ActorRef<Message> destinationNode = network.get(edge.getDestination());
            sourceNode.tell(new ChandyLamportActor.AddNeighbor(destinationNode));
        });

        // Assuming the initial snapshot trigger is from node "0"
        ActorRef<Message> initNode = network.get("0");

        initNode.tell(new ChandyLamportActor.InitiateSnapshot());

        // Allow some time for the snapshot process to complete
        Thread.sleep(10000);

        System.out.println("Chandy-Lamport simulation ended...");
    }


    public static void runPetersonKearns() throws InterruptedException, ExecutionException {
        // Create the actor system and the checkpoint manager actor
        ActorSystem<CheckpointRecoveryManager.Command> system = ActorSystem.create(CheckpointRecoveryManager.create(), "System");

        // Send command to build the network
        system.tell(new CheckpointRecoveryManager.BuildNetworkFromDotFile(TEST_FILE_PATH));

        Thread.sleep(1000);

        CompletionStage<ActorRef<Message>> actor0Future = AskPattern.ask(
                system,
                replyTo -> new CheckpointRecoveryManager.GetActorRef("0", replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        );

        CompletionStage<ActorRef<Message>> actor1Future = AskPattern.ask(
                system,
                replyTo -> new CheckpointRecoveryManager.GetActorRef("1", replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        );

        ActorRef<Message> actor0 = actor0Future.toCompletableFuture().get();
        ActorRef<Message> actor1 = actor1Future.toCompletableFuture().get();

        // Send messages to actors to change state
        actor0.tell(new PetersonKearnsActor.BasicMessage(10, actor1, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));

        // Allow time for messages to be processed
        Thread.sleep(2000);
        // Initiate snapshot across all nodes
        system.tell(new CheckpointRecoveryManager.InitiateNetworkSnapshot());
        Thread.sleep(2000);

        actor1.tell(new PetersonKearnsActor.BasicMessage(10, actor0, new HashMap<>()));
        actor1.tell(new PetersonKearnsActor.BasicMessage(20, actor0, new HashMap<>()));

        system.tell(new CheckpointRecoveryManager.TerminateActor("0"));
        Thread.sleep(2000);

        System.out.println("Peterson-kearns simulation ended...");

        system.terminate();
    }


}
