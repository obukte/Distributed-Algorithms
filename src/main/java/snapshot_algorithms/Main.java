package snapshot_algorithms;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import snapshot_algorithms.lai_yang.LaiYangActor;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class Main {
    public static void main(String[] args) {
        Duration initialDelay = Duration.ofSeconds(1);
        Duration calculationDelay = Duration.ofSeconds(1);

        ActorSystem<Void> system = ActorSystem.create(
                Behaviors.setup(context -> {
                    context.getLog().info("Starting LaiYang snapshot algorithm simulation");

                    // Create nodes with specific neighbors
                    ActorRef<LaiYangActor.Message> nodeA = context.spawn(LaiYangActor.create(new HashSet<>()), "NodeA");
                    ActorRef<LaiYangActor.Message> nodeB = context.spawn(LaiYangActor.create(Set.of(nodeA)), "NodeB"); // NodeB knows NodeA
                    ActorRef<LaiYangActor.Message> nodeC = context.spawn(LaiYangActor.create(Set.of(nodeB)), "NodeC"); // NodeC knows NodeB

                    Set<ActorRef<LaiYangActor.Message>> allNodes = Set.of(nodeA, nodeB, nodeC);

                    // Simulate some initial calculations
                    context.getSystem().scheduler().scheduleOnce(
                            calculationDelay,
                            () -> allNodes.forEach(node -> node.tell(new LaiYangActor.PerformCalculation(10))),
                            context.getSystem().executionContext()
                    );

                    // Initiate the snapshot process after some delay
                    context.getSystem().scheduler().scheduleOnce(
                            initialDelay,
                            () -> nodeA.tell(new LaiYangActor.MarkerMessage()), // Start snapshot from NodeA
                            context.getSystem().executionContext()
                    );

                    return Behaviors.empty();
                }),
                "LaiYangSystem"
        );
    }
}
