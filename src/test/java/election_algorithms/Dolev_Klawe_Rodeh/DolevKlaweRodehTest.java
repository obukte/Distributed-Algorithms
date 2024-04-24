package election_algorithms.Dolev_Klawe_Rodeh;

import akka.actor.typed.ActorSystem;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import election_algorithms.DolevklaweRodehAglorithm.DolevKlaweRodehActor;

import java.util.HashMap;
import java.util.Map;

public class DolevKlaweRodehTest {
    public static void main(String[] args) {
        // Define the number of actors in the ring
        final int numberOfActors = 5;
        Map<Integer, ActorRef<DolevKlaweRodehActor.Message>> actors = new HashMap<>();

        // Create the actor system and actors
        ActorSystem<DolevKlaweRodehActor.Message> actorSystem = ActorSystem.create(createMainBehavior(numberOfActors, actors), "DolevKlaweRodehActorSystem");
    }

    // Create a main behavior to initialize and manage the ring of actors
    private static Behavior<DolevKlaweRodehActor.Message> createMainBehavior(final int numberOfActors, final Map<Integer, ActorRef<DolevKlaweRodehActor.Message>> actors) {
        return Behaviors.setup(context -> {
            // Create actors with unique IDs and add them to the map
            for (int i = 0; i < numberOfActors; i++) {
                ActorRef<DolevKlaweRodehActor.Message> actorRef = context.spawn(DolevKlaweRodehActor.create(i), "actor" + i);
                actors.put(i, actorRef);
            }

            // Initialize the ring for each actor
            actors.forEach((id, ref) -> {
                int nextId = (id + 1) % numberOfActors; // Circular ring
                Map<Integer, ActorRef<DolevKlaweRodehActor.Message>> initMap = new HashMap<>();
                initMap.put(id, actors.get(nextId)); // Set the next actor
                ref.tell(new DolevKlaweRodehActor.InitializeRing(initMap));
            });

            // Start the election by sending an ElectionMessage from the first actor
            ActorRef<DolevKlaweRodehActor.Message> firstActor = actors.get(0);
            if (firstActor != null) {
                firstActor.tell(new DolevKlaweRodehActor.ElectionMessage(0, 0, false));
            }
            return Behaviors.empty();
        });
    }
}