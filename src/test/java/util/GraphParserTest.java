package util;

import util.GraphParser;
import org.junit.Test;
import util.GraphParser.Edge;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphParserTest {

    @Test
    public void testParseDotFile() {
        String filePath = "src/test/resources/graph/testGraph.dot";
        List<Edge> edges = GraphParser.parseDotFile(filePath);

        assertEquals(3, edges.size(), "Number of parsed edges should match");

        Edge firstEdge = edges.get(0);

        assertEquals("0", firstEdge.getSource(), "The source of the first edge should be '0'.");
        assertEquals("1", firstEdge.getDestination(), "The destination of the first edge should be '1'.");
        assertEquals(1.0, firstEdge.getWeight(), "The weight of the first edge should be 1.0.");
    }

    @Test
    public void testParseComplexDotFile() {
        // Load the complex test graph
        String filePath = "src/test/resources/graph/testGraph2.dot";
        List<Edge> edges = GraphParser.parseDotFile(filePath);

        assertEquals(11, edges.size(), "The number of parsed edges should match.");

        assertAll("edges",
                () -> {
                    Edge edge0 = edges.get(0);
                    assertEquals("0", edge0.getSource(), "Edge 0->1 source mismatch.");
                    assertEquals("1", edge0.getDestination(), "Edge 0->1 destination mismatch.");
                    assertEquals(1.0, edge0.getWeight(), "Edge 0->1 weight mismatch.");
                },
                () -> {
                    Edge edge6 = edges.get(6);
                    assertEquals("5", edge6.getSource(), "Edge 5->6 source mismatch.");
                    assertEquals("6", edge6.getDestination(), "Edge 5->6 destination mismatch.");
                    assertEquals(4.0, edge6.getWeight(), "Edge 5->6 weight mismatch.");
                },
                () -> {
                    Edge edge10 = edges.get(10);
                    assertEquals("9", edge10.getSource(), "Edge 9->0 source mismatch.");
                    assertEquals("0", edge10.getDestination(), "Edge 9->0 destination mismatch.");
                    assertEquals(6.0, edge10.getWeight(), "Edge 9->0 weight mismatch.");
                }
        );

    }
}
