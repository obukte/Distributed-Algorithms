package util;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GraphParser {

    public static class Edge {
        String source;
        String destination;
        double weight;

        public Edge(String source, String destination, double weight) {
            this.source = source;
            this.destination = destination;
            this.weight = weight;
        }

        @Override
        public String toString() {
            return "Edge{" +
                    "source='" + source + '\'' +
                    ", destination='" + destination + '\'' +
                    ", weight=" + weight +
                    '}';
        }

        public String getSource() {
            return source;
        }

        public String getDestination() {
            return destination;
        }

        public double getWeight() {
            return weight;
        }
    }

    public static List<Edge> parseDotFile(String filePath) {
        List<Edge> edges = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("->")) {
                    String[] parts = line.split(" ");
                    String source = parts[0].replace("\"", "");
                    String destination = parts[2].replace("\"", "");

                    double weight = extractWeight(line);
                    edges.add(new Edge(source, destination, weight));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return edges;
    }

    private static double extractWeight(String line) {
        try {
            String weightStr = line.substring(line.indexOf("\"weight\"=") + 9, line.lastIndexOf("]"));
            return Double.parseDouble(weightStr.replace("\"", ""));
        } catch (Exception e){
            System.err.println("Error parsing weight from line: " + line);
            return -1;
        }
    }

    public static void main(String[] args) {
        String filePath = "src/main/resources/graph/NetGraph_17-03-24-12-50-04.ngs.dot";
        List<Edge> edges = parseDotFile(filePath);
        edges.forEach(System.out::println);
    }


}
