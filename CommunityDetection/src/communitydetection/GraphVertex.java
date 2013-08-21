/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 */
public class GraphVertex extends Vertex<Text, NullWritable, MapWritable> {

    Set<String> Nr = new HashSet<String>(); // The remaining neighboors
    Set<String> Ni = new HashSet<String>(); // The neighboors to be insterted
    Set<String> Nd = new HashSet<String>(); // The neighboors to be deleted
    // The propinquity value map
    Map<String, Integer> P = new HashMap<String, Integer>();
    //cutting thresshold
    int a = 2;
    //emerging value
    int b = 5;
    
    int times = 1;
    
    // for the DFS
    LongWritable minimalVertexId;
    TreeSet<LongWritable> pointsTo;
    boolean activated;
    
    private Stages mainStage = new Stages(2);
    private Stages initializeStage = new Stages(6);
    private Stages incrementalStage = new Stages(4);

    private int h(String a) {
        return Integer.valueOf(a);
    }

    private int h(Text a) {
        return h(a.toString());
    }

    private Set<String> calculateRR(Set<String> nr) {
        Set<String> out = new HashSet<String>();

        if (Nr.size() > nr.size()) {
            return Sets.intersection(nr, Nr).copyInto(out);
        }
        return Sets.intersection(Nr, nr).copyInto(out);
    }

    private Set<String> calculateII(Set<String> nr, Set<String> ni) {
        Set<String> out = new HashSet<String>();

        Set<String> t1;
        if (Nr.size() > Ni.size()) {
            t1 = Sets.union(Ni, Nr);
        } else {
            t1 = Sets.union(Nr, Ni);
        }

        Set<String> t2;
        if (ni.size() > nr.size()) {
            t2 = Sets.union(nr, ni);
        } else {
            t2 = Sets.union(ni, nr);
        }

        if (t1.size() > t2.size()) {
            return Sets.intersection(t2, t1).copyInto(out);
        }

        return Sets.intersection(t1, t2).copyInto(out);
    }

    private Set<String> calculateDD(Set<String> nr, Set<String> nd) {
        Set<String> out = new HashSet<String>();

        Set<String> t1;
        if (Nr.size() > Nd.size()) {
            t1 = Sets.union(Nd, Nr);
        } else {
            t1 = Sets.union(Nr, Nd);
        }

        Set<String> t2;
        if (nd.size() > nr.size()) {
            t2 = Sets.union(nr, nd);
        } else {
            t2 = Sets.union(nd, nr);
        }

        if (t1.size() > t2.size()) {
            return Sets.intersection(t2, t1).copyInto(out);
        }

        return Sets.intersection(t1, t2).copyInto(out);
    }

    private Set<String> calculateRI(Set<String> nr, Set<String> ni) {
        Set<String> out = new HashSet<String>();

        Set<String> t1;
        if (Nr.size() > ni.size()) {
            t1 = Sets.intersection(ni, Nr);
        } else {
            t1 = Sets.intersection(Nr, ni);
        }

        Set<String> t2;
        if (Ni.size() > nr.size()) {
            t2 = Sets.intersection(nr, Ni);
        } else {
            t2 = Sets.intersection(Ni, nr);
        }

        Set<String> t3;
        if (Ni.size() > ni.size()) {
            t3 = Sets.intersection(ni, Ni);
        } else {
            t3 = Sets.intersection(Ni, ni);
        }

        Set<String> u1;
        if (t1.size() > t2.size()) {
            u1 = Sets.union(t2, t1);
        } else {
            u1 = Sets.union(t1, t2);
        }

        if (u1.size() > t3.size()) {
            return Sets.union(t3, u1).copyInto(out);
        }

        return Sets.union(u1, t3).copyInto(out);
    }

    private Set<String> calculateRD(Set<String> nr, Set<String> nd) {
        Set<String> out = new HashSet<String>();

        Set<String> t1;
        if (Nr.size() > nd.size()) {
            t1 = Sets.intersection(nd, Nr);
        } else {
            t1 = Sets.intersection(Nr, nd);
        }

        Set<String> t2;
        if (Nd.size() > nr.size()) {
            t2 = Sets.intersection(nr, Nd);
        } else {
            t2 = Sets.intersection(Nd, nr);
        }

        Set<String> t3;
        if (Nd.size() > nd.size()) {
            t3 = Sets.intersection(nd, Nd);
        } else {
            t3 = Sets.intersection(Nd, nd);
        }

        Set<String> u1;
        if (t1.size() > t2.size()) {
            u1 = Sets.union(t2, t1);
        } else {
            u1 = Sets.union(t1, t2);
        }

        if (u1.size() > t3.size()) {
            return Sets.union(t3, u1).copyInto(out);
        }

        return Sets.union(u1, t3).copyInto(out);
    }
    
    private void printNeighboors() {
        List<Edge<Text, NullWritable>> neighboors;
        neighboors = this.getEdges();
        Set<String> neighboorsSet = new HashSet<String>();
        for (Edge<Text, NullWritable> edge : neighboors) {
            neighboorsSet.add(edge.getDestinationVertexID().toString());
        }

        System.out.println("neighboors of " + this.getVertexID() + " are: " + neighboorsSet);
    }

    /* This method is responsible to initialize the propinquity
     * hash map in each vertex. Consists of 2 steps, the angle
     * and the conjugate propinquity update.
     * @param messages The messages received in each superstep.
     */
    private void initialize(Iterable<MapWritable> messages) throws IOException {
        List<Edge<Text, NullWritable>> neighboors;
        neighboors = this.getEdges();

        MapWritable outMsg = new MapWritable();

        /* Create a 2-way direction between vertexes. From each
         * vertex send a message with the vertex id to all of
         * the neighboors. When a vertex receives the message it
         * creates a new edge between the sender and the receiver
         */
        switch (this.initializeStage.getStage()) {
            case 0:
                outMsg = new MapWritable();
                outMsg.put(new Text("init"), this.getVertexID());
                this.sendMessageToNeighbors(outMsg);
                break;
            case 1:
                List<Edge<Text, NullWritable>> edges = this.getEdges();
                Set<String> uniqueEdges = new HashSet<String>();
                for (Edge edge : edges) {
                    uniqueEdges.add(edge.getDestinationVertexID().toString());
                }
                for (MapWritable message : messages) {
                    Text id = (Text) message.get(new Text("init"));
                    if (uniqueEdges.add(id.toString())) {
                        Edge<Text, NullWritable> e = new Edge<Text, NullWritable>(id, null);
                        this.addEdge(e);
                    }
                }
                break;
            case 2:
                /* ==== Initialize angle propinquity start ====
                 * The goal is to increase the propinquity between 2 
                 * vertexes according to the amoung of the common neighboors
                 * between them.
                 * Initialize the Nr Set by adding all the neighboors in
                 * it and send the Set to all neighboors so they know
                 * that for the vertexes of the Set, the sender vertex is
                 * a common neighboor.
                 */
                for (Edge<Text, NullWritable> edge : neighboors) {
                    Nr.add(edge.getDestinationVertexID().toString());
                }


                outMsg.put(new Text("Nr"), new ArrayWritable(Nr.toArray(new String[0])));
                outMsg.put(new Text("Sender"), this.getVertexID());

                this.sendMessageToNeighbors(outMsg);
                break;
            case 3:
                /* Initialize the propinquity hash map for the vertexes of the
                 * received list.
                 */
                for (MapWritable message : messages) {
                    ArrayWritable incoming = (ArrayWritable) message.get(new Text("Nr"));
                    Text sender = (Text) message.get(new Text("Sender"));
                    List<String> commonNeighboors = Arrays.asList(incoming.toStrings());
                    Set commonNeighboorsSet = new HashSet<String>(commonNeighboors);
                    commonNeighboorsSet.remove(this.getVertexID().toString());
                    updatePropinquity(commonNeighboorsSet,
                            PropinquityUpdateOperation.INCREASE);
                }
                /* ==== Initialize angle propinquity end ==== */
                /* ==== Initialize conjugate propinquity start ==== 
                 * The goal is to increase the propinquity of a vertex pair
                 * according to the amount of edges between the common neighboors
                 * of this pair.
                 * Send the neighboors list of the vertex to all his neighboors.
                 * To achive only one way communication, a function that compairs
                 * the vertex ids is being used.
                 */
                for (Edge<Text, NullWritable> edge : neighboors) {
                    String neighboor = edge.getDestinationVertexID().toString();
                    if (Integer.parseInt(neighboor) > Integer.parseInt(this.getVertexID().toString())) {
                        outMsg.put(new Text("Nr"), new ArrayWritable(Nr.toArray(new String[0])));
                        outMsg.put(new Text("Sender"), this.getVertexID());
                        this.sendMessage(edge.getDestinationVertexID(), outMsg);
                    }
                }
                break;
            case 4:
                /* Find the intersection of the received vertex list and the
                 * neighboor list of the vertex so as to create a list of the
                 * common neighboors between the sender and the receiver vertex.
                 * Send the intersection list to every element of this list so
                 * as to increase the propinquity.
                 */
                List<String> Nr_neighboors;
                Set<String> intersection = null;
                for (MapWritable message : messages) {
                    ArrayWritable incoming = (ArrayWritable) message.get(new Text("Nr"));
                    Text sender = (Text) message.get(new Text("Sender"));
                    Nr_neighboors = Arrays.asList(incoming.toStrings());
                    boolean Nr1IsLarger = Nr.size() > Nr_neighboors.size();
                    intersection = new HashSet<String>(Nr1IsLarger ? Nr_neighboors : Nr);
                    intersection.retainAll(Nr1IsLarger ? Nr : Nr_neighboors);
                    if (intersection != null) {
                        for (String vertex : intersection) {
                            Set<String> messageList = new HashSet<String>(intersection);
                            messageList.remove(vertex);

                            if (!messageList.isEmpty()) {
                                ArrayWritable aw = new ArrayWritable(messageList.toArray(new String[0]));
                                outMsg = new MapWritable();
                                outMsg.put(new Text("Intersection"), aw);
                                this.sendMessage(new Text(vertex), outMsg);
                            }
                            //System.out.println("");
                        }
                    }
                }
                break;
            case 5:
                // update the conjugate propinquity
                for (MapWritable message : messages) {
                    ArrayWritable incoming = (ArrayWritable) message.get(new Text("Intersection"));
                    Nr_neighboors = Arrays.asList(incoming.toStrings());

                    updatePropinquity(Nr_neighboors,
                            PropinquityUpdateOperation.INCREASE);
                }
                this.mainStage.increaseStage();
                break;
        }
        
        this.initializeStage.increaseStage();
        /* ==== Initialize conjugate propinquity end ==== */
    }

    /* Converts the Set to a List and call the updatePropinquity method.
     * @param vertexes The list of the vertex ids to increase the propinquity
     * @param operation The enum that identifies the operation (INCREASE OR
     * DECREASE)
     */
    private int updatePropinquity(Set<String> vertexes, PropinquityUpdateOperation operation) {
        List<String> vertexesSetToList = new ArrayList<String>();
        vertexesSetToList.addAll(vertexes);
        return updatePropinquity(vertexesSetToList, operation);
    }

    /* Increase the propinquity for each of the list items.
     * @param vertexes The list of the vertex ids to increase the propinquity
     * @param operation The enum that identifies the operation (INCREASE OR
     * DECREASE)
     */
    private int updatePropinquity(List<String> vertexes, PropinquityUpdateOperation operation) {
        switch (operation) {
            case INCREASE:
                for (String vertex : vertexes) {
                    if (P.get(vertex) != null && P.get(vertex) != 0) {
                        P.put(vertex, P.remove(vertex) + 1);
                    } else {
                        P.put(vertex, 1);
                    }
                }
                break;
            case DECREASE:
                for (String vertex : vertexes) {
                    if (P.get(vertex) != null && P.get(vertex) != 0) {
                        P.put(vertex, P.remove(vertex) - 1);
                    } else {
                        P.put(vertex, 1);
                    }
                }
                break;
        }
        return vertexes.size();
    }

    /* This method is responsible for the incremental update
     * @param messages The messages received in each superstep.
     */
    private void incremental(Iterable<MapWritable> messages) throws IOException {
        int changes = 0;
        switch (this.incrementalStage.getStage()) {
            case 0:
                for (String vertex : P.keySet()) {
                    int propinquityValue = P.get(vertex);
                    if (propinquityValue <= a && Nr.contains(vertex)) {
                        Nd.add(vertex);
                        Nr.remove(vertex);
                    }
                    if (propinquityValue >= b && !Nr.contains(vertex)) {
                        Ni.add(vertex);
                    }
                }
                for (String vertex : Nr) {
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(new Text("PU+"), new ArrayWritable(Ni.toArray(new String[0])));
                    this.sendMessage(new Text(vertex), outMsg);

                    outMsg = new MapWritable();

                    outMsg.put(new Text("PU-"), new ArrayWritable(Nd.toArray(new String[0])));
                    this.sendMessage(new Text(vertex), outMsg);
                }
                for (String vertex : Ni) {
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(new Text("PU+"), new ArrayWritable(Nr.toArray(new String[0])));
                    this.sendMessage(new Text(vertex), outMsg);

                    outMsg = new MapWritable();

                    Set<String> tmp = new HashSet<String>(Ni);
                    tmp.remove(vertex);

                    outMsg.put(new Text("PU+"), new ArrayWritable(tmp.toArray(new String[0])));
                    this.sendMessage(new Text(vertex), outMsg);

                }
                for (String vertex : Nd) {
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(new Text("PU-"), new ArrayWritable(Nr.toArray(new String[0])));
                    this.sendMessage(new Text(vertex), outMsg);

                    outMsg = new MapWritable();

                    Set<String> tmp = new HashSet<String>(Nd);
                    tmp.remove(vertex);

                    outMsg.put(new Text("PU-"), new ArrayWritable(tmp.toArray(new String[0])));
                    this.sendMessage(new Text(vertex), outMsg);
                }
                break;
            case 1:
                for (MapWritable message : messages) {
                    if (message.containsKey(new Text("PU+"))) {
                        ArrayWritable messageValue = (ArrayWritable) message.get(new Text("PU+"));
                        changes += updatePropinquity(Arrays.asList(messageValue.toStrings()),
                                PropinquityUpdateOperation.INCREASE);
                    } else if (message.containsKey(new Text("PU-"))) {
                        ArrayWritable messageValue = (ArrayWritable) message.get(new Text("PU-"));
                        changes += updatePropinquity(Arrays.asList(messageValue.toStrings()),
                                PropinquityUpdateOperation.DECREASE);
                    }
                }

                for (String vertex : Nr) {
                    if (h(vertex) > h(this.getVertexID())) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(new Text("Sender"), this.getVertexID());
                        outMsg.put(new Text("DN NR"), new ArrayWritable(Nr.toArray(new String[0])));
                        outMsg.put(new Text("DN NI"), new ArrayWritable(Ni.toArray(new String[0])));
                        outMsg.put(new Text("DN ND"), new ArrayWritable(Nd.toArray(new String[0])));
                        this.sendMessage(new Text(vertex), outMsg);
                    }
                }

                for (String vertex : Ni) {
                    if (h(vertex) > h(this.getVertexID())) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(new Text("Sender"), this.getVertexID());
                        outMsg.put(new Text("DN NR"), new ArrayWritable(Nr.toArray(new String[0])));
                        outMsg.put(new Text("DN NI"), new ArrayWritable(Ni.toArray(new String[0])));
                        this.sendMessage(new Text(vertex), outMsg);
                    }
                }

                for (String vertex : Nd) {
                    if (h(vertex) > h(this.getVertexID())) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(new Text("Sender"), this.getVertexID());
                        outMsg.put(new Text("DN NR"), new ArrayWritable(Nr.toArray(new String[0])));
                        outMsg.put(new Text("DN ND"), new ArrayWritable(Nd.toArray(new String[0])));
                        this.sendMessage(new Text(vertex), outMsg);
                    }
                }
                break;
            case 2:
                for (MapWritable message : messages) {
                    Text messageValue = (Text) message.get(new Text("Sender"));
                    String senderVertexId = messageValue.toString();
                    ArrayWritable messageValueNr = (ArrayWritable) message.get(new Text("DN NR"));
                    ArrayWritable messageValueNi = (ArrayWritable) message.get(new Text("DN NI"));
                    ArrayWritable messageValueNd = (ArrayWritable) message.get(new Text("DN ND"));
                    if (messageValueNi == null) {
                        messageValueNi = new ArrayWritable(new String[0]);
                    }
                    if (messageValueNd == null) {
                        messageValueNd = new ArrayWritable(new String[0]);
                    }
                    if (Nr.contains(senderVertexId)) {
                        //calculate RR
                        Set<String> RRList = calculateRR(new HashSet<String>(Arrays.asList(messageValueNr.toStrings())));
                        //calculate RI
                        Set<String> RIList = calculateRI(new HashSet<String>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<String>(Arrays.asList(messageValueNi.toStrings())));
                        //calculate RD
                        Set<String> RDList = calculateRD(new HashSet<String>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<String>(Arrays.asList(messageValueNd.toStrings())));

                        for (String vertex : RRList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(new Text("UP+"), new ArrayWritable(RIList.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);

                            outMsg = new MapWritable();

                            outMsg.put(new Text("UP-"), new ArrayWritable(RDList.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                        for (String vertex : RIList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(new Text("UP+"), new ArrayWritable(RRList.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);

                            outMsg = new MapWritable();

                            Set<String> tmp = new HashSet<String>(RIList);
                            tmp.remove(vertex);
                            outMsg.put(new Text("UP-"), new ArrayWritable(tmp.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                        for (String vertex : RDList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(new Text("UP-"), new ArrayWritable(RRList.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);

                            outMsg = new MapWritable();

                            Set<String> tmp = new HashSet<String>(RDList);
                            tmp.remove(vertex);
                            outMsg.put(new Text("UP-"), new ArrayWritable(tmp.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                    }
                    if (Ni.contains(senderVertexId)) {
                        //calculate II
                        Set<String> RIList = calculateII(new HashSet<String>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<String>(Arrays.asList(messageValueNi.toStrings())));
                        for (String vertex : RIList) {
                            MapWritable outMsg = new MapWritable();

                            Set<String> tmp = new HashSet<String>(RIList);
                            tmp.remove(vertex);
                            outMsg.put(new Text("UP+"), new ArrayWritable(tmp.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                    }
                    if (Nd.contains(senderVertexId)) {
                        //calculate DD
                        Set<String> RDList = calculateDD(new HashSet<String>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<String>(Arrays.asList(messageValueNd.toStrings())));
                        for (String vertex : RDList) {
                            MapWritable outMsg = new MapWritable();

                            Set<String> tmp = new HashSet<String>(RDList);
                            tmp.remove(vertex);
                            outMsg.put(new Text("UP-"), new ArrayWritable(tmp.toArray(new String[0])));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                    }
                }
                break;
            case 3:
                for (MapWritable message : messages) {
                    if (message.containsKey(new Text("UP+"))) {
                        ArrayWritable messageValue = (ArrayWritable) message.get(new Text("UP+"));
                        updatePropinquity(Arrays.asList(messageValue.toStrings()),
                                PropinquityUpdateOperation.INCREASE);
                    } else if (message.containsKey(new Text("UP-"))) {
                        ArrayWritable messageValue = (ArrayWritable) message.get(new Text("UP-"));
                        updatePropinquity(Arrays.asList(messageValue.toStrings()),
                                PropinquityUpdateOperation.DECREASE);
                    }
                }

                // NR ←NR + ND
                Set<String> tmp = new HashSet<String>();
                if (Nr.size() > Nd.size()) {
                    Nr = Sets.union(Nd, Nr).copyInto(tmp);
                } else {
                    Nr = Sets.union(Nr, Nd).copyInto(tmp);
                }

            printNeighboors();
            redistributeEdges();
            printNeighboors();
                break;
        }
        
        this.incrementalStage.increaseStage();
    }

    private void redistributeEdges() {
        HashMap<String, Edge<Text, NullWritable>> reversedEdges = edgesToMap(this.getEdges());

        for (String newDestinationVertexID : Ni) {
            reversedEdges.put(newDestinationVertexID, new Edge<Text, NullWritable>(new Text(newDestinationVertexID), null));
        }
        for (String oldDestinationVertexID : Nd) {
            reversedEdges.remove(oldDestinationVertexID);
        }
        
        this.setEdges(mapToEdges(reversedEdges));
     
    }

    /**
     * Creates a map from destination vertex IDs to Edge objects
     *
     * @param edges The list of edges
     * @return A map with the destination vertex IDs as key and the Edge object
     * as value
     */
    private HashMap<String, Edge<Text, NullWritable>> edgesToMap(List<Edge<Text, NullWritable>> edges) {
        HashMap<String, Edge<Text, NullWritable>> reversedEdges = new HashMap<String, Edge<Text, NullWritable>>();
        for (Edge edge : edges) {
            reversedEdges.put(edge.getDestinationVertexID().toString(), edge);
        }
        return reversedEdges;
    }
    
    /**
     * Converts the map <vertexID,Edge> to a list of Edges
     * @param map HashMap of <vertexID,Edge>
     * @return list of Edges
     */
    private List<Edge<Text,NullWritable>> mapToEdges(HashMap<String, Edge<Text, NullWritable>> map) {
        return new ArrayList(map.values());
    }

    @Override
    public void compute(Iterable<MapWritable> messages) throws IOException {
        
        if (this.getVertexID().toString().equals("0")) {
           //terminationCondition(messages);
        } else {
//            switch (this.mainStage.getStage()) {
//                case 0:
//                    initialize(messages);
//                    break;
//                case 1:
//                    incremental(messages);
//                    break;
//            }
            
            if (this.getSuperstepCount() < 6) {
                initialize(messages);
            }  
            else if (this.getSuperstepCount() >= 13) {
                detectCommunities(messages);
            }
            else if (this.getSuperstepCount() > 8 
                    //&& this.getSuperstepCount() < 13
                    ) { //before it was > 8
                incremental(messages);
            }
        }
    }

    private void detectCommunities(Iterable<MapWritable> messages) throws IOException {
        if (this.getSuperstepCount() == 13) {
            pointsTo = new TreeSet<LongWritable>();
            List<Edge<Text, NullWritable>> neighboors;
            neighboors = this.getEdges();

            for (Edge<Text, NullWritable> edge : neighboors) {
                pointsTo.add(new LongWritable(Integer.valueOf(edge.getDestinationVertexID().toString())));
            }
            minimalVertexId = pointsTo.first();
            activated = true;
            
        } else if(this.getSuperstepCount()%2==0) {
            if(activated) {
                MapWritable outMsg;
                outMsg = new MapWritable();
                outMsg.put(new Text("minimalVertexId"), minimalVertexId);
                sendMessageToNeighbors(outMsg);
                activated = false;
            } else {
                System.out.println("minimal of " + this.getVertexID() + " is: " + minimalVertexId);
                voteToHalt();
            }
        } else if(this.getSuperstepCount()%2==1) {
            for (MapWritable message : messages) {
                LongWritable messageReceived = (LongWritable) message.get(new Text("minimalVertexId"));
                if(messageReceived.compareTo(minimalVertexId) < 0 ) {
                    minimalVertexId = messageReceived;
                    activated = true;
                }
            }
        }
    }

    private void terminationCondition(Iterable<MapWritable> messages) throws IOException {
        long term = 0L;
        for (MapWritable m : messages) {
            term += ((IntWritable)m.keySet().toArray()[0]).get();
        }
        
        if (term < 5) {
            for (long i=1; i < this.getNumVertices(); i++) {
                MapWritable outMsg;
                outMsg = new MapWritable();
                outMsg.put(new Text("voteToHalt"), null);
                this.sendMessage(new Text(String.valueOf(i)), outMsg);
                
            }
        }
    }
}