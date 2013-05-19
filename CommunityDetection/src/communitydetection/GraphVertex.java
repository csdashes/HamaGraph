/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
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
    Set<Text> Ni = new HashSet<Text>(); // The neighboors to be insterted
    Set<Text> Nd = new HashSet<Text>(); // The neighboors to be deleted
    // The propinquity value map
    Map<String, Integer> P = new HashMap<String, Integer>();

    @Override
    public void compute(Iterator<MapWritable> messages) throws IOException {

        List<Edge<Text, NullWritable>> neighboors;
        neighboors = this.getEdges();
        
        MapWritable outMsg = new MapWritable();
        
        // for the first superstep, initialize the Nr Set by adding all the 
        // neighboors of the current vertex and send the Set to all neighboors
        if (this.getSuperstepCount() == 0) {
            
            System.out.println("### SUPERSTEP 1 ###");
            for (Edge<Text, NullWritable> edge : neighboors) {
                Nr.add(edge.getDestinationVertexID().toString());
            }
            
            System.out.println("neighboors of " + this.getVertexID() + " are: " + Nr);
            
            outMsg.put(new Text("Nr"), new ArrayWritable(Nr.toArray(new String[0])));

            this.sendMessageToNeighbors(outMsg);
            voteToHalt();
        } 
        
        // for the second superstep, read the messages and initialize the
        // propinquity hash map. Then send the Nr list only to one vertex
        // of each vertex pair.
        else if(this.getSuperstepCount() == 1) {
            
            System.out.println("### SUPERSTEP 2 ###");
            
            while (messages.hasNext()) {
                ArrayWritable incoming = (ArrayWritable) messages.next().get(new Text("Nr"));
                List<String> Nr_neighboors = Arrays.asList(incoming.toStrings());
                for(String neighboor : Nr_neighboors) {
                    if(!neighboor.equals(this.getVertexID().toString())) {
                        P.put(neighboor, 1);
                    }
                }
            }
            
            System.out.println("Hash for: " + this.getVertexID() + " -> " + P);
            
            for (Edge<Text, NullWritable> edge : neighboors) {
                String neighboor = edge.getDestinationVertexID().toString();
                // commented until we fix the "directed graph" problem
                //if(Integer.parseInt(neighboor) > Integer.parseInt(this.getVertexID().toString())) {
                    outMsg.put(new Text("Nr"), new ArrayWritable(Nr.toArray(new String[0])));
                    this.sendMessage(edge.getDestinationVertexID(), outMsg);
                //}
            }
            voteToHalt();
        } 
        // read the messages and find the intersection of the message list and
        // local Nr Set.
        else if(this.getSuperstepCount() == 2) {
            
            System.out.println("### SUPERSTEP 3 ###");
            
            List<String> Nr_neighboors;
            Set<String> intersection = null;
            while (messages.hasNext()) {
                ArrayWritable incoming = (ArrayWritable) messages.next().get(new Text("Nr"));
                Nr_neighboors = Arrays.asList(incoming.toStrings());
                System.out.println(this.getVertexID() + "-> Message received: " + Nr_neighboors);
                
                boolean Nr1IsLarger = Nr.size() > Nr_neighboors.size();
                intersection = new HashSet<String>(Nr1IsLarger ? Nr_neighboors : Nr);
                intersection.retainAll(Nr1IsLarger ? Nr : Nr_neighboors);
                System.out.println("Intersection of " + Nr + " and " + Nr_neighboors + ": " + intersection);
                System.out.println("");
            }
            
            for(String vertex : intersection) {
                System.out.print("destination vertex: " + vertex);
                Set<String> messageList = new HashSet<String>(intersection);
                System.out.print(", message list to send: " + messageList);
                messageList.remove(vertex);
                System.out.print(", after removal: " + messageList);
                
                if(!messageList.isEmpty()) {
                    ArrayWritable aw = new ArrayWritable(messageList.toArray(new String[0]));
                    outMsg = new MapWritable();
                    outMsg.put(new Text("Intersection"), aw );
                    this.sendMessage(new Text(vertex), outMsg);
                    //System.out.println("message sent: " + Arrays.asList(outMsg.get(new Text("Intersection")).toStrings()));
                }
                System.out.println("");
            }
        }
        
        // update the conjugate propinquity
        else if(this.getSuperstepCount() == 3) {
            
            System.out.println("### SUPERSTEP 4 ###");
            
            System.out.println("BEFORE Hash for: " + this.getVertexID() + " -> " + P);
            while (messages.hasNext()) {
                ArrayWritable incoming = (ArrayWritable) messages.next().get(new Text("Intersection"));
                List<String> Nr_neighboors = Arrays.asList(incoming.toStrings());
                System.out.println("");
                System.out.println(this.getVertexID() + "-> Message received: " + Nr_neighboors);
                for(String vertex : Nr_neighboors) {
                    int propinquity = 0;
                    propinquity = P.get(vertex);
                    if(propinquity != 0) {
                        P.put(vertex, P.remove(vertex)+1);
                    } 
                    else {
                        P.put(vertex, 1);
                    }
                }
            }
            System.out.println("Hash for: " + this.getVertexID() + " -> " + P);
        }
    }
}