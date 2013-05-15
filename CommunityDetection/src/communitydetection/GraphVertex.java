/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import java.io.IOException;
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
    Map<String, IntWritable> P = new HashMap<String, IntWritable>();

    @Override
    public void compute(Iterator<MapWritable> messages) throws IOException {
        if (this.getSuperstepCount() == 0) {

            List<Edge<Text, NullWritable>> neighboors;
            neighboors = this.getEdges();
            
            for (Edge<Text, NullWritable> edge : neighboors) {
                Nr.add(edge.getDestinationVertexID().toString());
            }
            
            MapWritable outMsg = new MapWritable();
            
            outMsg.put(new Text("Nr"), new ArrayWritable(Nr.toArray(new String[0])));

            this.sendMessageToNeighbors(outMsg);
            voteToHalt();

        } else if(this.getSuperstepCount() == 1) {
            System.out.print("MyVertex: " + this.getVertexID());
            
            while (messages.hasNext()) {
                ArrayWritable incoming = (ArrayWritable) messages.next().get(new Text("Nr"));
                List<String> neighboors = Arrays.asList(incoming.toStrings());
                for(String neighboor : neighboors) {
                    if(!neighboor.equals(this.getVertexID().toString())) {
                        P.put(neighboor, new IntWritable(1));
                    }
                }
                System.out.println(P);
            }
            voteToHalt();
        }
    }
}
