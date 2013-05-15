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
    Map<Text, IntWritable> P = new HashMap<Text, IntWritable>();

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

            System.out.println("MyVertex: " + this.getVertexID());
            this.sendMessageToNeighbors(outMsg);
            voteToHalt();

        } else {
            System.out.println("MyVertex: " + this.getVertexID());
            while (messages.hasNext()) {
                ArrayWritable incoming = (ArrayWritable) messages.next().get(new Text("Nr"));
                System.out.println(Arrays.asList(incoming.toStrings()));
            }
            voteToHalt();
        }
    }
}
