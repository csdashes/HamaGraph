/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 */
public class GraphVertex extends Vertex<Text, NullWritable, Text>{

    Set<Text> Nr = new HashSet<Text>(); // The remaining neighboors
    Set<Text> Ni = new HashSet<Text>(); // The neighboors to be insterted
    Set<Text> Nd = new HashSet<Text>(); // The neighboors to be deleted
    // The propinquity value map
    Map<HashMap<Text,Text>,LongWritable> P = new HashMap<HashMap<Text,Text>,LongWritable>();
    
    
    @Override
    public void compute(Iterator<Text> messages) throws IOException {
        List<Edge<Text, NullWritable>> neighboors;
        neighboors = new ArrayList<Edge<Text, NullWritable>>();
        neighboors = this.getEdges();
//        String s = null;
//        int i = 0;
//        Text firstNeighboor = neighboors.get(0).getDestinationVertexID();
        for (Edge<Text, NullWritable> edge : neighboors) {
//            s = s + "," + edge.getDestinationVertexID().toString();
//            i++;
            Nr.add(edge.getDestinationVertexID());
        }
//        this.setValue(new Text(s));
        this.setValue(new Text(Nr.toString()));
        voteToHalt();
    }
    
}
