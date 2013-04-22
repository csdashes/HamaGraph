/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    @Override
    public void compute(Iterator<Text> messages) throws IOException {
        List<Edge<Text, NullWritable>> neighboors;
        neighboors = new ArrayList<Edge<Text, NullWritable>>();
        neighboors = this.getEdges();
        String s = null;
        int i = 0;
//        Text firstNeighboor = neighboors.get(0).getDestinationVertexID();
        for (Edge<Text, NullWritable> edge : neighboors) {
            s = s + "," + edge.getDestinationVertexID().toString();
            i++;
        }
        this.setValue(new Text(s));
        voteToHalt();
    }
    
}
