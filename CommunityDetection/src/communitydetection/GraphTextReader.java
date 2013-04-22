/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 */
public class GraphTextReader extends VertexInputReader<LongWritable, Text, Text, NullWritable, LongWritable> {

    String lastVertexId = null;
    List<String> adjacents = new ArrayList<String>();
    
    @Override
    public boolean parseVertex(LongWritable key, Text value, Vertex<Text, NullWritable, LongWritable> vertex) throws Exception {
        String line = value.toString();
        String[] lineSplit = line.split("\t");
        if (!line.startsWith("#")) {
            if (lastVertexId == null) {
                lastVertexId = lineSplit[0];
            }
            if (lastVertexId.equals(lineSplit[0])) {
                adjacents.add(lineSplit[1]);
            } else {
                vertex.setVertexID(new Text(lastVertexId));
                for (String adjacent : adjacents) {
                    vertex.addEdge(new Edge<Text, NullWritable>(new Text(adjacent),
                            null));
                }
                adjacents.clear();
                lastVertexId = lineSplit[0];
                adjacents.add(lineSplit[1]);
                return true;
            }
        }
        return false;
    }
    
}
