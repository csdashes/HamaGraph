/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class GraphNewTextReader extends VertexInputReader<LongWritable, Text, Text, NullWritable, LongWritable> {

    @Override
    public boolean parseVertex(LongWritable key, Text value, Vertex<Text, NullWritable, LongWritable> vertex) throws Exception {
        String[] k = value.toString().split("\t");
        vertex.setVertexID(new Text(k[0]));

        String[] es = k[1].split(" ");
        for (String e : es) {
            vertex.addEdge(new Edge<Text, NullWritable>(new Text(e), null));
        }
        
        return true;
    }
    
}
