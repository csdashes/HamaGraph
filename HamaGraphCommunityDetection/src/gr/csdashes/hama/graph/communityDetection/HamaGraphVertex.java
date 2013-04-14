/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.csdashes.hama.graph.communityDetection;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.graph.Vertex;

/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class HamaGraphVertex extends Vertex<Text, NullWritable, Writable> {
    
    Set<Text> Nr = new HashSet<Text>(); // The remaining neighboors
    Set<Text> Ni = new HashSet<Text>(); // The neighboors to be insterted
    Set<Text> Nd = new HashSet<Text>(); // The neighboors to be deleted
    // The propinquity value map
    Map<HashMap<Text,Text>,LongWritable> P = new HashMap<HashMap<Text,Text>,LongWritable>();
    
    /**
     * The compute method.
     * @param messages The incoming messages to the vertex
     * @throws IOException
     */
    @Override
    public void compute(Iterator<Writable> messages) throws IOException {
        
        // Send the id of the vertex to all neighboors to initialize
        // the Nr Set
        if(this.getSuperstepCount() == 0) {
            sendMessageToNeighbors(new Text(this.getVertexID()));
        }
        else {
            // SUPERSTEP 1
            // Initialize the Nr Set
            while (messages.hasNext()) {
                Text msg = (Text)messages.next();
                this.Nr.add(msg);
            }
            // Send message to all neighboors to increase their 
            // propinquinty
            Iterator iterator = Nr.iterator();
            while(iterator.hasNext()) {
                Text neighboorID = (Text) iterator.next();
                sendMessage(neighboorID,new MessagePropinquityUpdate(this.Nr,PropinquityOperation.INCREASE));
            }
        }
    }
}
