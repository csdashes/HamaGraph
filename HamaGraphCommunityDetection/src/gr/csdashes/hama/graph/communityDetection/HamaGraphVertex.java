/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.csdashes.hama.graph.communityDetection;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class HamaGraphVertex extends Vertex<Text, NullWritable, LongWritable> {
    
    /**
     * 
     * @param messages
     * @throws IOException
     */
    @Override
    public void compute(Iterator<LongWritable> messages) throws IOException {
        
    }
}
