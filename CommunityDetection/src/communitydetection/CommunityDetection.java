/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.GraphJob;
/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 */
public class CommunityDetection {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            printUsage();
        }
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob graphJob = createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (graphJob.waitForCompletion(true)) {
            System.out.println("Job Finished in "
                    + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: <input> <output>");
        System.exit(-1);
    }
    
    private static GraphJob createJob(String[] args, HamaConfiguration conf) throws IOException {
        GraphJob graphJob = new GraphJob(conf, CommunityDetection.class);
        graphJob.setJobName("Community Detection");
        graphJob.setVertexClass(GraphVertex.class);
        graphJob.setInputPath(new Path(args[0]));
        graphJob.setOutputPath(new Path(args[1]));
        graphJob.setVertexIDClass(Text.class);
        graphJob.setVertexValueClass(Text.class);
        graphJob.setEdgeValueClass(NullWritable.class);
        graphJob.setInputFormat(TextInputFormat.class);
        graphJob.setInputKeyClass(LongWritable.class);
        graphJob.setInputValueClass(Text.class);
        graphJob.setVertexInputReaderClass(GraphTextReader.class);
        graphJob.setPartitioner(HashPartitioner.class);
        graphJob.setOutputFormat(TextOutputFormat.class);
        graphJob.setOutputKeyClass(Text.class);
        graphJob.setOutputValueClass(Text.class);
        return graphJob;
    }
}
