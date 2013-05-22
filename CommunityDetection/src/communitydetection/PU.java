/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package communitydetection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class PU extends Message implements WritableComparable<PU> {

    @Override
    public void write(DataOutput d) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(PU t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
