/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.csdashes.hama.graph.communityDetection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author Ilias Trichopoulos <itrichop@csd.auth.gr>
 */
public class MessagePropinquityUpdate implements WritableComparable<MessagePropinquityUpdate> {

    private Set<Text> vertexIDSet;
    private PropinquityOperation operation;
    
    MessagePropinquityUpdate(Set<Text> vertexIDSet, PropinquityOperation operation) {
        this.vertexIDSet = vertexIDSet;
        this.operation = operation;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(MessagePropinquityUpdate t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * @return the vertexIDSet
     */
    public Set<Text> getVertexIDSet() {
        return vertexIDSet;
    }

    /**
     * @param vertexIDSet the vertexIDSet to set
     */
    public void setVertexIDSet(Set<Text> vertexIDSet) {
        this.vertexIDSet = vertexIDSet;
    }

    /**
     * @return the operation
     */
    public PropinquityOperation getOperation() {
        return operation;
    }

    /**
     * @param operation the operation to set
     */
    public void setOperation(PropinquityOperation operation) {
        this.operation = operation;
    }
    
}
