package writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DataPair is a writeable object, to represent a pair of values.
 * Implement the methods for read, write and compare,
 * and meant to be used in the hadoop mapreduce logic
 */
public class DataPair implements Writable {

    private final IntWritable w2Count;
    private final IntWritable pairCount;


    public DataPair(int w2Count, int pairCount) {
        this.w2Count = new IntWritable(w2Count);
        this.pairCount = new IntWritable(pairCount);
    }

    /**
     * Empty constructor
     */
    public DataPair(){
        this.w2Count = new IntWritable(0);
        this.pairCount = new IntWritable(0);
    }

    /**
     * @return first element in pair
     */
    public IntWritable getW2Count() {
        return w2Count;
    }

    /**
     * @return second element in pair
     */
    public IntWritable getPairCount() {
        return pairCount;
    }

    /**
     * implementation of write method (from writeable)
     * @param dataOutput dataOutput (hadoop.io)
     * @throws IOException if dataOutput doesn't exist
     */
    public void write(DataOutput dataOutput) throws IOException {
        w2Count.write(dataOutput);
        pairCount.write(dataOutput);
    }

    /**
     * implementation of write method (from writeable)
     * @param dataInput dataInput (hadoop.io)
     * @throws IOException if dataInput doesn't exist
     */
    public void readFields(DataInput dataInput) throws IOException {
        w2Count.readFields(dataInput);
        pairCount.readFields(dataInput);
    }

    /**
     * Override of toString()
     * Used for producing output files of steps
     * @return string 'first_second' (values)
     */
    @Override
    public String toString() {
        return w2Count.toString() + " " + pairCount.toString();
    }
}