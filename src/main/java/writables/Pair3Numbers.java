package writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair3Numbers implements Writable {


    private final Text word1;
    private final Text word2;

    private final IntWritable w1Count;
    private final IntWritable pairCount;
    private final IntWritable trigramCount;

    public Pair3Numbers(String word1, String word2, int w1Count, int pairCount, int trigramCount) {
        this.word1 = new Text(word1);
        this.word2 = new Text(word2);
        this.w1Count = new IntWritable( w1Count);
        this.pairCount = new IntWritable(pairCount);
        this.trigramCount = new IntWritable(trigramCount);
    }

    public Pair3Numbers() {
        this.word1 = new Text();
        this.word2 =  new Text();
        this.w1Count = new IntWritable(0);
        this.pairCount = new IntWritable(0);
        this.trigramCount = new IntWritable(0);
    }

    /**
     * @return Word 1
     */
    public Text getWord1() {
        return word1;
    }

    /**
     * @return Word 2
     */
    public Text getWord2() {
        return word2;
    }

    public IntWritable getW1Count() {
        return w1Count;
    }

    public IntWritable getPairCount() {
        return pairCount;
    }

    public IntWritable getTrigramCount() {
        return trigramCount;
    }



    /**
     * implementation of write method (from writeable)
     * @param dataOutput dataOutput (hadoop.io)
     * @throws IOException if dataOutput doesn't exist
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        w1Count.write(dataOutput);
        pairCount.write(dataOutput);
        trigramCount.write(dataOutput);
    }

    /**
     * implementation of write method (from writeable)
     * @param dataInput dataInput (hadoop.io)
     * @throws IOException if dataInput doesn't exist
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        w1Count.readFields(dataInput);
        pairCount.readFields(dataInput);
        trigramCount.readFields(dataInput);
    }

    /**
     * Override of toString()
     * Used for producing output files of steps
     * @return string 's1 s2 s3 r0 r1' (string values)
     */
    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + w1Count+ " " +pairCount+" "+trigramCount ;
    }
}
