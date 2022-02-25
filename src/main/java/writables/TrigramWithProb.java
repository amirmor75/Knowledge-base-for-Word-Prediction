package writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class TrigramWithProb implements WritableComparable<TrigramWithProb> {


    private final Text word1;
    private final Text word2;
    private final Text word3;

    private final DoubleWritable prob;

    /**
     * Constructor
     * @param s first word
     * @param s1 second word
     * @param s2 third word
     * @param prob Gram's occurrences in Corpus 0
     */
    public TrigramWithProb(String s, String s1, String s2, double prob) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
        this.prob = new DoubleWritable(prob);
    }
    public TrigramWithProb(Text s, Text s1, Text s2, double prob) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
        this.prob = new DoubleWritable(prob);
    }

    /**
     * Empty constructor
     */
    public TrigramWithProb(){
        this.word1 = new Text();
        this.word2 = new Text();
        this.word3 = new Text();
        this.prob = new DoubleWritable(0);
    }



    /**
     * @return Word 1
     */
    public Text getWord1() {
        return word1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrigramWithProb that = (TrigramWithProb) o;
        return Objects.equals(word1, that.word1) && Objects.equals(word2, that.word2) && Objects.equals(word3, that.word3) && Objects.equals(prob, that.prob);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word1, word2, word3, prob);
    }

    /**
     * @return Word 2
     */
    public Text getWord2() {
        return word2;
    }

    /**
     * @return Word 3
     */
    public Text getWord3() {
        return word3;
    }

    /**
     * @return Word r_1
     */
    public DoubleWritable getProb() {
        return prob;
    }

    /**
     * Compare to other 3grams
     * @param o other 3gram
     * @return int value, result of comparing the 2 3grams
     */
    @Override
    public int compareTo(TrigramWithProb o) {
        String me = word1.toString() + " " + word2.toString() + " " + word3.toString();
        String other = o.getWord1().toString() + " "  + o.getWord2().toString();
        if ( me.compareTo(other) == 0){
            return o.getProb().get() - this.prob.get() < 0 ? -1 : 1;
        }
        return me.compareTo(other);
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
        word3.write(dataOutput);
        prob.write(dataOutput);
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
        word3.readFields(dataInput);
        prob.readFields(dataInput);
    }

    /**
     * Override of toString()
     * Used for producing output files of steps
     * @return string 's1 s2 s3 r0 r1' (string values)
     */
    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString() + " " + prob ;
    }
}