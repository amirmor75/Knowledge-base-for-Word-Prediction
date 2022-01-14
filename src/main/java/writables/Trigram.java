package writables;

import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


/**
 * 3Gram implementation & representation for the hadoop flow, and assignment logic.
 * Class implements WritableComparable, with the required methods.
 * Trigram is used through the 5 steps, in order to represent a 3gram which is:
 *      Writeable
 *      Comparable
 *      String represented as the given 3gram
 */
public class Trigram implements WritableComparable<Trigram> {


    private final Text word1;
    private final Text word2;
    private final Text word3;


    public Trigram(String s, String s1, String s2) {
        this.word1 = new Text(s);
        this.word2 = new Text(s1);
        this.word3 = new Text(s2);
    }


    public Trigram(Text s, Text s1, Text s2) {
        this.word1 = s;
        this.word2 = s1;
        this.word3 = s2;
    }
    public Trigram(){
        this.word1 = new Text("");
        this.word2 = new Text("");
        this.word3 = new Text("");
    }


    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Trigram && ((Trigram) obj).getWord1().equals(this.word1) && ((Trigram) obj).getWord2().equals(this.word2) && ((Trigram) obj).getWord3().equals(this.word3));
    }

    @Override
    public int hashCode() {
        return Objects.hash(word1, word2, word3);
    }

    public Text getWord1() {
        return word1;
    }


    public Text getWord2() {
        return word2;
    }


    public Text getWord3() {
        return word3;
    }


    @Override
    public int compareTo(Trigram o) {
        String me = word1.toString() + " " + word2.toString() ;
        String other = o.getWord1().toString() + " "  + o.getWord2().toString() ;
        return me.compareTo(other);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        word3.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        word3.readFields(dataInput);
    }

    @Override
    public String toString() {
        return word1.toString() + " " + word2.toString() + " " + word3.toString();
    }
}