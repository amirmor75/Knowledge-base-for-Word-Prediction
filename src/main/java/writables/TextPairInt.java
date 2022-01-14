package writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class TextPairInt implements WritableComparable<TextPairInt> {


    private final Text w1;
    private final Text w2;
    private final IntWritable count;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPairInt that = (TextPairInt) o;
        return Objects.equals(w1, that.w1) && Objects.equals(w2, that.w2) && Objects.equals(count, that.count);
    }

    @Override
    public String toString() {
        return w1.toString()+" "+w2.toString()+" "+String.valueOf(count.get());
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public IntWritable getCount() {
        return count;
    }

    public TextPairInt(Text w1, Text w2, IntWritable count) {
        this.w1 = w1;
        this.w2 = w2;
        this.count = count;
    }

    @Override
    public int compareTo(TextPairInt o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
