package jobs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class Job7Sort {


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keySplit = key.toString().split(" ");

            Text trigram = new Text( String.format("%s %s %s",keySplit[0],keySplit[1],keySplit[2]));
            Text prob = new Text(keySplit[3]);
            context.write(trigram,prob);
        }

    }
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class Comparator extends WritableComparator {
        protected Comparator() {
            super(Text.class, true);
        }
        @Override
        public int compare(WritableComparable key, WritableComparable other) { // key: w1 w2 w3 probability
            String[] keySplit = key.toString().split(" ");
            String[] otherSplit = other.toString().split(" ");
            if (keySplit[0].equals(otherSplit[0]) & keySplit[1].equals(otherSplit[1])) {
                if(Double.parseDouble(keySplit[3]) >= (Double.parseDouble(otherSplit[3]))){
                    return -1;
                }
                else
                    return 1;
            }
            return String.format("%s %s", keySplit[0], keySplit[1]).compareTo(String.format("%s %s", otherSplit[0], otherSplit[1]));
        }
    }

}






