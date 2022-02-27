package jobs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import writables.TrigramWithProb;

import java.io.IOException;

public class Job7Sort {
    public static class MapperClass extends Mapper<Text, Text, TrigramWithProb, Text> {



        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyWords = key.toString().split(" ");
            String[] valSplit = value.toString().split(" ");
            TrigramWithProb trip = new TrigramWithProb(keyWords[0],keyWords[1],keyWords[2],
                    Double.parseDouble(valSplit[0]));
            context.write(trip, new Text(""));
        }
    }

    public static class ReducerClass extends Reducer<TrigramWithProb, Text, Text, Text> {

        @Override
        public void reduce(TrigramWithProb key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text trigram = new Text(key.toStringNoProb());
            Text prob = new Text(String.valueOf(key.getProb()));
            context.write(trigram,prob);
        }

    }
    public static class PartitionerClass extends Partitioner<TrigramWithProb, Text> {
        @Override
        public int getPartition(TrigramWithProb key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

}






