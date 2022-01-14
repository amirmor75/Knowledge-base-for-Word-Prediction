package jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import writables.DataPair;

import java.io.IOException;


public class Job5Zip3With4 {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outval = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");
            String[] values = value.toString().split(" ");
            if(words.length > 2){ // the key is a Trigram
                outKey.set(String.format("%s %s",words[1],words[2]));
                outval.set(String.format("%s %s %s %d",words[0],words[1], words[3],Integer.parseInt(values[1])));
            }
            else{ // the key is a pair of words
                outKey.set(String.format("%s %s",words[1],words[2]));
                outval.set(String.format("%d", Integer.parseInt(values[1])));
            }
            context.write(outKey, outval);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, DataPair> {
        private final Text outKey = new Text();
        private final Text outval = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int singleWordCount = -1 ;
            int sum = 0;
            for (Text value : values) {
                String[] words = value.toString().split("\t");
                if(words.length==1){
                    singleWordCount= Integer.parseInt(words[0]);
                }
            }
            for (Text value : values) {
                String[] words = value.toString().split("\t");
                if(words.length>1){
                    outKey.set(String.format("%s %s",words[0],words[1]));
                    context.write(key, new DataPair(sum,Integer.parseInt(words[2])));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

}




