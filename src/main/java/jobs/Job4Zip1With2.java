package jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Job4Zip1With2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outval = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");


            if(words.length > 1){ // the key a 2 word pair
                outKey.set(String.format("%s",words[1]));
                outval.set(String.format("%s %s %d",words[0],words[1],Integer.parseInt(strings[1])));
            }
            else{ // the key is one word
                outKey.set(String.format("%s",words[0]));
                outval.set(String.valueOf(Integer.parseInt(strings[1])));
            }
            context.write(outKey , outval);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int singleWordCount = -1 ;

            for (Text value : values) {
                String[] words = value.toString().split(" ");
                if(words.length==1){
                    singleWordCount= Integer.parseInt(words[0]);
                }
            }
            for (Text value : values) {
                String[] words = value.toString().split(" ");
                if(words.length>1){
                    outKey.set(String.format("%s %s",words[0],words[1]));
                    outVal.set(String.format("%d %d",singleWordCount,Integer.parseInt(words[2])));
                    context.write(outKey,outVal);
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



