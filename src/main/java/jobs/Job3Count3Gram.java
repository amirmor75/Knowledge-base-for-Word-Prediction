package jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Job3Count3Gram {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static IntWritable count;
        private final Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");
            if (words.length == 3) {
                count = new IntWritable(Integer.parseInt(strings[2]));
                outKey.set(String.format("%s %s %s", words[0], words[1], words[3]));
                context.write(outKey, count);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

}


