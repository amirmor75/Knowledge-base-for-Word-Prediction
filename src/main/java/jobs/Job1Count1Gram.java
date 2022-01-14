package jobs;


import writables.C0;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;


public class Job1Count1Gram {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] strings = value.toString().split("\t");
            word.set(String.format("%s",strings[0]));
            context.write(word, new IntWritable(Integer.parseInt(strings[2])));

        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        private Counter counter;
        @Override
        protected void setup(Context context)
        {
            counter = context.getCounter(C0.C_0);
        }
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
            counter.increment(sum);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


}

