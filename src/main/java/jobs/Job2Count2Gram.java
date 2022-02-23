package jobs;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Job2Count2Gram {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private  static IntWritable count ;
        private final Text outKey= new Text();

        private boolean isAlphabeticSentence(String str){
            for (int i = 0; i < str.length() ; i++) {
                char curr = str.charAt(i);
                if (curr<'א' || curr>'ת'){
                    if(curr != ' ')
                        return false;
                }
            }
            return true;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");
            if(words.length == 2){
                count = new IntWritable( Integer.parseInt(strings[2]));
                String w1w2 = String.format("%s %s",words[0], words[1]);
                outKey.set(w1w2);
                if (isAlphabeticSentence(w1w2))
                    context.write(outKey ,count);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
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
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


}

