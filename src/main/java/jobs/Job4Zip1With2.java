package jobs;

        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Partitioner;
        import org.apache.hadoop.mapreduce.Reducer;

        import java.io.IOException;


public class Job4Zip1With2 {

    public static class MapperClass extends Mapper<Text, IntWritable, Text, Text> {

        private final Text outKey = new Text();
        private final Text outval = new Text();

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] strings = key.toString().split("\t");
            String[] words = strings[0].split(" ");


            if(words.length > 1){
                outKey.set(String.format("%s",words[1]));
                outval.set(String.format("%s\t%s\t%d",words[0],words[1],value.get()));
            }
            else{
                outKey.set(String.format("%s",words[0]));
                outval.set(String.valueOf(value.get()));
                context.write(outKey , outval);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, IntWritable> {
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
                    outKey.set();
                    context.write(key, new IntWritable(sum));
                }
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



