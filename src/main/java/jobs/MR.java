package jobs;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.BasicConfigurator;
import writables.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class MR {

    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
    {
        BasicConfigurator.configure();

        String workingDirBucketName = args[0];
        final boolean isWithCombiners = Boolean.parseBoolean(args[1]);
        boolean retStat;
        final Configuration conf = new Configuration();
        String gram1s3Url="s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
        String gram2s3Url="s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
        String gram3s3Url="s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";


        //-------------------------------------------------------------------------------------
        System.out.println("~configuring job 1~");

        Job job1 = Job.getInstance(conf, "1-Gram word count");
        job1.setJarByClass(Job1Count1Gram.class);
        job1.setMapperClass(Job1Count1Gram.MapperClass.class);

        job1.setPartitionerClass(Job1Count1Gram.PartitionerClass.class);

        if (isWithCombiners)
           job1.setCombinerClass(Job1Count1Gram.Combiner.class);
        job1.setReducerClass(Job1Count1Gram.ReducerClass.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job1, new Path(gram1s3Url));
        FileOutputFormat.setOutputPath(job1, new Path(workingDirBucketName + "step1output"));


        System.out.println("~Starting job 1~");
        System.out.println("Job 1 done with status: "
                + (retStat = job1.waitForCompletion(true)));
        if (!retStat)
            return ;
        long c0Long = job1.getCounters().findCounter(C0.C_0).getValue();
        conf.setLong("C0", c0Long );
        System.out.println("C0's value is : " + conf.getLong("C0",-1));
        //-----------------------------------------------------------------------

        System.out.println("~configuring job 2~");
        Job job2 = Job.getInstance(conf, "2-Gram word count");
        job2.setJarByClass(Job2Count2Gram.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapperClass(Job2Count2Gram.MapperClass.class);

        job2.setPartitionerClass(Job2Count2Gram.PartitionerClass.class);
        if (isWithCombiners)
            job2.setCombinerClass(Job2Count2Gram.ReducerClass.class);
        job2.setReducerClass(Job2Count2Gram.ReducerClass.class);
        job2.setPartitionerClass(Job2Count2Gram.PartitionerClass.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(gram2s3Url));
        FileOutputFormat.setOutputPath(job2, new Path(workingDirBucketName + "step2output"));

        System.out.println("~Starting job 2~");
        System.out.println("Job 2 done with status: "
                + (retStat = job2.waitForCompletion(true)));
        if (!retStat)
            return ;

        //--------------------------------------------------------------------------------------------------------------

        System.out.println("~configuring job 3~");
        Job job3 = Job.getInstance(conf, "3-Gram word count");
        job3.setJarByClass(Job3Count3Gram.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setMapperClass(Job3Count3Gram.MapperClass.class);

        job3.setPartitionerClass(Job3Count3Gram.PartitionerClass.class);
        if (isWithCombiners)
            job3.setCombinerClass(Job3Count3Gram.ReducerClass.class);
        job3.setReducerClass(Job3Count3Gram.ReducerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setPartitionerClass(Job3Count3Gram.PartitionerClass.class);

        FileInputFormat.addInputPath(job3, new Path(gram3s3Url));
        FileOutputFormat.setOutputPath(job3, new Path(workingDirBucketName + "step3output"));

        System.out.println("~Starting job 3~");
        System.out.println("Job 3 done with status: "
                + (retStat = job3.waitForCompletion(true)));
        if (!retStat)
            return ;

        //--------------------------------------------------------------------------------------------------------------

        System.out.println("~configuring job 4~");
        Job job4 = Job.getInstance(conf, "Job4 Zip out1 With out2");
        job4.setJarByClass(Job4Zip1With2.class);


        job4.setMapperClass(Job4Zip1With2.Mapper1Gram.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        if (isWithCombiners) {
            //nothing to do here for now
        }
        job4.setReducerClass(Job4Zip1With2.ReducerClass.class);
        job4.setPartitionerClass(Job4Zip1With2.PartitionerClass.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job4, new Path(workingDirBucketName + "step4output"));
        MultipleInputs.addInputPath(job4, new Path(workingDirBucketName + "step1output"), KeyValueTextInputFormat.class);
        MultipleInputs.addInputPath(job4, new Path(workingDirBucketName + "step2output"), KeyValueTextInputFormat.class);

        System.out.println("~Starting job 4~");
        System.out.println("Job 4 done with status: "
                + (retStat = job4.waitForCompletion(true)));
        if (!retStat)
            return ;

        //--------------------------------------------------------------------------------------------------------------
        System.out.println("~configuring job 5~");
        Job job5 = Job.getInstance(conf);
        job5.setJarByClass(Job5Zip3With4.class);


        job5.setMapperClass(Job5Zip3With4.MapperClass.class);

        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setReducerClass(Job5Zip3With4.ReducerClass.class);
        job5.setPartitionerClass(Job5Zip3With4.PartitionerClass.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job5, new Path(workingDirBucketName + "step3output"), KeyValueTextInputFormat.class);
        MultipleInputs.addInputPath(job5, new Path(workingDirBucketName + "step4output"), KeyValueTextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job5, new Path(workingDirBucketName+ "step5output"));

        System.out.println("~Starting job 5~");
        System.out.println("Job 5 done with status: "
                + (retStat = job5.waitForCompletion(true)));
        if (!retStat)
            return ;
        //--------------------------------------------------------------------------------------------------------------
        System.out.println("Building job 6...");
        Job job6 = Job.getInstance(conf);
        job6.setJarByClass(Job6CalcProb.class);

        job6.setReducerClass(Job6CalcProb.ReducerClass.class);
        job6.setPartitionerClass(Job6CalcProb.PartitionerClass.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);

        job6.setInputFormatClass(KeyValueTextInputFormat.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job6, new Path(workingDirBucketName+"step5output"));
        FileOutputFormat.setOutputPath(job6, new Path(workingDirBucketName + "step6output"));

        System.out.println("~Starting job 6~");
        System.out.println("Job 6 done with status: "
                + (retStat = job6.waitForCompletion(true)));
        if (!retStat)
            return ;
        //--------------------------------------------------------------------------------------------------------------
        System.out.println("Building job 7...");
        Job job7 = Job.getInstance(conf);
        job7.setJarByClass(Job7Sort.class);

        job7.setReducerClass(Job7Sort.ReducerClass.class);
        job7.setPartitionerClass(Job7Sort.PartitionerClass.class);
        job7.setSortComparatorClass(Job7Sort.Comparator.class);

        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);

        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);

        job7.setInputFormatClass(KeyValueTextInputFormat.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job7, new Path(workingDirBucketName+"step6output"));
        FileOutputFormat.setOutputPath(job7, new Path(workingDirBucketName + "finalOutput"));

        System.out.println("~Starting job 7~");
        System.out.println("Job 7 done with status: "
                + (job7.waitForCompletion(true)));
        System.out.println("finished!");
    }
}
