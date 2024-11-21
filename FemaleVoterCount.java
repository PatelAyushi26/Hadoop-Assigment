// Q5: Develop a MapReduce job that will count the total number of females voters in the record file Voters.txt. The input text file should be available on the HDFS. The file must have fields : ID,NAME,GENDER,AGE. The output will be in the form of

package exam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FemaleVoterCount {

    public static class VoterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text gender = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 4) {
                gender.set(fields[2].trim());
                if (gender.toString().equalsIgnoreCase("female")) {
                    context.write(gender, one);
                }
            }
        }
    }

    public static class VoterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            context.write(new Text("Number of female voters"), new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "female voter count");
        job.setJarByClass(FemaleVoterCount.class);
        job.setMapperClass(VoterMapper.class);
        job.setCombinerClass(VoterReducer.class);
        job.setReducerClass(VoterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("Voters.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}