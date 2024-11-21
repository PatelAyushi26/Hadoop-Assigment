// Q7-4 : Write a mapreduce job to display only titles of the movie having “Gold” anywhere in the title.

package exam;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GoldMovies {

    public static class GoldMapper extends Mapper<Object, Text, Text, Text> {
        private Text title = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String movieTitle = fields[1];  // Assuming title is the 2nd field
            if (movieTitle.contains("Gold")) {
                title.set(movieTitle);
                context.write(title, new Text(""));
            }
        }
    }

    public static class GoldReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "gold movies");
        job.setJarByClass(GoldMovies.class);
        job.setMapperClass(GoldMapper.class);
        job.setCombinerClass(GoldReducer.class);
        job.setReducerClass(GoldReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}