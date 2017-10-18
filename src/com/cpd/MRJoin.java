package com.cpd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRJoin {

    public MRJoin() {
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 6) {
            System.err.println("Usage: com.cpd.MRJoin <in> <left> <right> <field1> <field2> <out> [conf]");
            System.exit(2);
        }

        conf.set("left_filename", args[1]);
        conf.set("right_filename", args[2]);
        conf.set("field1", args[3]);
        conf.set("field2", args[4]);
        for (int i = 6; i < args.length; i += 2) {
            conf.set(args[i], args[i + 1]);
        }

        Job job = Job.getInstance(conf, "join");
        job.setJarByClass(MRJoin.class);
        job.setMapperClass(MRJoin.TokenizerMapper.class);
        job.setReducerClass(MRJoin.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private ArrayList<String> left_fields = new ArrayList<String>();
        private ArrayList<String> right_fields = new ArrayList<String>();
        private Text output = new Text();

        public IntSumReducer() {
        }

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            left_fields.clear();
            right_fields.clear();
            Text val;
            Configuration conf = context.getConfiguration();
            String left_filename = conf.get("left_filename");
            String right_filename = conf.get("right_filename");

            for (Iterator i$ = values.iterator(); i$.hasNext(); ) {
                val = (Text) i$.next();
                String filename = val.toString().split("\\|")[0];
                String fields = val.toString().substring(filename.length() + 1);
                if (filename.endsWith(left_filename)) {
                    left_fields.add(fields);
                } else if (filename.endsWith(right_filename)) {
                    right_fields.add(fields);
                }
            }

            for (String leftPart : left_fields) {
                for (String rightPart : right_fields) {
                    output.set(leftPart + rightPart);
                    context.write(key, output);
                }
            }
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text common_field = new Text();
        private Text output = new Text();

        public TokenizerMapper() {
        }

        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            Configuration conf = context.getConfiguration();
            String left_filename = conf.get("left_filename");
            String right_filename = conf.get("right_filename");
            int field1 = Integer.parseInt(conf.get("field1"));
            int field2 = Integer.parseInt(conf.get("field2"));

            if (pathName.endsWith(left_filename)) {
                common_field.set(value.toString().split("\\|")[field1]);
            } else if (pathName.endsWith(right_filename)) {
                common_field.set(value.toString().split("\\|")[field2]);
            }
            output.set(pathName + "|" + value.toString());
            context.write(common_field, output);

        }
    }
}
