package tfidf;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TfIdfDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TfIdfDriver [-D tfidf.reducers=R] [-D tfidf.top.n=N] <input> <output>");
            return 2;
        }
        Configuration conf = getConf();
        Path input = new Path(args[0]);
        Path out = new Path(args[1]);
        int reducers = conf.getInt("tfidf.reducers", 4);

        FileSystem fs = out.getFileSystem(conf);
        fs.delete(out, true);

        long numDocs = 0;
        for (FileStatus st : input.getFileSystem(conf).listStatus(input)) {
            if (st.isFile()) numDocs++;
        }
        conf.setLong(TfIdfJob.NUM_DOCS, numDocs);
        System.out.printf("Documents: %d, reducers: %d, top-N: %d, combine.mb: %d%n", numDocs, reducers,
                conf.getInt(TopNJob.TOP_N, 10), conf.getLong("tfidf.combine.mb", 0));

        Path counts = new Path(out, "1_term_counts");
        Path tf = new Path(out, "2_tf");
        Path tfidf = new Path(out, "3_tfidf");
        Path top = new Path(out, "4_top_n");
        List<String> report = new ArrayList<>();
        long t0 = System.nanoTime();

        Job j1 = Job.getInstance(conf, "tfidf-1-term-count");
        j1.setJarByClass(TfIdfDriver.class);
        long combineMb = conf.getLong("tfidf.combine.mb", 0);
        if (combineMb > 0) {
            j1.setInputFormatClass(CombineTextInputFormat.class);
            CombineTextInputFormat.setMaxInputSplitSize(j1, combineMb << 20);
        } else {
            j1.setInputFormatClass(TextInputFormat.class);
        }
        j1.setMapperClass(TermCountJob.TokenMapper.class);
        j1.setCombinerClass(TermCountJob.SumReducer.class);
        j1.setReducerClass(TermCountJob.SumReducer.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
        j1.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, counts);
        if (!runTimed(j1, report)) return 1;

        Job j2 = Job.getInstance(conf, "tfidf-2-term-frequency");
        j2.setJarByClass(TfIdfDriver.class);
        j2.setInputFormatClass(KeyValueTextInputFormat.class);
        j2.setMapperClass(TermFrequencyJob.DocMapper.class);
        j2.setReducerClass(TermFrequencyJob.DocLengthReducer.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        j2.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(j2, counts);
        FileOutputFormat.setOutputPath(j2, tf);
        if (!runTimed(j2, report)) return 1;

        Job j3 = Job.getInstance(conf, "tfidf-3-tfidf");
        j3.setJarByClass(TfIdfDriver.class);
        j3.setInputFormatClass(KeyValueTextInputFormat.class);
        j3.setMapperClass(TfIdfJob.TermMapper.class);
        j3.setReducerClass(TfIdfJob.TfIdfReducer.class);
        j3.setMapOutputKeyClass(Text.class);
        j3.setMapOutputValueClass(Text.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(DoubleWritable.class);
        j3.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(j3, tf);
        FileOutputFormat.setOutputPath(j3, tfidf);
        if (!runTimed(j3, report)) return 1;

        Job j4 = Job.getInstance(conf, "tfidf-4-top-n");
        j4.setJarByClass(TfIdfDriver.class);
        j4.setInputFormatClass(KeyValueTextInputFormat.class);
        j4.setMapperClass(TopNJob.TopNMapper.class);
        j4.setPartitionerClass(TopNJob.DocPartitioner.class);
        j4.setGroupingComparatorClass(TopNJob.DocGroupingComparator.class);
        j4.setReducerClass(TopNJob.TopNReducer.class);
        j4.setMapOutputKeyClass(TopNJob.DocScoreKey.class);
        j4.setMapOutputValueClass(Text.class);
        j4.setOutputKeyClass(Text.class);
        j4.setOutputValueClass(Text.class);
        j4.setNumReduceTasks(Math.max(1, reducers / 2));
        FileInputFormat.addInputPath(j4, tfidf);
        FileOutputFormat.setOutputPath(j4, top);
        if (!runTimed(j4, report)) return 1;

        System.out.println();
        System.out.println("=== TF-IDF pipeline summary ===");
        report.forEach(System.out::println);
        System.out.printf(Locale.ROOT, "TOTAL_SECONDS %.2f%n", (System.nanoTime() - t0) / 1e9);
        return 0;
    }

    private static boolean runTimed(Job job, List<String> report) throws Exception {
        long t = System.nanoTime();
        boolean ok = job.waitForCompletion(true);
        org.apache.hadoop.mapreduce.Counters c = job.getCounters();
        report.add(String.format(Locale.ROOT,
                "STAGE %-24s %7.2fs  maps=%d reduces=%d map_in=%d map_out=%d combine_out=%d shuffle_bytes=%d reduce_groups=%d reduce_out=%d spilled=%d",
                job.getJobName(), (System.nanoTime() - t) / 1e9,
                c.findCounter(org.apache.hadoop.mapreduce.JobCounter.TOTAL_LAUNCHED_MAPS).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.JobCounter.TOTAL_LAUNCHED_REDUCES).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.TaskCounter.MAP_INPUT_RECORDS).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.TaskCounter.MAP_OUTPUT_RECORDS).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.TaskCounter.COMBINE_OUTPUT_RECORDS).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.TaskCounter.REDUCE_SHUFFLE_BYTES).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.TaskCounter.REDUCE_INPUT_GROUPS).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS).getValue(),
                c.findCounter(org.apache.hadoop.mapreduce.TaskCounter.SPILLED_RECORDS).getValue()));
        return ok;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TfIdfDriver(), args));
    }
}
