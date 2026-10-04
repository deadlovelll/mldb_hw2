package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public final class TermCountJob {
    private TermCountJob() {}

    public static class TokenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text outKey = new Text();
        private String doc;
        private String currentFile;
        private String currentName;

        @Override
        protected void setup(Context ctx) {
            InputSplit split = ctx.getInputSplit();
            if (split instanceof FileSplit) {
                doc = ((FileSplit) split).getPath().getName();
            }
        }

        private String currentDoc(Context ctx) {
            if (doc != null) {
                return doc;
            }
            String file = ctx.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE);
            if (!file.equals(currentFile)) {
                currentFile = file;
                currentName = new Path(file).getName();
            }
            return currentName;
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String doc = currentDoc(ctx);
            for (String term : Tokenizer.tokenize(line.toString())) {
                outKey.set(term + "@" + doc);
                ctx.write(outKey, ONE);
                ctx.getCounter(Counters.TOKENS).increment(1);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            result.set(sum);
            ctx.write(key, result);
        }
    }
}
