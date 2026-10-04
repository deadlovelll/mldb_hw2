package tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public final class TfIdfJob {
    public static final String NUM_DOCS = "tfidf.num.docs";

    private TfIdfJob() {}

    public static class TermMapper extends Mapper<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(Text termAtDoc, Text tf, Context ctx) throws IOException, InterruptedException {
            String s = termAtDoc.toString();
            int at = s.lastIndexOf('@');
            outKey.set(s.substring(0, at));
            outValue.set(s.substring(at + 1) + "=" + tf);
            ctx.write(outKey, outValue);
        }
    }

    public static class TfIdfReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();
        private long numDocs;

        @Override
        protected void setup(Context ctx) {
            numDocs = ctx.getConfiguration().getLong(NUM_DOCS, -1);
            if (numDocs <= 0) {
                throw new IllegalStateException(NUM_DOCS + " is not set");
            }
        }

        @Override
        protected void reduce(Text term, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            List<String> docs = new ArrayList<>();
            List<Double> tfs = new ArrayList<>();
            for (Text v : values) {
                String s = v.toString();
                int eq = s.lastIndexOf('=');
                int slash = s.lastIndexOf('/');
                docs.add(s.substring(0, eq));
                tfs.add(Double.parseDouble(s.substring(eq + 1, slash)) / Double.parseDouble(s.substring(slash + 1)));
            }
            double idf = Math.log((double) numDocs / docs.size());
            ctx.getCounter(Counters.UNIQUE_TERMS).increment(1);
            for (int i = 0; i < docs.size(); i++) {
                outKey.set(term + "@" + docs.get(i));
                outValue.set(tfs.get(i) * idf);
                ctx.write(outKey, outValue);
            }
        }
    }
}
