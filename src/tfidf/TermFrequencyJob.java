package tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public final class TermFrequencyJob {
    private TermFrequencyJob() {}

    public static class DocMapper extends Mapper<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(Text termAtDoc, Text count, Context ctx) throws IOException, InterruptedException {
            String s = termAtDoc.toString();
            int at = s.lastIndexOf('@');
            outKey.set(s.substring(at + 1));
            outValue.set(s.substring(0, at) + "=" + count);
            ctx.write(outKey, outValue);
        }
    }

    public static class DocLengthReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text doc, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            List<String> terms = new ArrayList<>();
            List<Integer> counts = new ArrayList<>();
            long docLength = 0;
            for (Text v : values) {
                String s = v.toString();
                int eq = s.lastIndexOf('=');
                int n = Integer.parseInt(s.substring(eq + 1));
                terms.add(s.substring(0, eq));
                counts.add(n);
                docLength += n;
            }
            ctx.getCounter(Counters.DOCUMENTS).increment(1);
            for (int i = 0; i < terms.size(); i++) {
                outKey.set(terms.get(i) + "@" + doc);
                outValue.set(counts.get(i) + "/" + docLength);
                ctx.write(outKey, outValue);
            }
        }
    }
}
