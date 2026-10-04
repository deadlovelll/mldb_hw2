package tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public final class TopNJob {
    public static final String TOP_N = "tfidf.top.n";

    private TopNJob() {}

    public static class DocScoreKey implements WritableComparable<DocScoreKey> {
        private String doc = "";
        private double score;
        private String term = "";

        public DocScoreKey() {}

        DocScoreKey(String doc, double score, String term) {
            this.doc = doc;
            this.score = score;
            this.term = term;
        }

        public String getDoc() { return doc; }
        public double getScore() { return score; }
        public String getTerm() { return term; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(doc);
            out.writeDouble(score);
            out.writeUTF(term);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            doc = in.readUTF();
            score = in.readDouble();
            term = in.readUTF();
        }

        @Override
        public int compareTo(DocScoreKey o) {
            int c = doc.compareTo(o.doc);
            if (c != 0) return c;
            c = Double.compare(o.score, score);
            if (c != 0) return c;
            return term.compareTo(o.term);
        }

        @Override
        public int hashCode() { return doc.hashCode(); }

        @Override
        public boolean equals(Object o) {
            return o instanceof DocScoreKey && compareTo((DocScoreKey) o) == 0;
        }
    }

    public static class DocPartitioner extends Partitioner<DocScoreKey, Text> {
        @Override
        public int getPartition(DocScoreKey key, Text value, int numPartitions) {
            return (key.getDoc().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class DocGroupingComparator extends WritableComparator {
        public DocGroupingComparator() { super(DocScoreKey.class, true); }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            return ((DocScoreKey) a).getDoc().compareTo(((DocScoreKey) b).getDoc());
        }
    }

    public static class TopNMapper extends Mapper<Text, Text, DocScoreKey, Text> {
        private final Map<String, PriorityQueue<DocScoreKey>> heaps = new HashMap<>();
        private int n;

        @Override
        protected void setup(Context ctx) { n = ctx.getConfiguration().getInt(TOP_N, 10); }

        @Override
        protected void map(Text termAtDoc, Text score, Context ctx) {
            double s = Double.parseDouble(score.toString());
            if (s <= 0) return;
            String str = termAtDoc.toString();
            int at = str.lastIndexOf('@');
            DocScoreKey k = new DocScoreKey(str.substring(at + 1), s, str.substring(0, at));
            PriorityQueue<DocScoreKey> heap = heaps.computeIfAbsent(k.getDoc(),
                    d -> new PriorityQueue<>(n + 1, (a, b) -> b.compareTo(a)));
            heap.add(k);
            if (heap.size() > n) heap.poll();
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            Text term = new Text();
            for (PriorityQueue<DocScoreKey> heap : heaps.values()) {
                for (DocScoreKey k : heap) {
                    term.set(k.getTerm());
                    ctx.write(k, term);
                    ctx.getCounter(Counters.TOPN_CANDIDATES).increment(1);
                }
            }
        }
    }

    public static class TopNReducer extends Reducer<DocScoreKey, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private int n;

        @Override
        protected void setup(Context ctx) { n = ctx.getConfiguration().getInt(TOP_N, 10); }

        @Override
        protected void reduce(DocScoreKey key, Iterable<Text> terms, Context ctx) throws IOException, InterruptedException {
            outKey.set(key.getDoc());
            int rank = 0;
            for (Text term : terms) {
                if (++rank > n) break;
                outValue.set(rank + "\t" + term + "\t" + key.getScore());
                ctx.write(outKey, outValue);
            }
        }
    }
}
