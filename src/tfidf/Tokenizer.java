package tfidf;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Tokenizer {
    private static final Pattern WORD = Pattern.compile("[a-z]+");
    private static final Pattern MARKS = Pattern.compile("[\\u0300-\\u036f]");
    public static final int MIN_LEN = 2;

    private Tokenizer() {}

    static String normalize(String line) {
        String s = line.toLowerCase(Locale.ROOT);
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) > 127) {
                return MARKS.matcher(Normalizer.normalize(s, Normalizer.Form.NFD)).replaceAll("");
            }
        }
        return s;
    }

    public static List<String> tokenize(String line) {
        List<String> out = new ArrayList<>();
        Matcher m = WORD.matcher(normalize(line));
        while (m.find()) {
            if (m.end() - m.start() >= MIN_LEN) {
                out.add(m.group());
            }
        }
        return out;
    }
}
