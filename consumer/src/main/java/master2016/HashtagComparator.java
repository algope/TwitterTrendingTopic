package master2016;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.Comparator;

public class HashtagComparator implements Comparator<Hashtag> {
    @Override
    public int compare(Hashtag o1, Hashtag o2) {
        return ComparisonChain.start()
                .compare(o1.getCount(), o2.getCount(), Ordering.natural().reverse())
                .compare(o1.getHash(), o2.getHash())
                .result();
    }
}
