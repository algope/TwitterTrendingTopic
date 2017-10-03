package master2016;

import com.google.common.collect.ComparisonChain;

public class Hashtag {

    private String hash;
    private int count;

    public Hashtag (String hash, int count){
        this.hash=hash;
        this.count=count;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return hash+","+count;
    }

}
