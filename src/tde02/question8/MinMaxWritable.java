package tde02.question8;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxWritable implements Writable {
    private long min;
    private long max;

    public MinMaxWritable() {
        this.min = Long.MAX_VALUE;
        this.max = Long.MIN_VALUE;
    }

    public MinMaxWritable(long min, long max) {
        this.min = min;
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(min);
        out.writeLong(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        min = in.readLong();
        max = in.readLong();
    }

    @Override
    public String toString() {
        return "Min: " + min + "\t  Max: " + max;
    }
}