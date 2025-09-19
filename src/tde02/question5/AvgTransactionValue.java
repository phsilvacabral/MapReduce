package tde02.question5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgTransactionValue implements Writable {

    private float value;
    private int freq;

    public AvgTransactionValue() {
    }

    public AvgTransactionValue(float value, int freq) {
        this.value = value;
        this.freq = freq;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(value);
        dataOutput.writeInt(freq);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = dataInput.readFloat();
        freq = dataInput.readInt();
    }
}
