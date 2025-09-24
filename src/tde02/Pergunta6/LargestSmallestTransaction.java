package tde02.Pergunta6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LargestSmallestTransaction implements Writable{

    private float largest;
    private float smallest;


    public LargestSmallestTransaction() {
    }

    public LargestSmallestTransaction(float largest, float smallest) {
        this.largest = largest;
        this.smallest = smallest;
    }

    public float getLargest() {
        return largest;
    }

    public void setLargest(float largest) {
        this.largest = largest;
    }

    public float getSmallest() {
        return smallest;
    }

    public void setSmallest(float smallest) {
        this.smallest = smallest;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(largest);
        dataOutput.writeFloat(smallest);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        largest = dataInput.readFloat();
        smallest = dataInput.readFloat();
    }
}