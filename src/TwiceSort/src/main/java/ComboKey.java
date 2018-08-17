import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComboKey implements WritableComparable<ComboKey> {

    private int year ;
    private int temp ;
    public int compareTo(ComboKey o) {
        int y0=o.getYear();
        int t0=o.getTemp();
        if(y0==year){
            return -(temp-t0);
        }else{
            return year-y0;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(temp);
    }

    public void readFields(DataInput in) throws IOException {
        year=in.readInt();
        temp=in.readInt();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }
}
