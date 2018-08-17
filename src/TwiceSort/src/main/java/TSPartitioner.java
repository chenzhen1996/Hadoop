import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TSPartitioner extends Partitioner<ComboKey, NullWritable> {
    public int getPartition(ComboKey comboKey, NullWritable nullWritable, int i) {
        return comboKey.getYear()%i;
    }
}
