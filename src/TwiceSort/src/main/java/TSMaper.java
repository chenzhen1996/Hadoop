import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TSMaper extends Mapper<IntWritable, IntWritable, ComboKey, NullWritable>{
    @Override
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {

        ComboKey comboKey=new ComboKey();
        comboKey.setYear(key.get());
        comboKey.setTemp(value.get());

        context.write(comboKey,NullWritable.get());
    }
}
