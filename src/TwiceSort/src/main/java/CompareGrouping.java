import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class CompareGrouping   extends WritableComparator {


    protected CompareGrouping() {
            super(ComboKey.class, true);
        }

        public int compare(WritableComparable a, WritableComparable b) {
            ComboKey k1 = (ComboKey)a ;
            ComboKey k2 = (ComboKey)b ;
            return k1.getYear() - k2.getYear() ;
        }
}

