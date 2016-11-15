package org.bench;

import org.apache.hadoop.util.PriorityQueue;
import org.bench.util.Utils;
import org.bench.mergest.type.hadoop.Text;
import org.bench.mergest.type.hadoop.WritableUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Test the basic usage of hadoop {@link PriorityQueue}.
 */
public class PriorityTest {

    interface IComparator<V> extends Comparator<V> {
        int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);
    }

    static class PriorityMain extends PriorityQueue<Text> {
        IComparator<Text> comparator;

        PriorityMain(int size) {
            comparator = new IComparator<Text>() {
                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                    int n1 = WritableUtils.decodeVIntSize(b1[s1]);
                    int n2 = WritableUtils.decodeVIntSize(b2[s2]);
                    return Utils.compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
                }

                @Override
                public int compare(Text o1, Text o2) {
                    return o1.compareTo(o2);
                }
            };

            initialize(size);
        }

        @Override
        protected boolean lessThan(Object o, Object o1) {
            Text r1 = (Text) o;
            Text r2 = (Text) o1;
            return comparator.compare(r1.getBytes(), 0, r1.getLength(), r2.getBytes(), 0, r2.getLength()) < 0;
        }
    }

    static class TextComparator implements IComparator<Text> {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            int n2 = WritableUtils.decodeVIntSize(b2[s2]);
            return Utils.compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
        }

        @Override
        public int compare(Text o1, Text o2) {
            return compare(o1.getBytes(), 0, o1.getLength(), o2.getBytes(), 0, o2.getLength());
        }
    }

    @Test
    public void testEqual() {
        int recordSize = 10, recordLen = 10;
        List<Text> array = new ArrayList<>();

        PriorityMain queue = new PriorityMain(recordSize);
        for (int i = 0; i < recordSize; i++) {
            Text t = new Text(Utils.randomString(recordLen));
            queue.put(t);
            array.add(t);
        }

        Collections.sort(array, new TextComparator());

        for (int i = 0; i < recordSize; i++) {
            Assert.assertEquals(queue.pop(), array.get(i));
        }
    }
}
