package org.bench.mergest;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.PriorityQueue;
import org.bench.mergest.type.hadoop.RawComparator;
import org.bench.mergest.type.hadoop.Text;
import org.bench.mergest.type.hadoop.Writable;
import org.bench.mergest.type.hadoop.WritableComparator;
import org.bench.util.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Extract the core logic of hadoop merge sort and find the relationship
 * between segment size and sort performance.
 * <p>
 * The sort execute model is as follows:
 * <p>
 * 1. inner sort each segment
 * 2. merge sort all segments to get the total order
 * <p>
 * In the test, we use the {@link org.apache.hadoop.mapred.MapTask.MapOutputBuffer} data structure.
 * Each segment has raw data part and inner index part. Raw data starts from the segment head,
 * while inner index is from the segment end.
 */
public class MergeSortMain {
    interface Serializer<T> {

        void open(OutputStream stream) throws IOException;

        void serialize(T data) throws IOException;

        void close() throws IOException;
    }

    public interface RecordIterator {

        DataInputBuffer getRecord() throws IOException;

        boolean next() throws IOException;

        void close() throws IOException;
    }

    static class WritableSerializer implements Serializer<Writable> {
        private DataOutputStream dataOut;

        @Override
        public void open(OutputStream out) {
            if (out instanceof DataOutputStream) {
                dataOut = (DataOutputStream) out;
            } else {
                dataOut = new DataOutputStream(out);
            }
        }

        @Override
        public void serialize(Writable data) throws IOException {
            data.write(dataOut);
        }

        @Override
        public void close() throws IOException {
            dataOut.close();
        }
    }

    static class Buffer<T extends Writable> implements IndexedSortable {
        private final static int INT_SIZE = 4;
        private static final int RECSTART = 0;         // record offset in acct
        private static final int RECLEN = 1;           // length of record
        private static final int NMETA = 2;            // num meta ints
        private static final int METASIZE = NMETA * INT_SIZE; // size in bytes

        private byte[] buffer;
        private int position;
        private int capacity;

        private int kvindex;
        private int kvstart;
        private int kvend;

        private int maxRec;
        private int recordCounter;
        private int bufferRemaining;
        private IntBuffer kvmeta;
        private Serializer<Writable> serializer;
        private RawComparator<T> comparator;
        private IndexedSorter sorter;

        public Buffer(int size, RawComparator<T> comparator) throws IOException {
            buffer = new byte[size];
            position = 0;
            capacity = buffer.length;
            bufferRemaining = capacity;

            this.comparator = comparator;
            kvmeta = ByteBuffer.wrap(buffer).asIntBuffer();
            maxRec = kvmeta.capacity() / NMETA;

            serializer = new WritableSerializer();
            serializer.open(new DataOutputStream(new BlockOutput()));

            //  sorter = new QuickSort();
            sorter = new HeapSort();

            setEquator(0);
            kvstart = kvend = kvindex;
        }

        public int getRecordCounter() {
            return recordCounter;
        }

        void setEquator(int pos) {
            // set index prior to first entry, aligned at meta boundary
            final int aligned = pos - (pos % METASIZE);
            // Cast one of the operands to long to avoid integer overflow
            kvindex = (int) (((long) aligned - METASIZE + capacity) % capacity) / 4;
        }

        void serialize(T record) throws IOException {
            bufferRemaining -= METASIZE;
            if (bufferRemaining <= 0) {
                throw new ArrayIndexOutOfBoundsException();
            }

            int recStart = position;
            serializer.serialize(record);
            int recEnd = position;

            kvmeta.put(kvindex + RECSTART, recStart);
            kvmeta.put(kvindex + RECLEN, distanceTo(recStart, recEnd));

            // advance kvindex
            kvindex = (kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity();
            recordCounter++;
        }

        private final int distanceTo(final int i, final int j) {
            return distanceTo(i, j, capacity);
        }

        private int distanceTo(final int i, final int j, final int mod) {
            return i <= j ? j - i : mod - i + j;
        }

        public void sort() {
            kvend = (kvindex + NMETA) % kvmeta.capacity();

            final int mstart = kvend / NMETA;
            final int mend = 1 + // kvend is a valid record
                    (kvstart >= kvend
                            ? kvstart
                            : kvmeta.capacity() + kvstart) / NMETA;

            sorter.sort(this, mstart, mend);
        }

        public byte[] getBuffer() {
            return buffer;
        }

        private int offsetFor(int metapos) {
            return metapos * NMETA;
        }

        @Override
        public int compare(int mi, int mj) {
            final int kvi = offsetFor(mi % maxRec);
            final int kvj = offsetFor(mj % maxRec);

            return comparator.compare(
                    buffer,
                    kvmeta.get(kvi + RECSTART),
                    kvmeta.get(kvi + RECLEN),
                    buffer,
                    kvmeta.get(kvj + RECSTART),
                    kvmeta.get(kvj + RECLEN)
            );
        }

        final byte META_BUFFER_TMP[] = new byte[METASIZE];

        @Override
        public void swap(int mi, int mj) {
            int iOff = (mi % maxRec) * METASIZE;
            int jOff = (mj % maxRec) * METASIZE;
            System.arraycopy(buffer, iOff, META_BUFFER_TMP, 0, METASIZE);
            System.arraycopy(buffer, jOff, buffer, iOff, METASIZE);
            System.arraycopy(META_BUFFER_TMP, 0, buffer, jOff, METASIZE);
        }

        public int getPosition() {
            return position;
        }

        public int getCapacity() {
            return capacity;
        }

        public RecReader getIterator() {
            kvend = (kvindex + NMETA) % kvmeta.capacity();
            final int mstart = kvend / NMETA;
            final int mend = 1 + // kvend is a valid record
                    (kvstart >= kvend
                            ? kvstart
                            : kvmeta.capacity() + kvstart) / NMETA;

            return new RecReader(mstart, mend);
        }

        class BlockOutput extends OutputStream {
            final byte[] scratch = new byte[1];

            @Override
            public void write(int b) throws IOException {
                scratch[0] = (byte) (b & 0xff);
                write(scratch, 0, 1);
            }

            @Override
            public void write(byte b[], int off, int len) {
                if (position + len > bufferRemaining) {
                    throw new ArrayIndexOutOfBoundsException("currentCounter buffer position "
                            + position + " bytes, need len is " + len
                            + ", which exceeds partsize " + bufferRemaining);
                }

                System.arraycopy(b, off, buffer, position, len);
                position += len;
            }
        }

        protected class RecReader implements RecordIterator {
            private final DataInputBuffer recordBuf = new DataInputBuffer();
            private final int end;
            private int current;

            public RecReader(int start, int end) {
                this.end = end;
                current = start - 1;
            }

            @Override
            public DataInputBuffer getRecord() throws IOException {
                final int kvoff = offsetFor(current % maxRec);
                //  System.out.println("kvmeta " + kvmeta.get(kvoff + RECSTART) + " " + kvmeta.get(kvoff + RECLEN));
                recordBuf.reset(buffer, kvmeta.get(kvoff + RECSTART), kvmeta.get(kvoff + RECLEN));
                return recordBuf;
            }

            public boolean next() throws IOException {
                return ++current < end;
            }

            public void close() {
            }
        }
    }

    interface IComparator<V> extends Comparator<V> {
        int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);
    }

    static class Merger<T extends Writable> extends PriorityQueue {
        List<Buffer> buffers;
        WritableComparator comparator;
        Buffer.RecReader minSegment;
        DataInputBuffer currRecord;

        Merger(WritableComparator comparator, List<Buffer> buffers) {
            this.comparator = comparator;
            this.buffers = buffers;

            initialize(buffers.size());
            try {
                for (Buffer r : buffers) {
                    r.sort();
                    Buffer.RecReader reader = r.getIterator();
                    boolean hasNext = reader.next();
                    if (hasNext) {
                        put(reader);
                    } else {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        DataInputBuffer getRecord() {
            return currRecord;
        }

        private void adjustPriorityQueue(Buffer.RecReader reader) throws IOException {
            boolean hasNext = reader.next();
            if (hasNext) {
                adjustTop();
            } else {
                pop();
                reader.close();
            }
        }

        public boolean next() throws IOException {
            if (size() == 0)
                return false;

            if (minSegment != null) {
                adjustPriorityQueue(minSegment);
                if (size() == 0) {
                    minSegment = null;
                    return false;
                }
            }
            minSegment = (Buffer.RecReader) top();
            currRecord = minSegment.getRecord();

            return true;
        }

        @SuppressWarnings("unchecked")
        protected boolean lessThan(Object a, Object b) {
            DataInputBuffer key1 = null;
            DataInputBuffer key2 = null;
            try {
                key1 = ((Buffer.RecReader) a).getRecord();
                key2 = ((Buffer.RecReader) b).getRecord();
            } catch (IOException e) {
                e.printStackTrace();
            }
            int s1 = key1.getPosition();
            int l1 = key1.getLength() - s1;
            int s2 = key2.getPosition();
            int l2 = key2.getLength() - s2;
            return comparator.compare(key1.getData(), s1, l1, key2.getData(),
                    s2, l2) < 0;
        }

    }

    static class RecordReader extends DataInputStream {
        public RecordReader(InputStream in) {
            super(in);
        }

        public void setInput(InputStream in) {
            this.in = in;
        }
    }

    static Buffer<Text> createBuffer(int size) throws IOException {
        Buffer<Text> buffer = new Buffer<>(size, new Text.Comparator());
        Text t = new Text();
        while (true) {
            t.set(Utils.randomString(RECORD_LENGTH));
            try {
                buffer.serialize(t);
            } catch (ArrayIndexOutOfBoundsException e) {
                //  buffer.sort();
                break;
            }
        }
        return buffer;
    }

    static class GenerateData extends Thread {
        int blockDataSize;
        Lock lock;
        AtomicInteger currentCounter;
        List<Buffer<Text>> buffers;
        final int totalSize;

        GenerateData(int blockDataSize, Lock lock, AtomicInteger currentCounter, List<Buffer<Text>> buffers, int totalSize) {
            this.blockDataSize = blockDataSize;
            this.lock = lock;
            this.currentCounter = currentCounter;
            this.buffers = buffers;
            this.totalSize = totalSize;
        }

        @Override
        public void run() {
            try {
                while (!currentCounter.compareAndSet(totalSize, totalSize)) {
                    Buffer<Text> bf = createBuffer(blockDataSize);

                    lock.lock();
                    try {
                        if (currentCounter.compareAndSet(totalSize, totalSize)) {
                            break;
                        } else {
                            buffers.add(bf);
                            int n = currentCounter.incrementAndGet();
                            //  if (n % 10 == 0) {
                            //      System.err.println("generate data " + n);
                            //  }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    final static int MAX_THREAD_NUMBER = 10;
    final static int RECORD_LENGTH = 10;
    static boolean isFlushOutPut = false;

    public static void main(String args[]) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println(String.format("Usage %s: <total-size> <block-size> [<is output>]", MergeSortMain.class.getSimpleName()));
            System.exit(-1);
        }
        int totalDataSize = Integer.valueOf(args[0]);
        int blockDataSize = Integer.valueOf(args[1]);
        if (args.length == 3)
            isFlushOutPut = Boolean.valueOf(args[2]);

        totalDataSize <<= 20;  // MB
        blockDataSize <<= 10;  // KB

        int blockNumber = totalDataSize / blockDataSize;
        System.err.println(String.format("Total data size %d MB, block data size %d KB, total blocks: %d, output file: %s",
                blockNumber * blockDataSize / 1024 / 1024, blockDataSize / 1024, blockNumber, isFlushOutPut));

        // fix the random seed, make sure to generate the same data each time
        Utils.rnd.setSeed(100);

        // concurrently generate the block data
        long createBegin = System.currentTimeMillis();
        int threadNumber = blockNumber > MAX_THREAD_NUMBER ? MAX_THREAD_NUMBER : blockNumber;
        Lock lock = new ReentrantLock();
        AtomicInteger counter = new AtomicInteger(0);
        Thread[] threads = new Thread[threadNumber];
        List<Buffer<Text>> buffers = new ArrayList<>();
        assert counter.get() < blockNumber;
        for (int i = 0; i < threadNumber; i++) {
            threads[i] = new GenerateData(blockDataSize, lock, counter, buffers, blockNumber);
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long createEnd = System.currentTimeMillis();
        System.err.println(String.format("create data cost: %.5f sec", (createEnd - createBegin) / 1000f));

        // inner sort each block
        long innerSortBegin = System.currentTimeMillis();
        Merger m = new Merger(new Text.Comparator(), buffers);
        long innerSortEnd = System.currentTimeMillis();
        System.err.println(String.format("inner sort data cost: %.5f sec", (innerSortEnd - innerSortBegin) / 1000f));

        DataOutputStream w = null;
        if (isFlushOutPut) {
            String filename = "file-" + (System.currentTimeMillis() % 1000000);
            File f = new File(filename);
            w = new DataOutputStream(new FileOutputStream(f));
        }

        RecordReader rr = new RecordReader(null);
        Text newT = new Text();

        // merge sort all of the blocks to output orderly
        long mergeSortBegin = System.currentTimeMillis();
        while (m.next()) {
            rr.setInput(m.getRecord());
            newT.readFields(rr);
            // System.out.println(newT);
            if (isFlushOutPut) newT.write(w);
        }
        if (isFlushOutPut) w.close();
        long mergeSortEnd = System.currentTimeMillis();

        System.err.println(String.format("merge sort cost: %.5f sec", (mergeSortEnd - mergeSortBegin) / 1000f));
    }
}
