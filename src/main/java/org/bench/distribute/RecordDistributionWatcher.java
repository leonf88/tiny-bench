package org.bench.distribute;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.*;
import java.util.Arrays;

public class RecordDistributionWatcher {
    static abstract class Partitioner<KEY, VALUE> {

        /**
         * Get the partition number for a given key (hence record) given the total
         * number of partitions i.e. number of reduce-tasks for the job.
         * <p>
         * <p>Typically a hash function on a all or a subset of the key.</p>
         *
         * @param key           the key to be partioned.
         * @param value         the entry value.
         * @param numPartitions the total number of partitions.
         * @return the partition number for the <code>key</code>.
         */
        public abstract int getPartition(KEY key, VALUE value, int numPartitions);

    }

    /**
     * A partitioner that splits text keys into roughly equal partitions
     * in a global sorted order.
     */
    static class TotalOrderPartitioner extends Partitioner<Text, Text> {
        private TrieNode trie;
        private Text[] splitPoints;

        /**
         * A generic trie node
         */
        static abstract class TrieNode {
            private int level;

            TrieNode(int level) {
                this.level = level;
            }

            abstract int findPartition(Text key);

            abstract void print(PrintStream strm) throws IOException;

            int getLevel() {
                return level;
            }
        }

        /**
         * An inner trie node that contains 256 children based on the next
         * character.
         */
        static class InnerTrieNode extends TrieNode {
            private TrieNode[] child = new TrieNode[256];

            InnerTrieNode(int level) {
                super(level);
            }

            int findPartition(Text key) {
                int level = getLevel();
                if (key.getLength() <= level) {
                    return child[0].findPartition(key);
                }
                return child[key.getBytes()[level] & 0xff].findPartition(key);
            }

            void setChild(int idx, TrieNode child) {
                this.child[idx] = child;
            }

            void print(PrintStream strm) throws IOException {
                for (int ch = 0; ch < 256; ++ch) {
                    for (int i = 0; i < 2 * getLevel(); ++i) {
                        strm.print(' ');
                    }
                    strm.print(ch);
                    strm.println(" ->");
                    if (child[ch] != null) {
                        child[ch].print(strm);
                    }
                }
            }
        }

        /**
         * A leaf trie node that does string compares to figure out where the given
         * key belongs between lower..upper.
         */
        static class LeafTrieNode extends TrieNode {
            int lower;
            int upper;
            Text[] splitPoints;

            LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
                super(level);
                this.splitPoints = splitPoints;
                this.lower = lower;
                this.upper = upper;
            }

            int findPartition(Text key) {
                for (int i = lower; i < upper; ++i) {
                    if (splitPoints[i].compareTo(key) > 0) {
                        return i;
                    }
                }
                return upper;
            }

            void print(PrintStream strm) throws IOException {
                for (int i = 0; i < 2 * getLevel(); ++i) {
                    strm.print(' ');
                }
                strm.print(lower);
                strm.print(", ");
                strm.println(upper);
            }
        }

        /**
         * Given a sorted set of cut points, build a trie that will find the correct
         * partition quickly.
         *
         * @param splits   the list of cut points
         * @param lower    the lower bound of partitions 0..numPartitions-1
         * @param upper    the upper bound of partitions 0..numPartitions-1
         * @param prefix   the prefix that we have already checked against
         * @param maxDepth the maximum depth we will build a trie for
         * @return the trie node that will divide the splits correctly
         */
        private static TrieNode buildTrie(Text[] splits, int lower, int upper,
                                          Text prefix, int maxDepth) {
            int depth = prefix.getLength();
            if (depth >= maxDepth || lower == upper) {
                return new LeafTrieNode(depth, splits, lower, upper);
            }
            InnerTrieNode result = new InnerTrieNode(depth);
            Text trial = new Text(prefix);
            // append an extra byte on to the prefix
            trial.append(new byte[1], 0, 1);
            int currentBound = lower;
            for (int ch = 0; ch < 255; ++ch) {
                trial.getBytes()[depth] = (byte) (ch + 1);
                lower = currentBound;
                while (currentBound < upper) {
                    if (splits[currentBound].compareTo(trial) >= 0) {
                        break;
                    }
                    currentBound += 1;
                }
                trial.getBytes()[depth] = (byte) ch;
                result.child[ch] = buildTrie(splits, lower, currentBound, trial,
                        maxDepth);
            }
            // pick up the rest
            trial.getBytes()[depth] = (byte) 255;
            result.child[255] = buildTrie(splits, currentBound, upper, trial,
                    maxDepth);
            return result;
        }

        public int getPartition(Text key, Text value, int numPartitions) {
            return trie.findPartition(key);
        }

        private static Text[] readPartitions(FileSystem fs, Path p, Configuration conf) throws IOException {
            int reduces = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
            Text[] result = new Text[reduces - 1];
            DataInputStream reader = fs.open(p);
            for (int i = 0; i < reduces - 1; ++i) {
                result[i] = new Text();
                result[i].readFields(reader);
            }
            reader.close();
            System.out.println(result.length);
            return result;
        }

        public void setConf(Configuration conf, Path partFile) {
            try {
                FileSystem fs = FileSystem.getLocal(conf);
                System.out.println(partFile);
                splitPoints = readPartitions(fs, partFile, conf);
                trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
            } catch (IOException ie) {
                throw new IllegalArgumentException("can't read partitions file", ie);
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        if (args.length != 3) {
            System.err.printf("%s usage: <filename> <partition size> <span size>\n", RecordDistributionWatcher.class.getSimpleName());
            System.exit(-1);
        }
        String filename = args[0];
        int partition = Integer.valueOf(args[1]);
        int recordSize = Integer.valueOf(args[2]);
        BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));

        String line;
        int[] partBlock = new int[partition];
        long count = 0;
        long failed = 0;
        Job job = Job.getInstance();
        Path partitionFile = new Path("file:////Users/valder/Projects/PhD/PhDProj/kvcomm/zero/partition.lst");

        job.setNumReduceTasks(partition);
        TeraInputFormat.setInputPaths(job, filename);
        TeraInputFormat.writePartitionFile(job, partitionFile);
        TotalOrderPartitioner partitioner = new TotalOrderPartitioner();
        partitioner.setConf(job.getConfiguration(), partitionFile);
        while ((line = reader.readLine()) != null) {
            String[] subS = line.split("\\s+");
            int part;
            try {
                part = partitioner.getPartition(new Text(subS[0]), null, partition);
                partBlock[part]++;

                if ((++count) % recordSize == 0) {
                    System.out.println(Arrays.toString(Arrays.copyOfRange(partBlock, 0, 20)));
                    Arrays.fill(partBlock, 0);
                }
            } catch (Exception e) {
                failed++;
            }
        }
        System.out.println("failed: " + failed);
    }
}
