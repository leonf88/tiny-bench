/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bench.mergest.type.hadoop.serializer;


import org.bench.mergest.type.hadoop.Writable;
import org.bench.mergest.type.hadoop.conf.Configuration;
import org.bench.mergest.type.hadoop.conf.Configured;
import org.bench.mergest.type.util.ReflectionUtils;

import java.io.*;

/**
 * A {@link Serialization} for {@link Writable}s that delegates to
 * {@link Writable#write(DataOutput)} and
 * {@link Writable#readFields(DataInput)}.
 */
public class WritableSerialization extends Configured
        implements Serialization<Writable> {
    public static class WritableDeserializer extends Configured
            implements Deserializer<Writable> {

        private Class<?> writableClass;
        private DataInputStream dataIn;

        public WritableDeserializer(Configuration conf, Class<?> c) {
            setConf(conf);
            this.writableClass = c;
        }

        @Override
        public void open(InputStream in) {
            if (in instanceof DataInputStream) {
                dataIn = (DataInputStream) in;
            } else {
                dataIn = new DataInputStream(in);
            }
        }

        @Override
        public Writable deserialize(Writable w) throws IOException {
            Writable writable;
            if (w == null) {
                writable = (Writable) ReflectionUtils.newInstance(writableClass, getConf());
            } else {
                writable = w;
            }
            writable.readFields(dataIn);
            return writable;
        }

        @Override
        public void close() throws IOException {
            dataIn.close();
        }

    }

    public static class WritableSerializer extends Configured implements
            Serializer<Writable> {

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
        public void serialize(Writable w) throws IOException {
            w.write(dataOut);
        }

        @Override
        public void close() throws IOException {
            dataOut.close();
        }

    }

    @Override
    public boolean accept(Class<?> c) {
        return Writable.class.isAssignableFrom(c);
    }

    @Override
    public Serializer<Writable> getSerializer(Class<Writable> c) {
        return new WritableSerializer();
    }

    @Override
    public Deserializer<Writable> getDeserializer(Class<Writable> c) {
        return new WritableDeserializer(getConf(), c);
    }

}
