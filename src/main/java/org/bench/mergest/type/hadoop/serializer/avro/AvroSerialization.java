/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bench.mergest.type.hadoop.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.bench.mergest.type.hadoop.conf.Configured;
import org.bench.mergest.type.hadoop.serializer.Deserializer;
import org.bench.mergest.type.hadoop.serializer.Serialization;
import org.bench.mergest.type.hadoop.serializer.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Base class for providing serialization to Avro types.
 */
public abstract class AvroSerialization<T> extends Configured
	implements Serialization<T> {
  
  public static final String AVRO_SCHEMA_KEY = "Avro-Schema";

  @Override
  public Deserializer<T> getDeserializer(Class<T> c) {
    return new AvroDeserializer(c);
  }

  @Override
  public Serializer<T> getSerializer(Class<T> c) {
    return new AvroSerializer(c);
  }

  /**
   * Return an Avro Schema instance for the given class.
   */
  public abstract Schema getSchema(T t);

  /**
   * Create and return Avro DatumWriter for the given class.
   */
  public abstract DatumWriter<T> getWriter(Class<T> clazz);

  /**
   * Create and return Avro DatumReader for the given class.
   */
  public abstract DatumReader<T> getReader(Class<T> clazz);

  class AvroSerializer implements Serializer<T> {

    private DatumWriter<T> writer;
    private BinaryEncoder encoder;
    private OutputStream outStream;

    AvroSerializer(Class<T> clazz) {
      this.writer = getWriter(clazz);
    }

    @Override
    public void close() throws IOException {
      encoder.flush();
      outStream.close();
    }

    @Override
    public void open(OutputStream out) throws IOException {
      outStream = out;
      encoder = EncoderFactory.get().binaryEncoder(out, encoder);
    }

    @Override
    public void serialize(T t) throws IOException {
      writer.setSchema(getSchema(t));
      writer.write(t, encoder);
    }

  }

  class AvroDeserializer implements Deserializer<T> {

    private DatumReader<T> reader;
    private BinaryDecoder decoder;
    private InputStream inStream;

    AvroDeserializer(Class<T> clazz) {
      this.reader = getReader(clazz);
    }

    @Override
    public void close() throws IOException {
      inStream.close();
    }

    @Override
    public T deserialize(T t) throws IOException {
      return reader.read(t, decoder);
    }

    @Override
    public void open(InputStream in) throws IOException {
      inStream = in;
      decoder = DecoderFactory.get().binaryDecoder(in, decoder);
    }

  }

}
