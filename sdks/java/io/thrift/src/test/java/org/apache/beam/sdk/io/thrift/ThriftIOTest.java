/*
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
package org.apache.beam.sdk.io.thrift;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ThriftIO}. */
@RunWith(JUnit4.class)
public class ThriftIOTest implements Serializable {

  private static final String RESOURCE_DIR = "ThriftIOTest/";

  private static final String THRIFT_STRING =
      Resources.getResource(RESOURCE_DIR + "data").getPath();
  private static final String ALL_THRIFT_STRING =
      Resources.getResource(RESOURCE_DIR).getPath() + "*";
  private final TestThriftStruct tts = new TestThriftStruct();
  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    byte[] bytes = new byte[10];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    tts.testByte = 100;
    tts.testShort = 200;
    tts.testInt = 2500;
    tts.testLong = 79303L;
    tts.testDouble = 25.007;
    tts.testBool = true;
    tts.stringIntMap = new HashMap<>();
    tts.stringIntMap.put("first", (short) 1);
    tts.stringIntMap.put("second", (short) 2);
    tts.testBinary = buffer;
  }

  /** Tests {@link ThriftIO#readFiles(Class)}. */
  @Test
  public void testReadFiles() {

    TProtocolFactory proto = new TBinaryProtocol.Factory();

    PCollection<TestThriftStruct> testThriftDoc =
        mainPipeline
            .apply(Create.of(ALL_THRIFT_STRING).withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(proto));

    // Assert
    PAssert.that(testThriftDoc).containsInAnyOrder(tts);

    // Execute pipeline
    mainPipeline.run();
  }

  /** Tests {@link ThriftIO#write} which calls {@link ThriftIO#sink()}. */
  /*
  @Test
  public void testReadWrite() {
    writePipeline
        .apply(Create.of(goodDocuments).withCoder(ThriftCoder.of()))
        .apply(ThriftIO.write().to(temporaryFolder.getRoot().getAbsolutePath()));

    // Execute write pipeline
    writePipeline.run().waitUntilFinish();

    // Read written files
    PCollection<Document> readDocs =
        readPipeline.apply(
            ThriftIO.read().from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    // Assert
    PAssert.that(readDocs).containsInAnyOrder(goodDocuments);

    // Execute read pipeline
    readPipeline.run();
  }*/
}
