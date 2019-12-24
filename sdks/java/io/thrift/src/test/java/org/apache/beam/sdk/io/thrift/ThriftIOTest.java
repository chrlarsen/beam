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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomUtils;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
  private static final TestThriftStruct TEST_THRIFT_STRUCT = new TestThriftStruct();
  private static List<TestThriftStruct> testThriftStructs;
  private final TProtocolFactory tBinaryProtoFactory = new TBinaryProtocol.Factory();
  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    byte[] bytes = new byte[10];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    TEST_THRIFT_STRUCT.testByte = 100;
    TEST_THRIFT_STRUCT.testShort = 200;
    TEST_THRIFT_STRUCT.testInt = 2500;
    TEST_THRIFT_STRUCT.testLong = 79303L;
    TEST_THRIFT_STRUCT.testDouble = 25.007;
    TEST_THRIFT_STRUCT.testBool = true;
    TEST_THRIFT_STRUCT.stringIntMap = new HashMap<>();
    TEST_THRIFT_STRUCT.stringIntMap.put("first", (short) 1);
    TEST_THRIFT_STRUCT.stringIntMap.put("second", (short) 2);
    TEST_THRIFT_STRUCT.testBinary = buffer;

    testThriftStructs = ImmutableList.copyOf(generateTestObjects(2));
  }

  /** Tests {@link ThriftIO#readFiles(Class)} with {@link TBinaryProtocol}. */
  @Test
  public void testReadFilesBinaryProtocol() {

    PCollection<TestThriftStruct> testThriftDoc =
        mainPipeline
            .apply(Create.of(ALL_THRIFT_STRING).withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tBinaryProtoFactory));

    // Assert
    PAssert.that(testThriftDoc).containsInAnyOrder(TEST_THRIFT_STRUCT);

    // Execute pipeline
    mainPipeline.run();
  }

  /** Tests {@link ThriftIO#sink(TProtocolFactory)} with {@link TBinaryProtocol}. */
  @Test
  public void testReadWriteBinaryProtocol() {

    mainPipeline
        .apply(Create.of(testThriftStructs).withCoder(ThriftCoder.of()))
        .apply(
            FileIO.<TestThriftStruct>write()
                .via(ThriftIO.sink(tBinaryProtoFactory))
                .to(temporaryFolder.getRoot().getAbsolutePath()));

    // Execute write pipeline
    mainPipeline.run().waitUntilFinish();

    // Read written files
    PCollection<TestThriftStruct> readDocs =
        readPipeline
            .apply(
                Create.of(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                    .withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tBinaryProtoFactory));

    // Assert
    PAssert.that(readDocs).containsInAnyOrder(testThriftStructs);

    // Execute read pipeline
    readPipeline.run();
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} throws {@link IllegalArgumentException} when no
   * {@link TProtocolFactory} is supplied.
   */
  /*@Test
  public void testSinkNullProto() throws Exception {

    writePipeline
            .apply(Create.of(TEST_THRIFT_STRUCT).withCoder(ThriftCoder.of()))
            .apply(FileIO.<TestThriftStruct>write().via(ThriftIO.sink(null)).to(RESOURCE_DIR));

    expectedException.expect(IllegalArgumentException.class);

    // Execute write pipeline
    writePipeline.run();
  }*/

  private List<TestThriftStruct> generateTestObjects(long count) {
    List<TestThriftStruct> testThriftStructList = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      TestThriftStruct temp = new TestThriftStruct();
      byte[] bytes = RandomUtils.nextBytes(10);
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      // Generate random string
      String randomString = RandomStringUtils.random(10, true, false);
      short s = (short) RandomUtils.nextInt(0, Short.MAX_VALUE + 1);
      temp.stringIntMap = new HashMap<>();
      temp.stringIntMap.put(randomString, s);
      temp.testShort = s;
      temp.testBinary = buffer;
      temp.testBool = RandomUtils.nextBoolean();
      temp.testByte = (byte) RandomUtils.nextInt(0, Byte.MAX_VALUE + 1);
      temp.testDouble = RandomUtils.nextDouble();
      temp.testInt = RandomUtils.nextInt();
      temp.testLong = RandomUtils.nextLong();

      testThriftStructList.add(temp);
    }

    return testThriftStructList;
  }
}
