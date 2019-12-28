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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
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

  private static final String THRIFT_DIR = Resources.getResource(RESOURCE_DIR).getPath();
  private static final String ALL_THRIFT_STRING =
      Resources.getResource(RESOURCE_DIR).getPath() + "*";
  private static final TestThriftStruct TEST_THRIFT_STRUCT = new TestThriftStruct();
  private static List<TestThriftStruct> testThriftStructs;
  private final TProtocolFactory tBinaryProtoFactory = new TBinaryProtocol.Factory();
  private final TProtocolFactory tJsonProtocolFactory = new TJSONProtocol.Factory();
  private final TProtocolFactory tSimpleJsonProtocolFactory = new TSimpleJSONProtocol.Factory();
  private final TProtocolFactory tCompactProtocolFactory = new TCompactProtocol.Factory();
  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

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

    testThriftStructs = ImmutableList.copyOf(generateTestObjects(1000L));
  }

  /** Tests {@link ThriftIO#readFiles(Class)} with {@link TBinaryProtocol}. */
  @Test
  public void testReadFilesBinaryProtocol() {

    PCollection<TestThriftStruct> testThriftDoc =
        mainPipeline
            .apply(Create.of(THRIFT_DIR + "data").withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tBinaryProtoFactory));

    // Assert
    PAssert.that(testThriftDoc).containsInAnyOrder(TEST_THRIFT_STRUCT);

    // Execute pipeline
    mainPipeline.run();
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} and {@link ThriftIO#readFiles(Class)} with {@link
   * TBinaryProtocol}.
   */
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
    readPipeline.run().waitUntilFinish();
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} and {@link ThriftIO#readFiles(Class)} with {@link
   * TJSONProtocol}.
   */
  @Test
  public void testReadWriteJsonProtocol() {

    mainPipeline
        .apply(Create.of(testThriftStructs).withCoder(ThriftCoder.of()))
        .apply(
            FileIO.<TestThriftStruct>write()
                .via(ThriftIO.sink(tJsonProtocolFactory))
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
            .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tJsonProtocolFactory));

    // Assert
    PAssert.that(readDocs).containsInAnyOrder(testThriftStructs);

    // Execute read pipeline
    readPipeline.run().waitUntilFinish();
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} with {@link
   * TSimpleJSONProtocol}.
   */
  @Test
  public void testReadWriteSimpleJsonProtocol() throws IOException {

    mainPipeline
            .apply(Create.of(TEST_THRIFT_STRUCT).withCoder(ThriftCoder.of()))
            .apply(
                    FileIO.<TestThriftStruct>write()
                            .via(ThriftIO.sink(tSimpleJsonProtocolFactory))
                            .to(temporaryFolder.getRoot().getAbsolutePath()));

    // Execute write pipeline
    mainPipeline.run().waitUntilFinish();

    Gson gson = new Gson();
    File dir = new File(temporaryFolder.getRoot().getAbsolutePath());
    Collection<File> files = FileUtils.listFiles(dir, new WildcardFileFilter("output-*"), null);
    JsonReader jsonReader =
            new JsonReader(Files.newBufferedReader(files.iterator().next().toPath(), Charset.defaultCharset()));
    TestThriftStruct writtenStruct = gson.fromJson(jsonReader, TestThriftStruct.class);

    // Assert
    assertThat(writtenStruct, equalTo(TEST_THRIFT_STRUCT));
  }

  /**
   * Tests {@link ThriftIO#sink(TProtocolFactory)} and {@link ThriftIO#readFiles(Class)} with {@link
   * TCompactProtocol}.
   */
  @Test
  public void testReadWriteCompactProtocol() {

    mainPipeline
            .apply(Create.of(testThriftStructs).withCoder(ThriftCoder.of()))
            .apply(
                    FileIO.<TestThriftStruct>write()
                            .via(ThriftIO.sink(tCompactProtocolFactory))
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
                    .apply(ThriftIO.readFiles(TestThriftStruct.class).withProtocol(tCompactProtocolFactory));

    // Assert
    PAssert.that(readDocs).containsInAnyOrder(testThriftStructs);

    // Execute read pipeline
    readPipeline.run().waitUntilFinish();
  }

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
