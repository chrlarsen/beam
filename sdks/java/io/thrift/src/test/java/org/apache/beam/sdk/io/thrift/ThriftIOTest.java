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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.thrift.parser.model.BaseType;
import org.apache.beam.sdk.io.thrift.parser.model.ConstInteger;
import org.apache.beam.sdk.io.thrift.parser.model.Document;
import org.apache.beam.sdk.io.thrift.parser.model.Header;
import org.apache.beam.sdk.io.thrift.parser.model.IdentifierType;
import org.apache.beam.sdk.io.thrift.parser.model.IntegerEnumField;
import org.apache.beam.sdk.io.thrift.parser.model.ListType;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftField;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftMethod;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftType;
import org.apache.beam.sdk.io.thrift.parser.model.TypeAnnotation;
import org.apache.beam.sdk.io.thrift.parser.model.VoidType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
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
      Resources.getResource(RESOURCE_DIR + "simple_test.thrift").getPath();
  private static final String ALL_THRIFT_STRING =
      Resources.getResource(RESOURCE_DIR).getPath() + "*";
  private static List<Document> goodDocuments;
  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    Map<String, String> namespaces = new HashMap<>();
    namespaces.put("cpp", "tutorial");
    namespaces.put("d", "tutorial");
    namespaces.put("java", "tutorial");
    namespaces.put("php", "tutorial");
    namespaces.put("perl", "tutorial");

    List<TypeAnnotation> emptyAnnotations = new ArrayList<>();
    List<ThriftField> emptyArgs = new ArrayList<>();
    List<ThriftField> emptyThrows = new ArrayList<>();
    List<String> emptyStringList = new ArrayList<>();

    Header testHeader =
        new Header(Collections.singletonList("shared.thrift"), new ArrayList<>(), null, namespaces);

    ThriftType stringType = new BaseType(BaseType.Type.STRING, emptyAnnotations);
    ThriftType i32 = new BaseType(BaseType.Type.I32, emptyAnnotations);

    Map<Object, Object> objectMap = new LinkedHashMap<>();
    objectMap.put("hello", "world");
    objectMap.put("goodnight", "moon");
    Map<Object, Object> testMap = ImmutableMap.copyOf(objectMap);

    Document simpleTestDocument = Document.emptyDocument();
    simpleTestDocument.setHeader(testHeader);

    simpleTestDocument.addTypedef(
        "MyInteger", new BaseType(BaseType.Type.I32, emptyAnnotations), emptyAnnotations);

    simpleTestDocument.addConstInteger("INT32CONSTANT", BaseType.Type.I32, emptyAnnotations, 9853L);

    simpleTestDocument.addConstMap(
        "MAPCONSTANT", BaseType.Type.STRING, BaseType.Type.STRING, null, emptyAnnotations, testMap);

    List<IntegerEnumField> enumFields = new ArrayList<>();
    enumFields.add(new IntegerEnumField("ADD", 1L, 1L, emptyAnnotations));
    enumFields.add(new IntegerEnumField("SUBTRACT", 2L, 2L, emptyAnnotations));
    enumFields.add(new IntegerEnumField("MULTIPLY", 3L, 3L, emptyAnnotations));
    enumFields.add(new IntegerEnumField("DIVIDE", 4L, 4L, emptyAnnotations));
    simpleTestDocument.addEnum("Operation", enumFields, emptyAnnotations);

    List<ThriftField> structFields = new ArrayList<>();
    structFields.add(
        new ThriftField(
            "num1",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            new ConstInteger(0L),
            emptyAnnotations));
    structFields.add(
        new ThriftField(
            "num2",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    structFields.add(
        new ThriftField(
            "op",
            new IdentifierType("Operation"),
            3L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    structFields.add(
        new ThriftField(
            "comment",
            new BaseType(BaseType.Type.STRING, emptyAnnotations),
            4L,
            ThriftField.Requiredness.OPTIONAL,
            null,
            emptyAnnotations));
    simpleTestDocument.addStruct("Work", structFields, emptyAnnotations);

    List<ThriftField> exceptionStructFields = new ArrayList<>();
    exceptionStructFields.add(
        new ThriftField(
            "whatOp",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    exceptionStructFields.add(
        new ThriftField(
            "why",
            new BaseType(BaseType.Type.STRING, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    simpleTestDocument.addThriftException(
        "InvalidOperation", exceptionStructFields, emptyAnnotations);

    List<ThriftField> addArgs = new ArrayList<>();
    addArgs.add(
        new ThriftField("num1", i32, 1L, ThriftField.Requiredness.NONE, null, emptyAnnotations));
    addArgs.add(
        new ThriftField("num2", i32, 2L, ThriftField.Requiredness.NONE, null, emptyAnnotations));

    List<ThriftField> calculateArgs = new ArrayList<>();
    calculateArgs.add(
        new ThriftField("logid", i32, 1L, ThriftField.Requiredness.NONE, null, emptyAnnotations));
    calculateArgs.add(
        new ThriftField(
            "w",
            new IdentifierType("Work"),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));

    List<ThriftField> calculateThrows = new ArrayList<>();
    calculateThrows.add(
        new ThriftField(
            "ouch",
            new IdentifierType("InvalidOperation"),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));

    List<ThriftMethod> serviceMethods = new ArrayList<>();
    serviceMethods.add(
        new ThriftMethod("ping", new VoidType(), emptyArgs, false, emptyThrows, emptyAnnotations));
    serviceMethods.add(new ThriftMethod("add", i32, addArgs, false, emptyThrows, emptyAnnotations));
    serviceMethods.add(
        new ThriftMethod(
            "calculate", i32, calculateArgs, false, calculateThrows, emptyAnnotations));
    serviceMethods.add(
        new ThriftMethod("zip", new VoidType(), emptyArgs, true, emptyThrows, emptyAnnotations));
    simpleTestDocument.addService(
        "Calculator", "shared.SharedService", serviceMethods, emptyAnnotations);

    /////////////////////////////////////////////////////////////

    Document sharedTestDocument = Document.emptyDocument();
    Map<String, String> stNamespaces = new HashMap<>();
    stNamespaces.put("cpp", "shared");
    stNamespaces.put("d", "share");
    stNamespaces.put("java", "shared");
    stNamespaces.put("perl", "shared");
    stNamespaces.put("php", "shared");
    sharedTestDocument.setHeader(new Header(emptyStringList, emptyStringList, null, stNamespaces));

    List<ThriftField> sharedStructFields = new ArrayList<>();
    sharedStructFields.add(
        new ThriftField(
            "key",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    sharedStructFields.add(
        new ThriftField(
            "value",
            new BaseType(BaseType.Type.STRING, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    sharedTestDocument.addStruct("SharedStruct", sharedStructFields, emptyAnnotations);

    List<ThriftMethod> sharedServiceMethods = new ArrayList<>();
    sharedServiceMethods.add(
        new ThriftMethod(
            "getStruct",
            new IdentifierType("SharedStruct"),
            emptyArgs,
            false,
            emptyThrows,
            emptyAnnotations));
    sharedTestDocument.addService("SharedService", null, sharedServiceMethods, emptyAnnotations);

    //////////////////////////////////////////////////////////////
    // Create annotation test document
    Document annotationTestDocument = Document.emptyDocument();

    annotationTestDocument.addIncludes("simple_test.thrift");

    List<TypeAnnotation> annotationTestList = new ArrayList<>();
    annotationTestList.add(new TypeAnnotation("cpp.template", "std::list"));
    annotationTestList.add(new TypeAnnotation("presence", "required"));
    annotationTestList.add(new TypeAnnotation("presence", "manual"));
    annotationTestList.add(new TypeAnnotation("cpp.use_pointer", ""));
    annotationTestList.add(new TypeAnnotation("cpp.type", "DenseFoo"));
    annotationTestList.add(new TypeAnnotation("python.type", "DenseFoo"));
    annotationTestList.add(new TypeAnnotation("java.final", ""));
    annotationTestList.add(new TypeAnnotation("foo", "bar")); // index = 7
    annotationTestList.add(new TypeAnnotation("unicode.encoding", "UTF-16"));
    annotationTestList.add(new TypeAnnotation("cpp.fixed_point", "16")); // index = 9
    annotationTestList.add(new TypeAnnotation("weekend", "yes"));
    annotationTestList.add(new TypeAnnotation("foo.bar", "baz"));
    annotationTestList.add(new TypeAnnotation("a.b", "c")); // index = 12

    ListType annotationListType =
        new ListType(
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            null,
            Arrays.asList(annotationTestList.get(0)));

    annotationTestDocument.addTypedef("int_linked_list", annotationListType, emptyAnnotations);

    List<ThriftField> fooStructFields = new ArrayList<>();
    fooStructFields.add(
        new ThriftField(
            "bar",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            Arrays.asList(annotationTestList.get(1))));
    List<TypeAnnotation> fooStructBazAnnotations =
        new ArrayList<>(annotationTestList.subList(2, 4));
    fooStructFields.add(
        new ThriftField(
            "baz",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            fooStructBazAnnotations));
    fooStructFields.add(
        new ThriftField(
            "qux",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            3L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    fooStructFields.add(
        new ThriftField(
            "bop",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            4L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));
    List<TypeAnnotation> fooStructAnnotations = new ArrayList<>(annotationTestList.subList(4, 7));
    annotationTestDocument.addStruct("foo", fooStructFields, fooStructAnnotations);

    List<ThriftField> annotationExceptionStructFields = new ArrayList<>();
    annotationExceptionStructFields.add(
        new ThriftField(
            "error_code",
            new BaseType(BaseType.Type.I32, emptyAnnotations),
            1L,
            ThriftField.Requiredness.NONE,
            null,
            Arrays.asList(annotationTestList.get(7))));
    annotationExceptionStructFields.add(
        new ThriftField(
            "error_msg",
            new BaseType(BaseType.Type.STRING, emptyAnnotations),
            2L,
            ThriftField.Requiredness.NONE,
            null,
            emptyAnnotations));

    annotationTestDocument.addThriftException(
        "foo_error", annotationExceptionStructFields, Arrays.asList(annotationTestList.get(7)));

    annotationTestDocument.addTypedef(
        "non_latin_string",
        new BaseType(BaseType.Type.STRING, Arrays.asList(annotationTestList.get(8))),
        Arrays.asList(annotationTestList.get(7)));

    ListType tinyFloatListType =
        new ListType(
            new BaseType(BaseType.Type.DOUBLE, Arrays.asList(annotationTestList.get(9))),
            null,
            emptyAnnotations);
    annotationTestDocument.addTypedef("tiny_float_list", tinyFloatListType, emptyAnnotations);

    List<IntegerEnumField> annotationEnumFields = new ArrayList<>();
    annotationEnumFields.add(
        new IntegerEnumField("SUNDAY", null, 0L, Arrays.asList(annotationTestList.get(10))));
    annotationEnumFields.add(new IntegerEnumField("MONDAY", null, 1L, null));
    annotationEnumFields.add(new IntegerEnumField("TUESDAY", null, 2L, null));
    annotationEnumFields.add(new IntegerEnumField("WEDNESDAY", null, 3L, null));
    annotationEnumFields.add(new IntegerEnumField("THURSDAY", null, 4L, null));
    annotationEnumFields.add(new IntegerEnumField("FRIDAY", null, 5L, null));
    annotationEnumFields.add(
        new IntegerEnumField("SATURDAY", null, 6L, Arrays.asList(annotationTestList.get(10))));
    annotationTestDocument.addEnum(
        "weekdays", annotationEnumFields, Arrays.asList(annotationTestList.get(11)));

    List<String> annotationSenumFields = new ArrayList<>();
    annotationSenumFields.add("Spring");
    annotationSenumFields.add("Summer");
    annotationSenumFields.add("Fall");
    annotationSenumFields.add("Winter");
    annotationTestDocument.addSenum("seasons", annotationSenumFields);

    List<ThriftMethod> annotationServiceMethods = new ArrayList<>();
    annotationServiceMethods.add(
        new ThriftMethod(
            "foo",
            new VoidType(),
            emptyArgs,
            false,
            emptyThrows,
            Arrays.asList(annotationTestList.get(7))));
    annotationTestDocument.addService(
        "foo_service", null, annotationServiceMethods, Arrays.asList(annotationTestList.get(12)));

    goodDocuments =
        ImmutableList.of(simpleTestDocument, annotationTestDocument, sharedTestDocument);
  }

  /** Tests {@link ThriftIO#readFiles()}. */
  @Test
  public void testReadFiles() {

    PCollection<Document> testThriftDoc =
        mainPipeline
            .apply(Create.of(ALL_THRIFT_STRING).withCoder(StringUtf8Coder.of()))
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(ThriftIO.readFiles());

    // Assert
    PAssert.that(testThriftDoc).containsInAnyOrder(goodDocuments);

    // Execute pipeline
    mainPipeline.run();
  }

  /** Tests {@link ThriftIO#read()}. */
  @Test
  public void testRead() {

    PCollection<Document> testReadDocs =
        mainPipeline.apply(ThriftIO.read().from(ALL_THRIFT_STRING));

    // Assert
    PAssert.that(testReadDocs).containsInAnyOrder(goodDocuments);

    mainPipeline.run();
  }

  /** Tests {@link ThriftIO#write} which calls {@link ThriftIO#sink()}. */
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
  }
}
