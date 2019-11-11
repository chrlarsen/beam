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

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.thrift.parser.ThriftIdlParser;
import org.apache.beam.sdk.io.thrift.parser.model.BaseType;
import org.apache.beam.sdk.io.thrift.parser.model.Const;
import org.apache.beam.sdk.io.thrift.parser.model.Definition;
import org.apache.beam.sdk.io.thrift.parser.model.Document;
import org.apache.beam.sdk.io.thrift.parser.model.Header;
import org.apache.beam.sdk.io.thrift.parser.model.IdentifierType;
import org.apache.beam.sdk.io.thrift.parser.model.IntegerEnum;
import org.apache.beam.sdk.io.thrift.parser.model.IntegerEnumField;
import org.apache.beam.sdk.io.thrift.parser.model.ListType;
import org.apache.beam.sdk.io.thrift.parser.model.MapType;
import org.apache.beam.sdk.io.thrift.parser.model.Service;
import org.apache.beam.sdk.io.thrift.parser.model.StringEnum;
import org.apache.beam.sdk.io.thrift.parser.model.Struct;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftException;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftField;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftMethod;
import org.apache.beam.sdk.io.thrift.parser.model.ThriftType;
import org.apache.beam.sdk.io.thrift.parser.model.TypeAnnotation;
import org.apache.beam.sdk.io.thrift.parser.model.Typedef;
import org.apache.beam.sdk.io.thrift.parser.model.VoidType;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading and writing Thrift files.
 *
 * <h3>Reading Thrift Files</h3>
 *
 * <p>For simple reading, use {@link ThriftIO#read} with the desired file pattern to read from.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<Document> documents = pipeline.apply(ThriftIO.read().from("/foo/bar/*"));
 * ...
 * }</pre>
 *
 * <p>For more advanced use cases, like reading each file in a {@link PCollection} of {@link
 * FileIO.ReadableFile}, use the {@link ReadFiles} transform.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<FileIO.ReadableFile> files = pipeline
 *   .apply(FileIO.match().filepattern(options.getInputFilepattern())
 *   .apply(FileIO.readMatches());
 *
 * PCollection<Document> documents = files.apply(ThriftIO.readFiles());
 * }</pre>
 *
 * <h3>Writing Thrift Files</h3>
 *
 * <p>{@link ThriftIO.Sink} allows for a {@link PCollection} of {@link Document} to be written to
 * Thrift files. It can be used with the general-purpose {@link FileIO} transforms with
 * FileIO.write/writeDynamic specifically.
 *
 * <p>By default, {@link ThriftIO.Sink} can write multiple {@link Document} to a file so it is
 * highly recommended to provide a unique name for each desired file.
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // PCollection<Document>
 *   .apply(FileIO
 *     .<Document>write()
 *     .via(ThriftIO.sink()
 *     .to("destination/path")
 *     .withPrefix("unique_name")
 *     .withSuffix(".thrift"));
 * }</pre>
 *
 * <p>This IO API is considered experimental and may break or receive backwards-incompatible changes
 * in future versions of the Apache Beam SDK.
 *
 * <p>NOTE: At this time retention of comments are not supported.
 */
public class ThriftIO {

  private static final String DEFAULT_THRIFT_SUFFIX = ".thrift";

  private static final Logger LOG = LoggerFactory.getLogger(ThriftIO.class);

  /** Disable construction of utility class. */
  private ThriftIO() {}

  /**
   * A {@link PTransform} that reads one or more Thrift files matching a pattern and returns a
   * {@link PCollection} of {@link Document}.
   */
  public static Read read() {
    return new AutoValue_ThriftIO_Read.Builder().setCompression(Compression.AUTO).build();
  }

  /**
   * Like {@link #read()},but reads each file in a {@link PCollection} of {@link
   * org.apache.beam.sdk.io.FileIO.ReadableFile}, which allows more flexible usage.
   */
  public static ReadFiles readFiles() {
    return new AutoValue_ThriftIO_ReadFiles.Builder().build();
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #read()}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Document>> {

    @Nullable
    abstract ValueProvider<String> getFilePattern();

    abstract Compression getCompression();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilePattern(ValueProvider<String> filePattern);

      abstract Builder setCompression(Compression compression);

      abstract Read build();
    }

    /**
     * Returns a transform for reading Thrift files that reads from the file(s) with the given
     * filename or filename pattern. This can be a local path (if running locally), or a Google
     * Cloud Storage filename or filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if
     * running locally or using remote execution). Standard <a
     * href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java Filesystem glob
     * patterns</a> ("*", "?", "[..]") are supported.
     */
    public Read from(String filePattern) {
      return from(StaticValueProvider.of(filePattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      return toBuilder().setFilePattern(filepattern).build();
    }

    /**
     * Returns a transform for reading Thrift files that decompresses all input files using the
     * specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link Compression#AUTO}. In this
     * mode, the compression type of the file is determined by it's extension via {@link
     * Compression#detect(String)}.
     */
    public Read withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    public PCollection<Document> expand(PBegin input) {

      return input
          .apply("Create filepattern", Create.ofProvider(getFilePattern(), StringUtf8Coder.of()))
          .apply(FileIO.matchAll())
          .apply(FileIO.readMatches())
          .apply(readFiles());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item("filePattern", getFilePattern()).withLabel("Input File Pattern"));
      builder.add(
          DisplayData.item("compressionType", getCompression().toString())
              .withLabel("Compression Type"));
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Creates a {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}. */
  public static Sink sink() {
    return new AutoValue_ThriftIO_Sink.Builder().build();
  }

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<Document>> {

    abstract Builder toBuilder();

    @Override
    public PCollection<Document> expand(PCollection<FileIO.ReadableFile> input) {
      return input.apply(ParDo.of(new ReadFn()));
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract ReadFiles build();
    }

    /** Reads a {@link FileIO.ReadableFile} and outputs a Thrift {@link Document}. */
    static class ReadFn extends DoFn<FileIO.ReadableFile, Document> {

      @ProcessElement
      public void processElement(ProcessContext processContext) {
        FileIO.ReadableFile file = processContext.element();
        try {
          Document document =
              ThriftIdlParser.parseThriftIdl(
                  ByteSource.wrap(file.readFullyAsBytes()).asCharSource(Charsets.UTF_8));

          processContext.output(document);

        } catch (IOException ioe) {

          String filename = file.getMetadata().resourceId().toString();

          LOG.error(String.format("Error in reading file: %1$s%n%2$s", filename, ioe));

          throw new RuntimeException(ioe);
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link Write}. */
  public static Write write() {
    return new AutoValue_ThriftIO_Write.Builder().build();
  }

  /**
   * A {@link PTransform} for writing Thrift files.
   *
   * <p>Allows users to specify a prefix using {@link Write#withPrefix(String)}, a suffix using
   * {@link Write#withSuffix(String)} and a destination using {@link Write#to(String)}.
   *
   * <p>If no prefix is provided then {@link Document#hashCode()} will be used to avoid files from
   * colliding.
   *
   * <p>If no suffix is provided then ".thrift" will be used.
   *
   * <p>All methods also support {@link ValueProvider}.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Document>, PDone> {

    abstract Builder toBuilder();

    @Nullable
    abstract ValueProvider<String> getPrefix();

    @Nullable
    abstract ValueProvider<String> getSuffix();

    @Nullable
    abstract ValueProvider<String> getDestination();

    @Override
    public PDone expand(PCollection<Document> input) {
      checkNotNull(getDestination(), "Destination must not be null.");

      input.apply(
          FileIO.<Document, Document>writeDynamic()
              .by(Document::getDocument)
              .withNaming(
                  naming ->
                      FileIO.Write.defaultNaming(
                          (getPrefix() != null)
                              ? getPrefix().get()
                              : String.valueOf(naming.hashCode()),
                          (getSuffix() != null) ? getSuffix().get() : DEFAULT_THRIFT_SUFFIX))
              .via(ThriftIO.sink())
              .to(getDestination())
              .withDestinationCoder(ThriftCoder.of()));

      return PDone.in(input.getPipeline());
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setPrefix(@Nullable ValueProvider<String> prefix);

      abstract Builder setSuffix(@Nullable ValueProvider<String> suffix);

      abstract Builder setDestination(ValueProvider<String> destination);

      abstract Write build();
    }

    /** Returns a transform for writing Thrift files with the specified prefix. */
    public Write withPrefix(ValueProvider<String> prefix) {
      return toBuilder().setPrefix(prefix).build();
    }

    /** Like {@link Write#withPrefix(ValueProvider)} but allows for a string prefix. */
    public Write withPrefix(String prefix) {
      return withPrefix(StaticValueProvider.of(prefix));
    }

    /** Returns a transform for writing Thrift files with the specified suffix. */
    public Write withSuffix(ValueProvider<String> suffix) {
      return toBuilder().setSuffix(suffix).build();
    }

    /** Like {@link Write#withSuffix(ValueProvider)} but allows for a string suffix. */
    public Write withSuffix(String suffix) {
      return withSuffix(StaticValueProvider.of(suffix));
    }

    /**
     * Returns a transform for writing Thrift files that writes to the file(s) with the given
     * filename or filename pattern. This can be a local path (if running locally), or a Google
     * Cloud Storage filename or filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if
     * running locally or using remote execution). Standard <a
     * href="http://docs.oracle.com/javase/tutorial/essential/io/find.html">Java Filesystem
     * globpatterns</a> ("*", "?", "[..]") are supported.
     */
    public Write to(ValueProvider<String> destination) {
      return toBuilder().setDestination(destination).build();
    }

    /** Like {@link Write#to(ValueProvider)} but allows for a string destination. */
    public Write to(String destination) {
      return to(StaticValueProvider.of(destination));
    }
  }

  /** Implementation of {@link #sink}. */
  @AutoValue
  public abstract static class Sink implements FileIO.Sink<Document> {

    abstract Builder toBuilder();

    private transient ThriftWriter writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      this.writer = new ThriftWriter(Channels.newOutputStream(channel));
    }

    @Override
    public void write(Document element) throws IOException {
      checkNotNull(writer, "Writer cannot be null");
      writer.write(element);
    }

    @Override
    public void flush() throws IOException {
      writer.close();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Sink build();
    }
  }

  /** Class to write Thrift {@link Document}. */
  private static class ThriftWriter implements Closeable {

    private transient OutputStream channel;

    ThriftWriter(OutputStream channel) {
      this.channel = channel;
    }

    private void write(Document data) throws IOException {
      Header header = data.getHeader();
      writeIncludes(header.getIncludes());
      writeCppIncludes(header.getCppIncludes());
      writeNamespaces(header.getNamespaces());

      List<Definition> definitions = data.getDefinitions();
      for (Definition definition : definitions) {
        if (definition instanceof Typedef) {
          Typedef typedef = (Typedef) definition;
          String thriftTypeString = getThriftTypeString(typedef.getType());
          this.channel.write(
              String.format(
                      "typedef %1$s %2$s %3$s%n",
                      thriftTypeString,
                      typedef.getName(),
                      getAnnotationString(typedef.getAnnotations()))
                  .getBytes(Charset.defaultCharset()));
        }

        if (definition instanceof Const) {
          Const constant = (Const) definition;
          String thriftTypeString = getThriftTypeString(constant.getType());
          this.channel.write(
              String.format(
                      "const %1$s %2$s = %3$s%n",
                      thriftTypeString, constant.getName(), constant.getValue().getValueString())
                  .getBytes(Charset.defaultCharset()));
        }

        if (definition instanceof IntegerEnum) {
          IntegerEnum integerEnum = (IntegerEnum) definition;
          String integerEnumFields = getIntegerEnumFieldsString(integerEnum.getFields());
          String enumAnnotations = getAnnotationString(integerEnum.getAnnotations());
          this.channel.write(
              String.format(
                      "enum %1$s {%n%2$s%n}%3$s%n",
                      integerEnum.getName(), integerEnumFields, enumAnnotations)
                  .getBytes(Charset.defaultCharset()));
        }

        if (definition instanceof StringEnum) {
          StringEnum stringEnum = (StringEnum) definition;
          String stringEnumFields = getStringEnumFieldString(stringEnum.getValues());
          this.channel.write(
              String.format("senum %1$s {%n%2$s%n}%n", stringEnum.getName(), stringEnumFields)
                  .getBytes(Charset.defaultCharset()));
        }

        if (definition instanceof Struct) {
          Struct struct = (Struct) definition;
          String structFields = getStructFieldsString(struct.getFields(), ";\n", true);
          String structAnnotations = getAnnotationString(struct.getAnnotations());
          this.channel.write(
              String.format(
                      "struct %1$s {%n%2$s%n}%3$s%n",
                      struct.getName(), structFields, structAnnotations)
                  .getBytes(Charset.defaultCharset()));
        }

        if (definition instanceof ThriftException) {
          ThriftException thriftException = (ThriftException) definition;
          String exceptionFields = getStructFieldsString(thriftException.getFields(), ",\n", true);
          String exceptionAnnotations = getAnnotationString(thriftException.getAnnotations());
          this.channel.write(
              String.format(
                      "exception %1$s {%n%2$s%n}%3$s%n",
                      thriftException.getName(), exceptionFields, exceptionAnnotations)
                  .getBytes(Charset.defaultCharset()));
        }

        if (definition instanceof Service) {
          Service service = (Service) definition;
          String parent =
              service.getParent().isPresent() ? "extends " + service.getParent().get() : "";
          String methods = getThriftMethodString(service.getMethods());
          String serviceAnnotations = getAnnotationString(service.getAnnotations().get());
          this.channel.write(
              String.format(
                      "service %1$s %2$s {%n%3$s%n}%4$s%n",
                      service.getName(), parent, methods, serviceAnnotations)
                  .getBytes(Charset.defaultCharset()));
        }
      }
    }

    @Override
    public void close() throws IOException {
      this.channel.flush();
      this.channel.close();
    }

    private void writeIncludes(List<String> includes) throws IOException {
      if (!includes.isEmpty()) {
        String includeString =
            includes.stream()
                    .map(include -> format("include \"%s\"", include))
                    .collect(joining("\n"))
                + "\n\n";
        this.channel.write(includeString.getBytes(Charset.defaultCharset()));
      }
    }

    private void writeCppIncludes(List<String> includes) throws IOException {
      if (!includes.isEmpty()) {
        String includeString =
            includes.stream()
                    .map(include -> format("cpp_include \"%s\"", include))
                    .collect(joining("\n"))
                + "\n\n";
        this.channel.write(includeString.getBytes(Charset.defaultCharset()));
      }
    }

    private void writeNamespaces(Map<String, String> namespaces) throws IOException {
      if (!namespaces.isEmpty()) {
        String includeString =
            namespaces.entrySet().stream()
                    .map(
                        include ->
                            format("namespace %1$s %2$s", include.getKey(), include.getValue()))
                    .collect(joining("\n"))
                + "\n\n";
        this.channel.write(includeString.getBytes(Charset.defaultCharset()));
      }
    }

    private String getAnnotationString(List<TypeAnnotation> annotations) {
      StringBuilder stringBuilder = new StringBuilder();
      if (!annotations.isEmpty()) {
        stringBuilder.append(" (");
        List<String> annotationStringList = new ArrayList<>();
        for (TypeAnnotation annotation : annotations) {
          if (annotation.getValue() != null) {
            annotationStringList.add(annotation.getName() + " = \"" + annotation.getValue() + "\"");
          } else {
            annotationStringList.add(annotation.getName());
          }
        }
        stringBuilder.append(String.join(", ", annotationStringList));
        stringBuilder.append(")");
      }
      return stringBuilder.toString();
    }

    private String getThriftTypeString(ThriftType thriftType) {
      StringBuilder stringBuilder = new StringBuilder();

      if (thriftType instanceof BaseType) {
        BaseType baseType = (BaseType) thriftType;
        stringBuilder.append(baseType.getType().toString().toLowerCase());
        stringBuilder.append(getAnnotationString(baseType.getAnnotations()));

      } else if (thriftType instanceof ListType) {
        ListType listType = (ListType) thriftType;
        String baseTypeString = getThriftTypeString(listType.getElementType());
        stringBuilder.append(String.format("list<%s>", baseTypeString));
        stringBuilder.append(getAnnotationString(listType.getAnnotations()));

      } else if (thriftType instanceof IdentifierType) {
        IdentifierType identifierType = (IdentifierType) thriftType;
        stringBuilder.append(identifierType.getName());

      } else if (thriftType instanceof VoidType) {
        stringBuilder.append("void");

      } else if (thriftType instanceof MapType) {
        MapType mapType = (MapType) thriftType;
        stringBuilder
            .append(
                String.format(
                    "map<%1$s,%2$s>",
                    getThriftTypeString(mapType.getKeyType()),
                    getThriftTypeString(mapType.getValueType())))
            .append(getAnnotationString(mapType.getAnnotations()));
      }
      return stringBuilder.toString();
    }

    private String getIntegerEnumFieldsString(List<IntegerEnumField> integerEnumFields) {

      List<String> integerEnumFieldList = new ArrayList<>();
      for (IntegerEnumField field : integerEnumFields) {
        StringBuilder stringBuilder = new StringBuilder();

        if (field.getExplicitValue().isPresent()) {
          stringBuilder.append("  ").append(field.getName()).append(" = ").append(field.getValue());
        } else {
          stringBuilder.append("  ").append(field.getName());
        }

        // field.getAnnotations() returns null sometimes.
        try {
          if (!field.getAnnotations().isEmpty()) {
            stringBuilder.append(getAnnotationString(field.getAnnotations()));
          }
        } catch (NullPointerException ignored) {
        }

        integerEnumFieldList.add(stringBuilder.toString());
      }

      return String.join(",\n", integerEnumFieldList);
    }

    private String getStructFieldsString(
        List<ThriftField> structFields, String separator, Boolean indent) {

      List<String> structFieldList = new ArrayList<>();

      for (ThriftField structField : structFields) {
        StringBuilder stringBuilder = new StringBuilder();
        if (indent) {
          stringBuilder.append("  ");
        }
        stringBuilder.append(structField.getIdentifier().get()).append(": ");
        if (structField.getRequiredness().equals(ThriftField.Requiredness.OPTIONAL)) {
          stringBuilder.append("optional ");
        } else if (structField.getRequiredness().equals(ThriftField.Requiredness.REQUIRED)) {
          stringBuilder.append("required ");
        }
        stringBuilder
            .append(getThriftTypeString(structField.getType()))
            .append(" ")
            .append(structField.getName());
        if (structField.getValue().isPresent()) {
          stringBuilder.append(" = ").append(structField.getValue().get().getValueString());
        }

        // field.getAnnotations() returns null sometimes.
        try {
          if (!structField.getAnnotations().isEmpty()) {
            stringBuilder.append(getAnnotationString(structField.getAnnotations()));
          }
        } catch (NullPointerException ignored) {
        }
        structFieldList.add(stringBuilder.toString());
      }

      return String.join(separator, structFieldList);
    }

    private String getStringEnumFieldString(List<String> sEnumFields) {
      StringBuilder stringBuilder = new StringBuilder();
      List<String> sEnumFieldList = new ArrayList<>();

      for (String sEnumField : sEnumFields) {
        sEnumFieldList.add("  \"" + sEnumField + "\"");
      }

      stringBuilder.append(String.join(",\n", sEnumFieldList));

      return stringBuilder.toString();
    }

    private String getThriftMethodString(List<ThriftMethod> methods) {
      List<String> methodsList = new ArrayList<>();

      for (ThriftMethod method : methods) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("  ");
        if (method.isOneway()) {
          stringBuilder.append("oneway ");
        }
        stringBuilder
            .append(getThriftTypeString(method.getReturnType()))
            .append(" ")
            .append(method.getName());

        stringBuilder
            .append("(")
            .append(getStructFieldsString(method.getArguments(), ", ", false))
            .append(")");

        if (!method.getThrowsFields().isEmpty()) {
          stringBuilder
              .append(" throws (")
              .append(getStructFieldsString(method.getThrowsFields(), ",", false))
              .append(")");
        }

        if (!method.getAnnotations().isEmpty()) {
          stringBuilder.append(" ").append(getAnnotationString(method.getAnnotations()));
        }

        methodsList.add(stringBuilder.toString());
      }

      return String.join(",\n", methodsList);
    }
  }
}
