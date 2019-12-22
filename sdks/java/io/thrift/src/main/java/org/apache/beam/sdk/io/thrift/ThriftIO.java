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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading and writing files containing Thrift encoded data.
 *
 * <h3>Reading Thrift Files</h3>
 *
 * <p>For simple reading, use {@link ThriftIO#} with the desired file pattern to read from.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<ExampleType> examples = pipeline.apply(ThriftIO.read().from("/foo/bar/*"));
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
 * PCollection<ExampleType> examples = files.apply(ThriftIO.readFiles(ExampleType.class).withProtocol(thriftProto);
 * }</pre>
 *
 * <h3>Writing Thrift Files</h3>
 *
 * <p>{@link ThriftIO.Sink} allows for a {@link PCollection} of {@link byte[]} to be written to
 * Thrift files. It can be used with the general-purpose {@link FileIO} transforms with
 * FileIO.write/writeDynamic specifically.
 *
 * <p>By default, {@link ThriftIO.Sink} can write multiple {@link byte[]} to a file so it is highly
 * recommended to provide a unique name for each desired file.
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // PCollection<byte[]>
 *   .apply(FileIO
 *     .<byte[]>write()
 *     .via(ThriftIO.sink()
 *     .to("destination/path")
 *     .withPrefix("unique_name")
 *     .withSuffix(".thrift"));
 * }</pre>
 *
 * <p>This IO API is considered experimental and may break or receive backwards-incompatible changes
 * in future versions of the Apache Beam SDK.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class ThriftIO {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftIO.class);

  /** Disable construction of utility class. */
  private ThriftIO() {}

  /**
   * Like {@link #read()},but reads each file in a {@link PCollection} of {@link
   * org.apache.beam.sdk.io.FileIO.ReadableFile}, which allows more flexible usage.
   */
  public static <T> ReadFiles<T> readFiles(Class<T> recordClass) {
    return new AutoValue_ThriftIO_ReadFiles.Builder<T>().setRecordClass(recordClass).build();
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Creates a {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}. */
  public static Sink sink() {
    return new AutoValue_ThriftIO_Sink.Builder().build();
  }

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles<T>
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {

    abstract Builder<T> toBuilder();

    @Nullable
    abstract Class<T> getRecordClass();

    @Nullable
    abstract TProtocolFactory getTProtocol();

    public ReadFiles<T> withProtocol(TProtocolFactory protocol) {
      return toBuilder().setTProtocol(protocol).build();
    }

    @Override
    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {
      checkNotNull(getRecordClass(), "Record class cannot be null");
      checkNotNull(getTProtocol(), "Thrift protocol cannot be null");
      final Coder<T> coder = ThriftCoder.of(getRecordClass(), getTProtocol());
      return input.apply(ParDo.of(new ReadFn<>(getRecordClass(), getTProtocol()))).setCoder(coder);
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setTProtocol(TProtocolFactory tProtocol);

      abstract ReadFiles<T> build();
    }

    /**
     * Reads a {@link FileIO.ReadableFile} and outputs a {@link PCollection} of {@link
     * #getRecordClass()}.
     */
    static class ReadFn<T> extends DoFn<FileIO.ReadableFile, T> {

      final Class<T> tBaseType;
      final TProtocolFactory tProtocol;

      ReadFn(Class<T> tBaseType, TProtocolFactory tProtocol) {
        this.tBaseType = tBaseType;
        this.tProtocol = tProtocol;
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) {
        FileIO.ReadableFile file = processContext.element();

        try {

          TBase tb = (TBase) tBaseType.getDeclaredConstructor().newInstance();
          // TMemoryBuffer buffer = new TMemoryBuffer((int) file.getMetadata().sizeBytes());
          TDeserializer deserializer = new TDeserializer(tProtocol);
          deserializer.deserialize(tb, file.readFullyAsBytes());
          processContext.output((T) tb);

        } catch (Exception ioe) {

          String filename = file.getMetadata().resourceId().toString();
          LOG.error(String.format("Error in reading file: %1$s%n%2$s", filename, ioe));
          throw new RuntimeException(ioe);
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item("Thrift class", getRecordClass().toString()).withLabel("Thrift class"));
      builder.add(
          DisplayData.item("Thrift Protocol", getTProtocol().toString())
              .withLabel("Protocol Type"));
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
   * <p>If no prefix is provided then {@link byte[]#hashCode()} will be used to avoid files from
   * colliding.
   *
   * <p>If no suffix is provided then ".thrift" will be used.
   *
   * <p>All methods also support {@link ValueProvider}.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    abstract Builder toBuilder();

    @Nullable
    abstract ValueProvider<String> getPrefix();

    @Nullable
    abstract ValueProvider<String> getSuffix();

    @Nullable
    abstract ValueProvider<String> getDestination();

    @Override
    public PDone expand(PCollection<byte[]> input) {
      checkNotNull(getDestination(), "Destination must not be null.");

      /*input.apply(
      FileIO.<byte[], byte[]>writeDynamic()
          .by(byte[]::getbyte[])
          .withNaming(
              naming ->
                  FileIO.Write.defaultNaming(
                      (getPrefix() != null)
                          ? getPrefix().get()
                          : String.valueOf(naming.hashCode()),
                      (getSuffix() != null) ? getSuffix().get() : DEFAULT_THRIFT_SUFFIX))
          .via(ThriftIO.sink())
          .to(getDestination())
          .withDestinationCoder(ThriftCoder.of()));*/

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
     * filename or filename pattern. This can be any path supported by {@link FileIO}, for example:
     * a Google Cloud Storage filename or filename pattern of the form {@code
     * "gs://<bucket>/<filepath>"} (if running locally or using remote execution). Standard <a
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
  public abstract static class Sink<T> implements FileIO.Sink<T> {

    abstract Builder toBuilder();

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      // this.writer = new ThriftWriter(Channels.newOutputStream(channel));
    }

    @Override
    public void write(T element) throws IOException {
      // checkNotNull(writer, "Writer cannot be null");
      // writer.write(element);
    }

    @Override
    public void flush() throws IOException {
      // writer.close();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Sink build();
    }
  }
}
