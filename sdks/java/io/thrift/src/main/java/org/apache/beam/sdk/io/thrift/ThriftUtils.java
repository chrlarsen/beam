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

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.thrift.TBase;

/** Utilities for interacting with Beam {@link Schema}s and Thrift encoded objects. */
public class ThriftUtils {

  private ThriftUtils() {}

  /**
   * Converts a Thrift class to a Beam schema.
   * @param clazz class to be converted to schema
   * @param <T> type of class to be converted
   * @return {@link Schema} Beam schema
   */
  public static <T extends TBase<?,?>> Schema toBeamSchema(Class<T> clazz) {
    Schema.Builder builder = Schema.builder();

    for (java.lang.reflect.Field f : ReflectUtils.getFields(clazz)) {
      Schema.Field field = toBeamField(f);
      builder.addField(field);
    }

    return builder.build();
  }

  public static Schema.Field toBeamField(java.lang.reflect.Field field) {
    Schema.FieldType beamFieldType = toFieldType(field.getType())
  }

  /** Converts Thrift record field to a Beam field. */
  private static Schema.FieldType toFieldType() {

  }
  /*

    public static <T> SchemaUserTypeCreator getCreator(Class<T> clazz, Schema schema) {

    }

    public static <T> List<FieldValueGetter> getGetters(Class<T> clazz, Schema schema) {

    }

    public static <T> List<FieldValueTypeInformation> getFieldTypes(Class<T> clazz, Schema schema) {

    }

    public static <T> SerializableFunction<T, Row> getToRowFunction(
            Class<T> clazz) {

    }

    public static <T> SerializableFunction<T, Row> getFromRowFunction(
            Class<T> clazz) {

    }
  */

  private static final class ThriftFieldValueTypeSupplier implements FieldValueTypeSupplier {
    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz) {
      Map<String, FieldValueTypeInformation> types = Maps.newHashMap();
      for (java.lang.reflect.Field f : ReflectUtils.getFields(clazz)) {
        FieldValueTypeInformation typeInformation = FieldValueTypeInformation.forField(f);
      }
      return null;
    }
  }
}
