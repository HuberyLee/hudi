/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.secondary.index.filter;

import org.apache.hudi.internal.schema.Types.Field;
import org.apache.hudi.secondary.index.ISecondaryIndexReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class IndexFilter implements IFilter {
  protected ISecondaryIndexReader indexReader;
  protected Field[] fields;

  public IndexFilter() {
  }

  public IndexFilter(ISecondaryIndexReader indexReader, Field field) {
    this.indexReader = indexReader;
    this.fields = new Field[] {field};
  }

  public IndexFilter(Field[]... fields) {
    List<Field> tmpFields = new ArrayList<>();
    for (Field[] fs : fields) {
      tmpFields.addAll(Arrays.asList(fs));
    }
    this.fields = tmpFields.toArray(new Field[0]);
  }

  public Field getField() {
    return fields[0];
  }

  public Field[] getFields() {
    return fields;
  }
}
