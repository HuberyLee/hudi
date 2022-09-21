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
import org.apache.hudi.secondary.index.IRowIdSet;

import java.io.IOException;

public class OrFilter extends IndexFilter {
  private final IndexFilter leftFilter;
  private final IndexFilter rightFilter;

  public OrFilter(IndexFilter leftFilter, IndexFilter rightFilter) {
    this.leftFilter = leftFilter;
    this.rightFilter = rightFilter;

    Field[] leftFields = leftFilter.getFields();
    Field[] rightFields = rightFilter.getFields();
    fields = new Field[leftFields.length + rightFields.length];
    System.arraycopy(leftFields, 0, fields, 0, leftFields.length);
    System.arraycopy(rightFields, 0, fields, leftFields.length, rightFields.length);
  }

  @Override
  public IRowIdSet getRowIdSet() throws IOException {
    IRowIdSet rowIdSet = leftFilter.getRowIdSet();
    return rowIdSet.or(rightFilter.getRowIdSet());
  }

  @Override
  public String toString() {
    return "or(" + leftFilter.toString() + ", " + rightFilter.toString() + ")";
  }
}
