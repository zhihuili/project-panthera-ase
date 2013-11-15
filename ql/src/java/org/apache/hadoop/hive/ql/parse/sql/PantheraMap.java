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

package org.apache.hadoop.hive.ql.parse.sql;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A faster Map implement for SQL translating.<br>
 * Key which is usually tree node type must be Integer.<br>
 * PantheraMap.
 *
 * @param <V>
 */
public class PantheraMap<V> implements Map<Integer, V> {
  private static final String EXCEPTION_MSG = "Not Support!";
  private static final long serialVersionUID = 1678863398833113238L;
  private int capacity = 1000;
  private Object[] kv;

  public PantheraMap() {
    init();
  }

  public PantheraMap(int size) {
    this.capacity = size;
    init();
  }

  private void init() {
    kv = new Object[capacity];
  }

  @Override
  public V put(Integer key, V value) {
    if (this.get(key) != null) {
      throw new RuntimeException("Depulcate -" + value.getClass().getName() + " with " + key);
    }

    kv[key] = value;
    return value;
  }

  /**
   * BE CARE FOR<br>
   * It dose not return KV entry's size, it just return map's capacity.<br>
   */
  @Override
  public int size() {
    return capacity;
  }

  @Override
  public V get(Object key) {
    return (V) kv[(Integer) key];
  }

  @Override
  public void clear() {
    throw new RuntimeException(EXCEPTION_MSG);

  }

  @Override
  public boolean containsKey(Object key) {
    throw new RuntimeException(EXCEPTION_MSG);
  }

  @Override
  public boolean containsValue(Object value) {
    throw new RuntimeException(EXCEPTION_MSG);
  }

  @Override
  public Set<java.util.Map.Entry<Integer, V>> entrySet() {
    throw new RuntimeException(EXCEPTION_MSG);
  }

  @Override
  public boolean isEmpty() {
    throw new RuntimeException(EXCEPTION_MSG);
  }

  @Override
  public Set<Integer> keySet() {
    throw new RuntimeException(EXCEPTION_MSG);
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends V> m) {
    throw new RuntimeException(EXCEPTION_MSG);
  }

  @Override
  public V remove(Object key) {
    throw new RuntimeException(EXCEPTION_MSG);
  }

  @Override
  public Collection<V> values() {
    throw new RuntimeException(EXCEPTION_MSG);
  }

}
