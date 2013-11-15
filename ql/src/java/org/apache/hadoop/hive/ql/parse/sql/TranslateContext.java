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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil.HiveMetadata;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;

/**
 * In SQL translate phase, store context information for thread.
 *
 * TranslateContext.
 *
 */
public class TranslateContext {
  public final static String JOIN_TYPE_NODE_BALL = "joinTypeNode";

  private HiveConf conf;
  private HiveMetadata meta;
  private QueryInfo qInfoRoot;
  private final Map<Object, Object> basket = new HashMap<Object, Object>();
  private List<QueryInfo> qInfoList;// without QueryInfo root
  SqlXlateUtil.AliasGenerator aliasGen = new SqlXlateUtil.AliasGenerator();

  public TranslateContext(HiveConf conf) throws SqlXlateException {
    this.conf = conf;
    meta = new HiveMetadata(conf);
  }

  public List<QueryInfo> getqInfoList() {
    return qInfoList;
  }

  public HiveMetadata getMeta() {
    return meta;
  }

  public void setMeta(HiveMetadata meta) {
    this.meta = meta;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public QueryInfo getQInfoRoot() {
    return qInfoRoot;
  }

  public void setQInfoRoot(QueryInfo qf) {
    this.qInfoRoot = qf;
    qInfoList = new ArrayList<QueryInfo>();
    for (QueryInfo q : qf.getChildren()) {
      buildQInfoList(qInfoList, q);
    }
  }

  private void buildQInfoList(List<QueryInfo> qfl, QueryInfo qf) {
    qfl.add(qf);
    for (QueryInfo q : qf.getChildren()) {
      buildQInfoList(qInfoList, q);
    }
  }



  public Object getBallFromBasket(Object name) {
    return basket.get(name);
  }

  public void putBallToBasket(Object name, Object value) {
    basket.put(name, value);
  }

  public Map<Object, Object> getBasket() {
    return basket;
  }

  public SqlXlateUtil.AliasGenerator getAliasGen() {
    return aliasGen;
  }

}
