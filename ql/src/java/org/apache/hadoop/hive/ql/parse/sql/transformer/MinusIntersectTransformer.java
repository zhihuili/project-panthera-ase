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
package org.apache.hadoop.hive.ql.parse.sql.transformer;

import java.util.LinkedList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

/**
 * handle MINUS and INTERSECT in the same transformer, in case there are both MINUS and INTERSECT
 * under SUBQUERY node.
 *
 * MinusIntersectTransformer.
 *
 */
public class MinusIntersectTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;
  MinusTransformer minusTransformer = null;
  IntersectTransformer intersectTransformer = null;


  public MinusIntersectTransformer(SqlASTTransformer tf) {
    this.tf = tf;
    minusTransformer = new MinusTransformer();
    intersectTransformer = new IntersectTransformer();
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  private void trans(CommonTree node, TranslateContext context) throws SqlXlateException {
    List<CommonTree> childList = new LinkedList<CommonTree> ();
    // store every child into a list, since these node might be changed during transforming
    for (int i = 0; i < node.getChildCount(); i++) {
      childList.add((CommonTree) node.getChild(i));
    }
    for (int i = 0; i < childList.size(); i++) {
      trans(childList.get(i), context);
    }
    if (node.getType() == PantheraExpParser.PLSQL_RESERVED_MINUS) {
      minusTransformer.processIntersect(node, context);
    }
    if (node.getType() == PantheraExpParser.SQL92_RESERVED_INTERSECT) {
      intersectTransformer.processIntersect(node, context);
    }
  }

}