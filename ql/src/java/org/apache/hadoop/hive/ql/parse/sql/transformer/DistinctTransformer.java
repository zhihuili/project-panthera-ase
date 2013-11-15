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

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * delete distinct if there is group node under the select.
 * if there is group node, all rows will not have duplicated ones.
 *
 * DistinctTransformer.
 *
 */
public class DistinctTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public DistinctTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformDistinct(tree, context);
  }

  private void transformDistinct(CommonTree tree, TranslateContext context)
      throws SqlXlateException {
    int childCount = tree.getChildCount();
    for (int i = 0; i < childCount; i++) {
      transformDistinct((CommonTree) tree.getChild(i), context);
    }
    if (tree.getType() == PantheraExpParser.SQL92_RESERVED_DISTINCT) {
      processDistinct((CommonTree) tree.getParent(), context);
    }
  }

  private void processDistinct(CommonTree distinct, TranslateContext context) {
    CommonTree select = (CommonTree) distinct.getParent();
    if (select.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      if (select.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP) != null) {
        select.deleteChild(distinct.getChildIndex());
      }
    }
  }

}
