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
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * translate [xx in (select max(x) from t1 where ...)] to [xx = (select max(x) from t1 where ...)] <br/>
 * translate [xx not in (select max(x) from t1 where ...)] to [xx <> (select max(x) from t1 where
 * ...)] <br/>
 * InTransformer.
 *
 */
public class InTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public InTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  void trans(CommonTree tree, TranslateContext context) throws SqlXlateException {
    // deep firstly
    for (int i = 0; i < tree.getChildCount(); i++) {
      trans((CommonTree) (tree.getChild(i)), context);
    }
    if (tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_IN
        && tree.getChild(1).getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      transIn(false, tree, context);
    }
    if (tree.getType() == PantheraExpParser.NOT_IN
        && tree.getChild(1).getType() == PantheraParser_PLSQLParser.SUBQUERY) {
      transIn(true, tree, context);
    }
  }

  void transIn(Boolean isNot, CommonTree tree, TranslateContext context) throws SqlXlateException {
    if (!processIn(tree)) {
      if (isNot) {
        tree.token.setType(PantheraParser_PLSQLParser.NOT_EQUAL_OP);
        tree.token.setText("<>");
      } else {
        // translate [xx in (select max(x) from t1 where ...)] to [xx = (select max(x) from t1 where
        // ...)]
        tree.token.setType(PantheraParser_PLSQLParser.EQUALS_OP);
        tree.token.setText("=");
      }
      return;
    }
  }

  Boolean processIn(CommonTree tree) throws SqlXlateException {
    CommonTree bottomSelect = (CommonTree) tree.getChild(1).getChild(0);
    CommonTree bottomSelectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    if (bottomSelectList == null) {
      return true;
    }
    CommonTree rightIn = (CommonTree) bottomSelectList.getChild(0).getChild(0).getChild(0);
    if (FilterBlockUtil.isAggrFunc(rightIn)) {
      CommonTree group = (CommonTree) bottomSelect
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
      if (group == null) {
        return false;
      }
    }
    return true;
  }

}
