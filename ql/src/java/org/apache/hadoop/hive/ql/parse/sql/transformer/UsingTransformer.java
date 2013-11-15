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

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

/**
 * transform USING to ON
 *
 * UsingTransformer.
 *
 */
public class UsingTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public UsingTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  private void trans(CommonTree node, TranslateContext context) throws SqlXlateException {
    int childCount = node.getChildCount();
    for (int i = 0; i < childCount; i++) {
      trans((CommonTree) node.getChild(i), context);
    }
    if (node.getType() == PantheraExpParser.PLSQL_NON_RESERVED_USING) {
      processUsing(node, context);
    }
  }

  private void processUsing(CommonTree node, TranslateContext context) throws SqlXlateException {
    CommonTree join = (CommonTree) node.getParent();
    int index = node.getChildIndex();
    if (join.getType() != PantheraExpParser.JOIN_DEF) {
      throw new SqlXlateException(join, "Unsupported using type:" + join.getText());
    }
    CommonTree tableRef = (CommonTree) join.getParent();
    CommonTree leftTableRefElement = (CommonTree) tableRef
        .getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT);
    CommonTree rightTableRefElement = (CommonTree) join
        .getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT);
    String leftTableName = getTableName(leftTableRefElement);
    String rightTableName = getTableName(rightTableRefElement);
    List<String> leftColumnList = new ArrayList<String>();
    List<String> rightColumnList = new ArrayList<String>();
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree columnName = (CommonTree) node.getChild(i);
      String column = columnName.getChild(0).getText();
      leftColumnList.add(column);
      rightColumnList.add(column);
    }
    CommonTree on = FilterBlockUtil.makeOn(node, leftTableName, rightTableName, leftColumnList,
        rightColumnList);
    join.deleteChild(index);
    SqlXlateUtil.addCommonTreeChild(join, index, on);
  }

  private String getTableName(CommonTree tableRefElement) throws SqlXlateException {
    String tableName;
    if (tableRefElement.getFirstChildWithType(PantheraExpParser.ALIAS) != null) {
      tableName = tableRefElement.getChild(0).getChild(0).getText();
    } else {
      CommonTree tableViewName = FilterBlockUtil.findOnlyNode(tableRefElement,
          PantheraExpParser.TABLEVIEW_NAME);
      if (tableViewName == null) {
        throw new SqlXlateException(tableRefElement, "Can't find table name from join");
      }
      tableName = tableViewName.getChild(0).getText();
    }
    return tableName;
  }
}
