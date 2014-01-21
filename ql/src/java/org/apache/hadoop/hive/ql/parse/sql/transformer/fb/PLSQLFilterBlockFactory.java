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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb;

import java.util.List;
import java.util.Stack;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraConstants;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


/**
 * create PLSQL filter block.<br>
 * Define filter block's PLSQL node type.<br>
 * Implement PLSQL node type method.<br>
 * PLSQLFilterBlockFactory.
 *
 */
public class PLSQLFilterBlockFactory extends FilterBlockFactory {

  private static PLSQLFilterBlockFactory instance = new PLSQLFilterBlockFactory();

  private PLSQLFilterBlockFactory() {
    typeMap.put(PantheraParser_PLSQLParser.EQUALS_OP, EQUALS);
    typeMap.put(PantheraParser_PLSQLParser.NOT_EQUAL_OP, NOT_EQUAL);
    typeMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OP, GREATER_THAN);
    typeMap.put(PantheraParser_PLSQLParser.LESS_THAN_OP, LESS_THAN);
    typeMap.put(PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP, LESS_THAN_OR_EQUALS);
    typeMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP, GREATER_THAN_OR_EQUALS);
    typeMap.put(PantheraParser_PLSQLParser.NOT_IN, NOT_IN);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_IN, IN);
    typeMap.put(PantheraParser_PLSQLParser.NOT_BETWEEN, NOT_BETWEEN);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_BETWEEN, BETWEEN);
    typeMap.put(PantheraParser_PLSQLParser.NOT_LIKE, NOT_LIKE);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_LIKE, LIKE);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_AND, AND);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_OR, OR);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_NOT, NOT);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, SELECT);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS, EXISTS);
    typeMap.put(PantheraParser_PLSQLParser.SELECT_LIST, SELECT_LIST);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, WHERE);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING, HAVING);
    typeMap.put(PantheraParser_PLSQLParser.IS_NULL, IS_NULL);
    typeMap.put(PantheraParser_PLSQLParser.IS_NOT_NULL, IS_NOT_NULL);
  }

  public static PLSQLFilterBlockFactory getInstance() {
    return instance;
  }


  @Override
  public int isCorrelated(QueryInfo qInfo, Stack<CommonTree> selectStack, CommonTree cascatedElement)
      throws SqlXlateException {
    if (cascatedElement.getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
      cascatedElement = FilterBlockUtil.findOnlyNode(cascatedElement, PantheraParser_PLSQLParser.CASCATED_ELEMENT);
      if (cascatedElement == null) {
        return 0;
      }
    }
    CommonTree anyElement = (CommonTree) cascatedElement.getChild(0);
    if (anyElement.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      return isAnyElementCorrelated(qInfo, selectStack, anyElement);
    }
    return 0;
  }

  @Override
  public int isAnyElementCorrelated(QueryInfo qInfo, Stack<CommonTree> selectStack,
      CommonTree anyElement)
      throws SqlXlateException {
    Stack<CommonTree> tempStack = new Stack<CommonTree>();
    if (selectStack.size() <= 1) {
      return 0;
    }
    int level = -1;
    if (anyElement.getChildCount() == 2) {
      String tablename = anyElement.getChild(0).getText();
      // tableName.columnName
      if (tablename.startsWith(PantheraConstants.PANTHERA_UPPER)) {
        // secret sauce for some cases need add alias later!
        return 1;
      }
      while (selectStack.size() > 0) {
        if (SqlXlateUtil.containTableName(tablename, (CommonTree) selectStack
                .peek().getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM))) {
          level = tempStack.size();
          break;
        }
        tempStack.push(selectStack.pop());
      }
    } else {// only columnName
      String columnName = anyElement.getChild(0).getText();

      while (selectStack.size() > 0) {
        CommonTree from = (CommonTree) selectStack.peek().getFirstChildWithType(
            PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
        List<Column> columnList = qInfo.getRowInfo(from);
        boolean find = false;
        for (Column column : columnList) {
          if (columnName.equals(column.getColAlias())) {
            find = true;
            break;
          }
        }
        if (find) {
          level = tempStack.size();
          break;
        }
        tempStack.push(selectStack.pop());
      }
    }
    while (tempStack.size() > 0) {
      selectStack.push(tempStack.pop());
    }
    if (level >= 0) {
      return level;
    } else {
      throw new SqlXlateException((CommonTree) anyElement.getChild(0), "Element not find in scope.");
    }
  }
}
