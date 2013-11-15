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
import java.util.Iterator;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * remove redundant select items in non top most select-list
 * remove redundant group by items in all group by
 * RedundantSelectItemTransformer.
 *
 */
public class RedundantSelectGroupItemTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public RedundantSelectGroupItemTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context, PantheraParser_PLSQLParser.SELECT_LIST);
    trans(tree, context, PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
  }

  void trans(CommonTree tree, TranslateContext context, int type) {
    List<CommonTree> itemListList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(tree, type, itemListList);
    Iterator<CommonTree> it = itemListList.iterator();
    while (it.hasNext()) {
      CommonTree itemList = (CommonTree) it.next();
      if (type == PantheraParser_PLSQLParser.SELECT_LIST) {
        // out most select, process select list
        if (!it.hasNext()) {
          break;
        }
        removeDupItem(itemList, itemList.getChildCount(), type);
      } else {
        if (itemList.getChild(itemList.getChildCount() - 1).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING) {
          removeDupItem(itemList, itemList.getChildCount() - 1, type);
        } else {
          removeDupItem(itemList, itemList.getChildCount(), type);
        }
      }
    }
  }

  private class ColumnInfo {
    public String tableName;
    public String columnName;
    public int position;

    ColumnInfo(String columnName, int position) {
      this.tableName = "";
      this.columnName = columnName;
      this.position = position;
    }

    ColumnInfo(String tableName, String columnName, int position) {
      this.tableName = tableName;
      this.columnName = columnName;
      this.position = position;
    }
  }

  private void removeDupItem(CommonTree itemList, int count, int type) {
    List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
    List<CommonTree> deleteList = new ArrayList<CommonTree>();
    for (int i = 0; i < count; i++) {
      CommonTree item = (CommonTree) itemList.getChild(i);
      int needDelete = -1;
      if (item.getChildCount() == 2) {
        needDelete = checkCheckedList(columnList, new ColumnInfo(item.getChild(1).getChild(0)
            .getText(), i));
      } else {
        CommonTree expr = (CommonTree) item.getChild(0);
        if (((CommonTree) expr.getChild(0)).getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
          if (type == PantheraParser_PLSQLParser.SELECT_LIST) {
            // in non-out-most query, a expression without alias not useful for outer select, can be
            // deleted
            deleteList.add(item);
            continue;
          } else { // type == PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP
            boolean hasSameTreeFlag = false;
            for (int j = 0; j < i; j++) {
              if (FilterBlockUtil.equalsTree(item, (CommonTree) itemList.getChild(j))) {
                hasSameTreeFlag = true;
                break;
              }
            }
            if (hasSameTreeFlag) {
              deleteList.add(item);
            }
            continue;
          }
        }
        CommonTree anyElement = (CommonTree) expr.getChild(0).getChild(0);
        if (anyElement.getChildCount() == 2) {
          needDelete = checkCheckedList(columnList, new ColumnInfo(anyElement.getChild(0)
              .getText(), anyElement.getChild(1).getText(), i));
        } else {
          needDelete = checkCheckedList(columnList, new ColumnInfo(anyElement.getChild(0)
              .getText(), i));
        }
      }
      if (needDelete >= 0) {
        deleteList.add((CommonTree) itemList.getChild(needDelete));
      }
    }

    // after check, delete those in deleteList
    for (int i = 0; i < deleteList.size(); i++) {
      itemList.deleteChild(deleteList.get(i).childIndex);
    }

  }

  private int checkCheckedList(List<ColumnInfo> ciList, ColumnInfo ci) {
    for (int i = 0; i < ciList.size(); i++) {
      ColumnInfo ici = ciList.get(i);
      if (ici.columnName.equals(ci.columnName)) {
        if (ci.tableName.equals("")) {
          return ci.position;
        } else if (ici.tableName.equals(ci.tableName)) {
          return ci.position;
        } else if (ici.tableName.equals("")) {
          ciList.remove(i);
          ciList.add(ci);
          return ici.position;
        }
      }
    }
    ciList.add(ci);
    return -1;
  }

}
