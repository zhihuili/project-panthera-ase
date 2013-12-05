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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * handle user written schema.table
 * add alias for schema.table in from clause and substitute schema.table with the alias exclude from
 * clause.
 *
 * TODO:
 * need to support more for schema.table, currently don't support "table.column" in filters when
 * the table
 * is corresponding to schema.table. Only support "column" or "schema.table.column".
 * (it is to be decided whether need to support(there is same table name in current schema with
 * specified schema)! e.g. select s_grade from zly.staff join zly.works where staff.s_empnum =
 * works.w_empnum;)
 * e.g.
 * only support: SELECT COL1, EMPNUM, GRADE FROM CUGINI.VTABLE, STAFF WHERE COL1 < 200 AND GRADE >
 * 12;
 * and SELECT COL1, EMPNUM, GRADE FROM CUGINI.VTABLE, STAFF WHERE CUGINI.VTABLE.COL1 < 200 AND GRADE
 * > 12;
 * not support: SELECT COL1, EMPNUM, GRADE FROM CUGINI.VTABLE, STAFF WHERE VTABLE.COL1 < 200 AND
 * GRADE > 12;
 *
 * SchemaDotTableTransformer.
 *
 */
public class SchemaDotTableTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public SchemaDotTableTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  protected void transform(CommonTree tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);

    trans(tree, context);

    List<CommonTree> anyElementList = new ArrayList<CommonTree>();
    anyElementList.clear();
    // after trans there should be no schema.table.column left.
    // check whether there is any schema.table.column left
    existsSchemaDotTable(tree, context, anyElementList);
    if (!anyElementList.isEmpty()) {
      throw new SqlXlateException(anyElementList.get(0), "Unknow column name: "
          + anyElementList.get(0).getChild(0).getText() + "."
          + anyElementList.get(0).getChild(1).getText() + "."
          + anyElementList.get(0).getChild(2).getText());
    }
  }

  void trans(CommonTree tree, TranslateContext context) throws SqlXlateException {
    int childCount = tree.getChildCount();
    for (int i = 0; i < childCount; i++) {
      trans((CommonTree) tree.getChild(i), context);
    }
    if (tree.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT) {
      processSelect(tree, context);
    }
  }

  /**
   * each SELECT statement is a processing unit.
   * deep first.
   */
  private void processSelect(CommonTree select, TranslateContext context) throws SqlXlateException {
    // handle subQuery in where clause
    CommonTree where = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    if (where != null) {
      trans(where, context);
    }
    // handle subQuery in having clause
    CommonTree group = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group != null) {
      CommonTree having = (CommonTree) group.getChild(group.getChildCount() - 1);
      if (having.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING) {
        trans(having, context);
      }
    }
    // handle subQuery in from clause
    CommonTree from = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    trans(from, context);

    Map<String, String> schemaDotTblAliasPair = new HashMap<String, String>();
    getSchemaDotTblAliasPair(from, schemaDotTblAliasPair);
    // if there is no schema.table in from clause, just return.
    if (schemaDotTblAliasPair.isEmpty()) {
      return;
    }
    // skip from node, refresh other branches under select node.
    for (int i = 1; i < select.getChildCount(); i++) {
      // refresh anyElement
      substituteAnyElement((CommonTree) select.getChild(i), context, schemaDotTblAliasPair);
      // refresh schema.table.* (under tableViewName node instead of anyElement)
      substituteTableViewName((CommonTree) select.getChild(i), context, schemaDotTblAliasPair);
    }
    // refresh order by
    if (select.getParent().getType() == PantheraParser_PLSQLParser.SUBQUERY
        && select.getParent().getParent().getType() == PantheraParser_PLSQLParser.SELECT_STATEMENT) {
      CommonTree order = (CommonTree) ((CommonTree) select.getParent().getParent())
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER);
      if (order != null) {
        // refresh anyElement
        substituteAnyElement(order, context, schemaDotTblAliasPair);
        // refresh schema.table.* (under tableViewName node instead of anyElement)
        substituteTableViewName(order, context, schemaDotTblAliasPair);
      }
    }
  }

  /**
   * create alias for schema.table in from clause and make pair.
   */
  private void getSchemaDotTblAliasPair(CommonTree node, Map<String, String> schemaDotTblAliasPair) {
    // only top TABLE_REF_ELEMENT under from node need to be processed
    if (node.getType() == PantheraParser_PLSQLParser.TABLE_REF_ELEMENT) {
      String aliasName = null;
      String name = null;
      int index = 0;
      if (node.getChild(0).getType() == PantheraParser_PLSQLParser.ALIAS) {
        // if table_ref_element has alias originally, then no need to transform schema.table.
        return;
      }
      int type = node.getChild(index).getType();
      if (type == PantheraParser_PLSQLParser.TABLE_EXPRESSION) {
        type = node.getChild(index).getChild(0).getType();
        if (type == PantheraParser_PLSQLParser.DIRECT_MODE) {
          CommonTree tableViewName = (CommonTree) node.getChild(index).getChild(0).getChild(0);
          name = tableViewName.getChild(0).getText();
          if (tableViewName.getChildCount() > 1) {
            // schema.table
            // create alias for schema.table
            name += "_" + tableViewName.getChild(1).getText();
            aliasName = "Panthera_" + name;
            CommonTree tableAlias = FilterBlockUtil.createAlias(node, aliasName);
            SqlXlateUtil.addCommonTreeChild(node, 0, tableAlias);
            schemaDotTblAliasPair.put(name, "Panthera_" + name);
          }
        } else if (type == PantheraParser_PLSQLParser.SELECT_MODE) {
          // for select_mode, there would not exists schema.table format
        } else {
          return;
        }
      } else if (type == PantheraParser_PLSQLParser.TABLE_REF) {
        // TODO: join operator: t1, t2, (t3 join t4) join_alias ...
        // currently Panthera doesn't support this format.
      }
      return;
    }
    // recurse for all children
    for (int i = 0; i < node.getChildCount(); i++) {
      getSchemaDotTblAliasPair((CommonTree) node.getChild(i), schemaDotTblAliasPair);
    }
  }

  /**
   * check whether there is schema.table.column in the query (under anyElement node)
   *
   * @param tree
   * @param context
   * @param schemaTblAnyEleList
   * @throws SqlXlateException
   */
  private void existsSchemaDotTable(CommonTree tree, TranslateContext context,
      List<CommonTree> schemaTblAnyEleList) throws SqlXlateException {
    List<CommonTree> anyElementList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(tree, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElementList);
    if (!anyElementList.isEmpty()) {
      for (CommonTree anyElement : anyElementList) {
        if (anyElement.getChildCount() > 2) {
          schemaTblAnyEleList.add(anyElement);
        }
      }
    }
  }

  /**
   * find all node which type is input type in the tree which root is node.
   * subTree whose root with excluldeBranch type will be excluded.
   */
  public static void findNodeWithoutBranch(CommonTree node, int type, List<CommonTree> nodeList,
      int excludeBranch) {
    if (node == null) {
      return;
    }
    if (node.getType() == excludeBranch) {
      return;
    }
    if (node.getType() == type) {
      nodeList.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      findNodeWithoutBranch((CommonTree) node.getChild(i), type, nodeList, excludeBranch);
    }
  }

  /**
   * substitute the table.schema.column in filters with alias of schema.table in from clause.
   *
   * @param anyElementList
   * @param schemaTblAliasPair
   * @throws SqlXlateException
   */
  private void refreshSchemaTbl(List<CommonTree> anyElementList,
      Map<String, String> schemaTblAliasPair) throws SqlXlateException {
    // anyElementList should never be empty.
    for (CommonTree anyElement : anyElementList) {
      assert (anyElement.getChildCount() > 2);
      String schemaDotTable = anyElement.getChild(0).getText() + "_"
          + anyElement.getChild(1).getText();
      String alias = schemaTblAliasPair.get(schemaDotTable);
      if (alias != null) {
        CommonTree table = FilterBlockUtil.createSqlASTNode(anyElement,
            PantheraParser_PLSQLParser.ID, alias);
        anyElement.replaceChildren(0, 1, table);
      }
    }
  }

  /**
   * substitute schema.table.column under AnyElement with alias.
   */
  private void substituteAnyElement(CommonTree node, TranslateContext context,
      Map<String, String> schemaTblAliasPair) throws SqlXlateException {
    List<CommonTree> anyElementList = new ArrayList<CommonTree>();
    existsSchemaDotTable(node, context, anyElementList);
    if (!anyElementList.isEmpty()) {
      refreshSchemaTbl(anyElementList, schemaTblAliasPair);
    }
  }

  /**
   * for select schema.table.* case.
   * substitute schema.table under TableViewName node with alias.
   */
  private void substituteTableViewName(CommonTree node, TranslateContext context,
      Map<String, String> schemaTblAliasPair) throws SqlXlateException {
    List<CommonTree> tableViewNameList = new ArrayList<CommonTree>();
    findNodeWithoutBranch(node, PantheraParser_PLSQLParser.TABLEVIEW_NAME, tableViewNameList,
        PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    if (!tableViewNameList.isEmpty()) {
      for (CommonTree tableViewName : tableViewNameList) {
        if (tableViewName.getChildCount() == 2) {
          String schemaDotTable = tableViewName.getChild(0).getText() + "_"
              + tableViewName.getChild(1).getText();
          String alias = schemaTblAliasPair.get(schemaDotTable);
          if (alias != null) {
            CommonTree table = FilterBlockUtil.createSqlASTNode(tableViewName,
                PantheraParser_PLSQLParser.ID, alias);
            tableViewName.replaceChildren(0, 1, table);
          }
        }
      }
    }
  }

}
