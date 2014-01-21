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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraConstants;
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
 * support: SELECT COL1, EMPNUM, GRADE FROM CUGINI.VTABLE, STAFF WHERE COL1 < 200 AND GRADE > 12;
 * and: SELECT COL1, EMPNUM, GRADE FROM CUGINI.VTABLE, STAFF WHERE CUGINI.VTABLE.COL1 < 200 AND GRADE > 12;
 * and: SELECT COL1, EMPNUM, GRADE FROM CUGINI.VTABLE, STAFF WHERE VTABLE.COL1 < 200 AND GRADE > 12;
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
    // after trans there should be no schema.table.column left.
    // check whether there is any schema.table.column left
    existsSchemaDotTable(tree, anyElementList);
    if (!anyElementList.isEmpty()) {
      throw new SqlXlateException(anyElementList.get(0), "Unknow column name: "
          + anyElementList.get(0).getChild(0).getText() + "."
          + anyElementList.get(0).getChild(1).getText() + "."
          + anyElementList.get(0).getChild(2).getText());
    }
  }

  void trans(CommonTree tree, TranslateContext context) throws SqlXlateException {
    int childCount = tree.getChildCount();
    // transform FROM after WHERE and GROUP
    for (int i = childCount - 1; i >= 0; i--) {
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
    CommonTree from = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);

    Map<String, String> schemaDotTblAliasPair = new HashMap<String, String>();
    Map<String, CommonTree> expectedAliasToAliasNodePair = new HashMap<String, CommonTree>();
    List<CommonTree> tempTblList = new ArrayList<CommonTree>();
    getSchemaDotTblAliasPair(from, schemaDotTblAliasPair, expectedAliasToAliasNodePair, tempTblList);
    // if there is no schema.table in from clause, just return.
    if (schemaDotTblAliasPair.isEmpty()) {
      return;
    }
    Iterator<String> it = expectedAliasToAliasNodePair.keySet().iterator();
    while (it.hasNext()) {
      String key = it.next();
      CommonTree val = expectedAliasToAliasNodePair.get(key);
      String aliasName = val.getText();
      if (aliasName.startsWith(PantheraConstants.PANTHERA_PREFIX)) {
        // this is where we need to simplify alias name to origin table name.
        // val is the alias node, restore it.
        val.token.setText(key);
        for (CommonTree tbl : tempTblList) {
          // check if we have refreshed ON-clause tablename
          if (tbl.getText().equals(aliasName)) {
            // restore it.
            tbl.token.setText(key);
          }
        }
        String oldAlias = schemaDotTblAliasPair.get(aliasName);
        if (oldAlias != null) {
          schemaDotTblAliasPair.remove(aliasName);
          // should refresh the complex column as origin name
          schemaDotTblAliasPair.put(aliasName, key);
        }
      }
    }
    // now we can refresh all with schemaDotTblAliasPair, purely
    // skip from node, refresh other branches under select node.
    for (int i = 1; i < select.getChildCount(); i++) {
      // refresh anyElement
      substituteAnyElement((CommonTree) select.getChild(i), schemaDotTblAliasPair);
      // refresh schema.table.* (under tableViewName node instead of anyElement)
      substituteTableViewName((CommonTree) select.getChild(i), schemaDotTblAliasPair);
    }
    // refresh order by
    if (select.getParent().getType() == PantheraParser_PLSQLParser.SUBQUERY
        && select.getParent().getParent().getType() == PantheraParser_PLSQLParser.SELECT_STATEMENT) {
      CommonTree order = (CommonTree) ((CommonTree) select.getParent().getParent())
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER);
      if (order != null) {
        // refresh anyElement
        substituteAnyElement(order, schemaDotTblAliasPair);
        // refresh schema.table.* (under tableViewName node instead of anyElement)
        substituteTableViewName(order, schemaDotTblAliasPair);
      }
    }
  }

  /**
   *  create alias for schema.table in from clause and make pair.
   *  For each schema.table, one alias will be created. The alias will be the same name with table if
   *  there is no other schema.table which has the same table name with the current one. Otherwise,
   *  the alias will be in format of PantheraConstants.PANTHERA_PREFIX + "schema_table".
   *  e.g. select * from schema1.table1 cross join schema2.table1 where schema1.table1.col1 = 3; will transformed into:
   *  select * from schema1.table1 panthera_schema1_table1 cross join schema2.table1 panthera_schema2_table1 where table1.col1 = 3;
   *  later in processSelect(); will restore table alias for tables with unique table name as original table name.
   *
   *  in this way, there would no need to refresh the table name after adding alias since there would not
   *  exists such queries:
   *  select * from schema1.table1, schema2.table1 where table1.col1 = 3; which will lead to ambiguous table name "table1".
   *
   * @param node
   * @param schemaDotTblAliasPair
   *        a map contain <full alias as panthera_schema_table, current alias>
   * @param expectedAliasToAliasNodePair
   *        <String, CommonTree>
   *        a map contain <origin table name/alias, full alias> and <shema.table, schema.table>
   * @param tempTblList
   *        list of CommonTree already refreshed in ON-clause
   * @throws SqlXlateException
   *        same tables join without alias defined
   */
  private void getSchemaDotTblAliasPair(CommonTree node, Map<String,
      String> schemaDotTblAliasPair, Map<String, CommonTree> expectedAliasToAliasNodePair,
      List <CommonTree> tempTblList) throws SqlXlateException {
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
            // create alias for schema.table as panthera_schema_table
            String tblName = tableViewName.getChild(1).getText();
            // alias name as pantehra_schema_table
            aliasName = PantheraConstants.PANTHERA_PREFIX + name + "_" + tblName;
            CommonTree tableAlias = FilterBlockUtil.createAlias(node, aliasName);
            if (expectedAliasToAliasNodePair.get(tblName) != null) {
              expectedAliasToAliasNodePair.remove(tblName);
              // the val CommonTree no longer start with PantheraConstants.PANTHERA_PREFIX
              // this case the previous table with same name should use full alias.
              expectedAliasToAliasNodePair.put(tblName, (CommonTree) tableViewName.getChild(1));
            } else {
              // when simplify full alias to origin name, direct refresh the CommonTree, it is the alias node
              expectedAliasToAliasNodePair.put(tblName, (CommonTree) tableAlias.getChild(0));
            }
            if (expectedAliasToAliasNodePair.get(name + "." + tblName) != null) {
              throw new SqlXlateException(tableViewName, "ambiguously table defined");
            } else {
              // check if dup, and will not result in simplify as val text not start with PantheraConstants.PANTHERA_PREFIX
              CommonTree id = FilterBlockUtil.createSqlASTNode(tableViewName, PantheraParser_PLSQLParser.ID, name + "." + tblName);
              // use XX.XXX to avoid schema name contains '_', not cause exception
              expectedAliasToAliasNodePair.put(name + "." + tblName, id);
            }
            SqlXlateUtil.addCommonTreeChild(node, 0, tableAlias);
            schemaDotTblAliasPair.put(aliasName, aliasName);
          } else {
            // direct table, no schema
            if (expectedAliasToAliasNodePair.get(name) != null) {
              expectedAliasToAliasNodePair.remove(name);
              // this case the previous table with same name should use full alias.
              expectedAliasToAliasNodePair.put(name, (CommonTree) tableViewName.getChild(0));
            }
          }
        } else if (type == PantheraParser_PLSQLParser.SELECT_MODE) {
          // for select_mode, there would not exists schema.table format
        } else {
          return;
        }
      } else if (type == PantheraParser_PLSQLParser.TABLE_REF) {
        // TODO: join operator: from t1, t2, (t3 join t4) ...
        // currently Panthera doesn't support this format.
      }
      return;
    } else if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ON) {
      substituteAnyElementOn(node, schemaDotTblAliasPair, expectedAliasToAliasNodePair, tempTblList);
      return;
    }
    // recurse for all children
    for (int i = 0; i < node.getChildCount(); i++) {
      getSchemaDotTblAliasPair((CommonTree) node.getChild(i), schemaDotTblAliasPair, expectedAliasToAliasNodePair, tempTblList);
    }
  }

  /**
   * check whether there is schema.table.column in the query (under anyElement node)
   *
   * @param tree
   * @param schemaTblAnyEleList
   * @throws SqlXlateException
   */
  private void existsSchemaDotTable(CommonTree tree, List<CommonTree> schemaTblAnyEleList) throws SqlXlateException {
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
   * @return
   * @throws SqlXlateException
   */
  private List<CommonTree> refreshSchemaTbl(List<CommonTree> anyElementList,
      Map<String, String> schemaTblAliasPair) throws SqlXlateException {
    // anyElementList should never be empty.
    List<CommonTree> tempTblList = new ArrayList<CommonTree>();
    for (CommonTree anyElement : anyElementList) {
      assert (anyElement.getChildCount() > 2);
      String schemaDotTable = PantheraConstants.PANTHERA_PREFIX + anyElement.getChild(0).getText() + "_"
          + anyElement.getChild(1).getText();
      String alias = schemaTblAliasPair.get(schemaDotTable);
      if (alias != null) {
        CommonTree table = FilterBlockUtil.createSqlASTNode(anyElement,
            PantheraParser_PLSQLParser.ID, alias);
        tempTblList.add(table);
        anyElement.replaceChildren(0, 1, table);
      }
    }
    return tempTblList;
  }

  private List<CommonTree> refreshTbl(List<CommonTree> anyElementList,
      Map<String, CommonTree> expectedAliasToAliasNodePair) throws SqlXlateException {
    // anyElementList should never be empty.
    List<CommonTree> tempTblList = new ArrayList<CommonTree>();
    for (CommonTree anyElement : anyElementList) {
      assert (anyElement.getChildCount() == 2);
      String key = anyElement.getChild(0).getText();
      CommonTree val = expectedAliasToAliasNodePair.get(key);
      if (val != null) {
        if (!val.getText().equals(key)) {
          String alias = val.getText();
          CommonTree table = FilterBlockUtil.createSqlASTNode(anyElement,
              PantheraParser_PLSQLParser.ID, alias);
          tempTblList.add(table);
          anyElement.replaceChildren(0, 0, table);
        } else {
          // do nothing
        }
      }
    }
    return tempTblList;
  }

  /**
   * substitute table.column under ON node with alias.
   * @param node
   * @param schemaTblAliasPair
   * @return
   * @throws SqlXlateException
   */
  private void substituteAnyElementOn(CommonTree node, Map<String, String> schemaTblAliasPair,
      Map<String, CommonTree> expectedAliasToAliasNodePair, List<CommonTree> tempTblList) throws SqlXlateException {
    List<CommonTree> schemaTblAnyEleList = new ArrayList<CommonTree>();
    List<CommonTree> tblAnyEleList = new ArrayList<CommonTree>();
    List<CommonTree> anyElementList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(node, PantheraParser_PLSQLParser.ANY_ELEMENT, anyElementList);
    if (!anyElementList.isEmpty()) {
      for (CommonTree anyElement : anyElementList) {
        if (anyElement.getChildCount() > 2) {
          schemaTblAnyEleList.add(anyElement);
        } else if (anyElement.getChildCount() == 2) {
          tblAnyEleList.add(anyElement);
        }
      }
    }
    if (!schemaTblAnyEleList.isEmpty()) {
      tempTblList.addAll(refreshSchemaTbl(schemaTblAnyEleList, schemaTblAliasPair));
    }
    if (!tblAnyEleList.isEmpty()) {
      tempTblList.addAll(refreshTbl(tblAnyEleList, expectedAliasToAliasNodePair));
    }
  }

  /**
   * substitute schema.table.column under AnyElement with alias.
   * @param node
   * @param schemaTblAliasPair
   * @throws SqlXlateException
   */
  private void substituteAnyElement(CommonTree node, Map<String, String> schemaTblAliasPair) throws SqlXlateException {
    List<CommonTree> anyElementList = new ArrayList<CommonTree>();
    existsSchemaDotTable(node, anyElementList);
    if (!anyElementList.isEmpty()) {
      refreshSchemaTbl(anyElementList, schemaTblAliasPair);
    }
  }

  /**
   * for select schema.table.* case.
   * substitute schema.table under TableViewName node with alias.
   */
  private void substituteTableViewName(CommonTree node, Map<String, String> schemaTblAliasPair) throws SqlXlateException {
    List<CommonTree> tableViewNameList = new ArrayList<CommonTree>();
    findNodeWithoutBranch(node, PantheraParser_PLSQLParser.TABLEVIEW_NAME, tableViewNameList,
        PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    if (!tableViewNameList.isEmpty()) {
      for (CommonTree tableViewName : tableViewNameList) {
        if (tableViewName.getChildCount() == 2) {
          String schemaDotTable = PantheraConstants.PANTHERA_PREFIX + tableViewName.getChild(0).getText() + "_"
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
