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


import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 *
 * Contains utility methods used during Sql Translate process.
 *
 */
public final class SqlXlateUtil {

  private static final Log LOG = LogFactory.getLog("hive.ql.parse.sql.SqlXlateUtil");



  /**
   * Create a new Hive ASTNode from type and text
   *
   * @param ttype
   *          Token type
   * @param text
   *          text
   * @return ASTNode
   */
  public static ASTNode newASTNode(int ttype, String text) {
    ASTNode n = new ASTNode(new org.antlr.runtime.CommonToken(ttype, text));
    LOG.debug("creating ASTNode :" + n.toString());
    return n;
  }

  /**
   * Create a new Hive ASTNode from token
   *
   * @param token
   *          token
   * @return ASTNode
   */
  public static ASTNode newASTNode(org.antlr.runtime.Token token) {
    ASTNode n = new ASTNode(token);
    LOG.debug("creating ASTNode :" + n.toString());
    return n;
  }

  /**
   * Copy create a new Hive AST node from another ASTNode
   *
   * @param other
   * @return
   */
  public static ASTNode newASTNode(ASTNode other) {
    return newASTNode(other.getToken());
  }

  public static SqlASTNode newSqlASTNode(SqlASTNode src) {
    return new SqlASTNode(new CommonToken(src.getToken()));
  }

  /**
   * Wrapper to throw Unsupported grammar error
   *
   * @param src
   *          error was found when processing which SqlASTNode
   * @throws SqlXlateException
   */
  public static void error(SqlASTNode src) throws SqlXlateException {
    LOG.error("Unsupported grammar starting at :" + src.toStringTree());
    throw new SqlXlateException((CommonTree) src, "Error when Transformaing PLSQL AST node - type:"
        + src.getToken().getType() + ", text:" + src.getText());
  }

  public static boolean isLogicalOp(SqlASTNode op) {
    int type = op.getType();
    return (type == PantheraParser_PLSQLParser.SQL92_RESERVED_AND
        || type == PantheraParser_PLSQLParser.SQL92_RESERVED_OR || type == PantheraParser_PLSQLParser.SQL92_RESERVED_NOT);
  }

  /**
   * Check if is relational Operator
   *
   * @param op
   * @return
   */
  public static boolean isRelationalOperator(SqlASTNode op) {
    int type = op.getType();
    return (type == PantheraParser_PLSQLParser.EQUALS_OP
        || type == PantheraParser_PLSQLParser.NOT_EQUAL_OP
        || type == PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP
        || type == PantheraParser_PLSQLParser.LESS_THAN_OP
        || type == PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP || type == PantheraParser_PLSQLParser.GREATER_THAN_OP);
  }

  public static boolean isLiteral(SqlASTNode node) {
    int type = node.getType();
    return (type == PantheraParser_PLSQLParser.CHAR_STRING_PERL
        || type == PantheraParser_PLSQLParser.CHAR_STRING
        || type == PantheraParser_PLSQLParser.EXACT_NUM_LIT
        || type == PantheraParser_PLSQLParser.UNSIGNED_INTEGER
        || type == PantheraParser_PLSQLParser.NATIONAL_CHAR_STRING_LIT
        || type == PantheraParser_PLSQLParser.APPROXIMATE_NUM_LIT
        || type == PantheraParser_PLSQLParser.SQL92_RESERVED_NULL
        || type == PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE
        || type == PantheraParser_PLSQLParser.SQL92_RESERVED_FALSE || (type == PantheraParser_PLSQLParser.ID && (node
        .getText().startsWith("'") || node.getText().startsWith("\""))));

  }

  public static boolean isAllOperator(SqlASTNode op) {
    return (op.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL);
  }

  public static boolean isSomeAnyOperator(SqlASTNode op) {
    return (op.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY || op.getType() == PantheraParser_PLSQLParser.SOME_VK);
  }

  /**
   * Check whether there's node of type tokenType in subtree
   *
   * @param node
   *          root of the AST subtree
   * @return whether there's a node of type tokenType
   */
  public static boolean hasNodeTypeInTree(CommonTree node, int tokenType) {
    if (node.getType() == tokenType) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasNodeTypeInTree((CommonTree) node.getChild(i), tokenType)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check whether there's node of type tokenType in subtree
   *
   * @param node
   *          root of the AST subtree
   * @return whether there's a node of type tokenType
   */
  public static <T> boolean hasNodeTypeInTree(ASTNode node, int tokenType) {
    if (node.getType() == tokenType) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasNodeTypeInTree((ASTNode) node.getChild(i), tokenType)) {
        return true;
      }
    }
    return false;
  }

  public static void changeNodeToken(SqlASTNode src, int type, String text) {
    src.getToken().setType(type);
    src.getToken().setText(text);
  }

  /**
   * Collect all src table name and aliase pairs in from clause.
   *
   * Note that src table name and aliase within any sub-query won't be included.
   *
   * If a table name has an alias, return the <alias, name> pair. If only table name, then return
   * <name, name> pair. For a sub-query, return<alias, null> pair.
   *
   * Note that when this function is called, QueryInfo must have been prepared so that sub-quries in
   * from clause have alias node generated if it is not existing.
   *
   * @param n
   *          root of SQL AST subtree
   * @param collection
   *          result set
   */
  public static void getSrcTblAliasNamePair(CommonTree n, Map<String, String> srcTbls) {
    // We need only process all top TABLE_REF_ELEMENT nodes of the children of the from node.
    //
    if (n.getType() == PantheraParser_PLSQLParser.TABLE_REF_ELEMENT) {
      String alias = null;
      String name = null;
      int index = 0;
      if (n.getChild(0).getType() == PantheraParser_PLSQLParser.ALIAS) {
        alias = n.getChild(0).getChild(0).getText();
        index = 1;
      }
      int type = n.getChild(index).getType();
      if (type == PantheraParser_PLSQLParser.TABLE_EXPRESSION) {
        type = n.getChild(index).getChild(0).getType();
        if (type == PantheraParser_PLSQLParser.DIRECT_MODE) {
          CommonTree tableViewName = (CommonTree) n.getChild(index).getChild(0).getChild(0);
          name = tableViewName.getChild(0).getText();
          if (tableViewName.getChildCount() > 1) {
            // schema.table
            // for HIVE 0.9, 'name' should be in format of "schema.table"
            name += ("." + tableViewName.getChild(1).getText());
            // for HIVE version higher than HIVE 0.9.0, 'name' should be in format of "table"
            // Please refer to HIVE-2721, which is committed on Apr 26th, 2012 in apache/hive github.
            //name = tableViewName.getChild(1).getText();
          }
          if (alias == null) {
            alias = name;
          }
        } else if (type == PantheraParser_PLSQLParser.SELECT_MODE) {
          if (alias == null) {
            // Note that when this function is called, QueryInfo must have been prepared so that
            // sub-quries in from clause have alias node generated if it is not existing.
            assert (false);
          }
        } else {
          return;
        }

        if (!srcTbls.containsKey(alias)) {
          srcTbls.put(alias, name);
        }
      } else if (type == PantheraParser_PLSQLParser.TABLE_REF) {
        // TBD: may need special handling for join operator: (a join b ...) join_alias
      }
      return;
    }

    // recurse for all children
    for (int i = 0; i < n.getChildCount(); i++) {
      getSrcTblAliasNamePair((CommonTree) n.getChild(i), srcTbls);
    }
  }

  /**
   * Collect all src table names or aliases in from clause.
   *
   * If a table has an alias, then its alias instead of its name is returned.
   *
   * Note that src table name and aliase within any sub-query won't be included.
   *
   * @param n
   *          root of SQL AST subtree
   * @param collection
   *          result set
   */
  public static void getSrcTblAlias(CommonTree n, Set<String> srcTblAliases) {
    Map<String, String> srcTbls = new HashMap<String, String>();
    getSrcTblAliasNamePair(n, srcTbls);
    for (String alias : srcTbls.keySet()) {
      srcTblAliases.add(alias);
    }
  }

  /**
   * Collect all src table names and aliases in from clause.
   *
   * If a table has an alias, then both its alias and name are returned.
   *
   * Note that src table name and aliase within any sub-query won't be included.
   *
   * @param n
   *          root of SQL AST subtree
   * @param collection
   *          result set
   */
  public static void getSrcTblAndAlias(CommonTree n, Set<String> srcTblAliases) {
    Map<String, String> srcTbls = new HashMap<String, String>();
    getSrcTblAliasNamePair(n, srcTbls);
    for (String alias : srcTbls.keySet()) {
      srcTblAliases.add(alias);
      String name = srcTbls.get(alias);
      if (name != null && !name.equals(alias)) {
        srcTblAliases.add(name);
      }
    }
  }

  /**
   * Collect all table names and aliases referred (normally in filter and select items)
   *
   * @param n
   *          root of SQL AST subtree
   * @param referedTbls
   *          result set
   */
  public static void getReferredTblAlias(SqlASTNode n, Set<String> referedTbls) {
    if (n.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      if (n.getChildCount() == 2) {
        // TODO should add schema support
        referedTbls.add(n.getChild(0).getText());
      }
    }
    for (int i = 0; i < n.getChildCount(); i++) {
      getReferredTblAlias((SqlASTNode) n.getChild(i), referedTbls);
    }
  }

  /**
   * Get SQL92_RESERVED_WHERE node from the subtree of SQL92_RESERVED_SELECT
   *
   * @param selectKey
   * @return
   */
  public static SqlASTNode getWhereInSelectRaw(SqlASTNode selectKey) {
    assert (selectKey.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    return (SqlASTNode) selectKey
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
  }

  /**
   * Get SELECT_LIST node from the subtree of SQL92_RESERVED_SELECT
   *
   * @param selectKey
   * @return
   */
  public static SqlASTNode getSelectListInSelectRaw(SqlASTNode selectKey) {
    assert (selectKey.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    SqlASTNode selectList = (SqlASTNode) selectKey
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    if (selectList == null) {
      return (SqlASTNode) selectKey.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK);
    }
    return selectList;
  }


  /**
   * Get SQL92_RESERVED_FROM node from the subtree of SQL92_RESERVED_SELECT
   *
   * @param selectKey
   * @return
   */
  public static SqlASTNode getFromInSelectRaw(SqlASTNode selectKey) {
    assert (selectKey.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    return (SqlASTNode) selectKey
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
  }

  /**
   * Get SQL92_RESERVED_GROUP node from subtree of select key
   *
   * @param selectKey
   * @return
   */
  public static SqlASTNode getGroupKeyInSelect(SqlASTNode selectKey) {
    assert (selectKey.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT);
    return (SqlASTNode) selectKey
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
  }

  /**
   * check if table ref expression is direct mode
   */
  public static boolean isTableExprDirectMode(SqlASTNode tableExpression) {
    assert (tableExpression.getType() == PantheraParser_PLSQLParser.TABLE_EXPRESSION);
    SqlASTNode directMode = (SqlASTNode) tableExpression
        .getFirstChildWithType(PantheraParser_PLSQLParser.DIRECT_MODE);
    if (directMode != null) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check if subtree represents select count(*)
   *
   * @param select
   * @return
   */
  public static boolean isSelectCountDistinctStar(ASTNode select) {
    ASTNode selexpr = (ASTNode) select.getChild(0);
    if (selexpr.getChild(0).getType() != HiveParser.TOK_FUNCTIONDI) {
      return false;
    }
    ASTNode functiondi = (ASTNode) selexpr.getChild(0);
    if (functiondi.getChildCount() == 1
        && functiondi.getChild(0).getType() == HiveParser.Identifier
        && functiondi.getChild(0).getText() == "count") {
      return true;
    }

    return false;
  }

  public static boolean isSelectCountStar(SqlASTNode selectItem) {
    SqlASTNode expr = null;
    if ((expr = (SqlASTNode) selectItem.getFirstChildWithType(PantheraParser_PLSQLParser.EXPR)) != null) {
      SqlASTNode standardFunction = null;
      if ((standardFunction = (SqlASTNode) expr
          .getFirstChildWithType(PantheraParser_PLSQLParser.STANDARD_FUNCTION)) != null) {
        SqlASTNode count = null;
        if ((count = (SqlASTNode) standardFunction
            .getFirstChildWithType(PantheraParser_PLSQLParser.COUNT_VK)) != null) {
          if (count.getFirstChildWithType(PantheraParser_PLSQLParser.ASTERISK) != null) {
            return true;
          }
        }
      }
    }
    return false;
  }



  /**
   * Check if subtree represents select distinct *
   *
   * @param select
   * @return
   */
  public static boolean isSelectDistinctStar(ASTNode select) {
    if (select.getType() != HiveParser.TOK_SELECTDI) {
      return false;
    }
    if (select.getChildCount() > 1) {
      return false;
    }
    if (select.getChild(0).getType() != HiveParser.TOK_SELEXPR) {
      return false;
    }
    if (select.getChild(0).getChild(0).getType() != HiveParser.TOK_ALLCOLREF) {
      return false;
    }
    return true;
  }

  /**
   * Check if subtree represents select *
   *
   * @param select
   * @return
   */
  public static boolean isSelectStar(ASTNode select) {
    ASTNode selexpr = (ASTNode) select.getFirstChildWithType(HiveParser.TOK_SELEXPR);
    if (selexpr.getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check if it is position order by clause e.g. order by 1
   *
   * @param orderby
   * @return
   */
  public static boolean isOrderByPosition(ASTNode orderby) {
    if (orderby == null) {
      return false;
    }
    for (int i = 0; i < orderby.getChildCount(); i++) {
      String id = orderby.getChild(i).getChild(0).getText();
      if (Pattern.matches("\\d+", id)) {
        // if found any, return true;
        return true;
      }
    }
    return false;
  }

  /**
   * Merge Two Filters with op
   *
   * @param op
   * @param left
   * @param right
   * @return
   */
  public static ASTNode mergeFilters(ASTNode op, ASTNode left, ASTNode right) {
    if (left == null && right == null) {
      return null;
    }
    if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    } else {
      op.addChild(left);
      op.addChild(right);
      return op;
    }
  }

  /**
   * Duplicate subtree for Hive AST
   *
   * @param src
   * @return
   */
  public static ASTNode duplicateSubTree(ASTNode src) {
    ASTNode newNode = SqlXlateUtil.newASTNode(src);
    for (int i = 0; i < src.getChildCount(); i++) {
      newNode.addChild(duplicateSubTree((ASTNode) src.getChild(i)));
    }
    return newNode;
  }

  /**
   * Duplicate subtree for SqlAST
   *
   * @param src
   * @return
   */
  public static SqlASTNode duplicateSubTree(SqlASTNode src) {
    SqlASTNode newNode = SqlXlateUtil.newSqlASTNode(src);
    for (int i = 0; i < src.getChildCount(); i++) {
      newNode.addChild(duplicateSubTree((SqlASTNode) src.getChild(i)));
    }
    return newNode;
  }

  /**
   * Make Table Ref AST piece based on column
   *
   * @param c
   * @return
   */
  public static ASTNode makeASTforColumn(Column c) {
    return makeASTforColumn(c.getTblAlias(), c.getColAlias());
  }

  /**
   * Make Table Ref AST piece based on table alias and column alias
   *
   * @param c
   * @return
   */
  public static ASTNode makeASTforColumn(String tabAlias, String colAlias) {
    ASTNode item = SqlXlateUtil.newASTNode(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");

    if (!tabAlias.isEmpty()) {
      ASTNode dot = SqlXlateUtil.newASTNode(HiveParser.DOT, ".");
      dot.addChild(item);
      item.addChild(SqlXlateUtil.newASTNode(HiveParser.Identifier, tabAlias));
      item = dot;
    }
    item.addChild(SqlXlateUtil.newASTNode(HiveParser.Identifier, colAlias));
    return item;
  }

  /**
   *
   * Class to generate random alias for subquries and columns.
   *
   */
  public static class AliasGenerator {
    private int aliasNum = 0;

    /**
     * Generate an alias for SubQuery
     *
     * @return
     */
    public String generateAliasName() {
      return generateSequenceAlias();
    }

    /**
     * Generate alias with sequence number<br>
     *
     * @return
     */
    private String generateSequenceAlias() {
      return "panthera_" + getAliasNum();
    }

    /**
     * Generate alias with random string
     *
     * @return
     */
    private String generateRandomAlias() {
      // TODO generate random string now
      // later we will check for conflicts
      return "gen_" + RandomStringUtils.randomAlphanumeric(12);
    }

    private synchronized int getAliasNum() {
      return aliasNum++;
    }
  }



  /**
   * Get table alias name from tab ref tree
   *
   * @param node
   * @return
   */
  public static String getTblAliasNameFromTabRef(ASTNode node) {
    // return the first tableRef or subq alias found
    if (node.getType() == HiveParser.TOK_SUBQUERY) {
      return node.getChild(1).getText();
    } else if (node.getType() == HiveParser.TOK_TABNAME) {
      if (node.getChildCount() == 1) {
        return node.getChild(0).getText();
      } else {
        return node.getChild(1).getText();
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      String tab = getTblAliasNameFromTabRef((ASTNode) node.getChild(i));
      if (tab != null) {
        return tab;
      }
    }
    return null;
  }

  /**
   *
   * Class to retrieve table meta data information in Hive.
   *
   */
  public static class HiveMetadata {
    protected final Hive db;
    protected final HiveConf conf;
    protected HashMap<String, RowResolver> tblRRMap;

    public HiveMetadata(HiveConf conf) throws SqlXlateException {
      try {
        // prevent from calling
        this.conf = conf;
        db = Hive.get(conf);
        tblRRMap = new HashMap<String, RowResolver>();
      } catch (HiveException e) {
        throw new SqlXlateException(null, "HiveException thrown : " + e);
      }
    }

    /**
     * Get meta data object for table
     *
     * @param tabName
     * @return
     * @throws SqlXlateException
     */
    private Table getMetaDataForAlias(String tabName) throws SqlXlateException {
      try {
        return db.getTable(tabName);
      } catch (HiveException e) {
        throw new SqlXlateException(null, "HiveException thrown : " + e);
      }
    }

    /**
     * Get meta data object for table
     *
     * @param tabName
     * @return
     * @throws SqlXlateException
     */
    private Table getMetaDataForAlias(String dbName, String tabName) throws SqlXlateException {
      try {
        return db.getTable(dbName, tabName);
      } catch (HiveException e) {
        throw new SqlXlateException(null, "HiveException thrown : " + e);
      }
    }

    /**
     * Get Row Resolve for Table
     *
     * @param dbName
     * @param tblName
     * @return
     * @throws SqlXlateException
     */
    public RowResolver getRRForTbl(String dbName, String tblName) throws SqlXlateException {
      try {
        String tbl = getFullTblName(dbName, tblName);
        if (tblRRMap.containsKey(tbl)) {
          return tblRRMap.get(tbl);
        } else {
          Table tab = getMetaDataForAlias(dbName, tblName);
          if (tab == null) {
            return null;
          }
          RowResolver rr = getRRForTblInternal(tab);
          tblRRMap.put(tblName, rr);
          return rr;
        }
      } catch (HiveException e) {
        throw new SqlXlateException(null, "HiveException thrown : " + e);
      }
    }

    /**
     * Get Row Resolve for Table
     *
     * @param dbName
     * @param tblName
     * @return
     * @throws SqlXlateException
     */
    public RowResolver getRRForTbl(String tblName) throws SqlXlateException {
      try {
        if (tblRRMap.containsKey(tblName)) {
          return tblRRMap.get(tblName);
        } else {
          Table tab = getMetaDataForAlias(tblName);
          RowResolver rr = getRRForTblInternal(tab);
          tblRRMap.put(tblName, rr);
          return rr;
        }
      } catch (HiveException e) {
        throw new SqlXlateException(null, "HiveException thrown : " + e);
      } catch (Exception e) {
        throw new SqlXlateException(null, "Hive metastore excption encountered, tables or columns are not valid!");
      }
    }

    /**
     * Get full name string of table
     *
     * @param dbName
     * @param tblName
     * @return
     * @throws SqlXlateException
     */
    public String getFullTblName(String dbName, String tblName) {
      if (dbName == null) {
        dbName = SessionState.get().getCurrentDatabase();
      }
      return dbName + "@" + tblName;
    }

    private RowResolver getRRForTblInternal(Table tab) throws HiveException {
      String alias = getFullTblName(tab.getDbName(), tab.getTableName());
      RowResolver rr = new RowResolver();
      try {
        if (tab.isView()) {
          List<FieldSchema> fields = tab.getAllCols();
          for (int i = 0; i < fields.size(); i++) {
            // add fields into row resolver
            // TODO in colInfo we now only add table name as tab alias,
            // fix it later
            rr.put(alias, fields.get(i).getName(), new ColumnInfo(fields.get(i).getName(),
                TypeInfoUtils.getTypeInfoFromTypeString(fields.get(i).getType()), tab
                    .getTableName(), false));
          }
          return rr;
        }

        if (tab.getDeserializer() == null) {
          return null;
        }
        StructObjectInspector rowObjectInspector = (StructObjectInspector) tab.getDeserializer()
            .getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          // add fields into row resolver
          // TODO in colInfo we now only add table name as tab alias,
          // fix it later
          rr.put(alias, fields.get(i).getFieldName(),
              new ColumnInfo(fields.get(i).getFieldName(), TypeInfoUtils
                  .getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector()), tab
                  .getTableName(), false));
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      // add partition into row resolver
      for (FieldSchema part_col : tab.getPartCols()) {
        LOG.trace("Adding partition col: " + part_col);
        // TODO: use the right type by calling part_col.getType() instead of
        // String.class
        rr.put(alias, part_col.getName(), new ColumnInfo(part_col.getName(),
            TypeInfoFactory.stringTypeInfo, tab.getTableName(), true));
      }
      return rr;
    }
  }

  public static boolean isJoinOp(ASTNode op) {
    if (op.getType() == HiveParser.TOK_JOIN || op.getType() == HiveParser.TOK_CROSSJOIN
        || op.getType() == HiveParser.TOK_LEFTOUTERJOIN
        || op.getType() == HiveParser.TOK_RIGHTOUTERJOIN
        || op.getType() == HiveParser.TOK_FULLOUTERJOIN
        || op.getType() == HiveParser.TOK_LEFTSEMIJOIN) {
      return true;
    } else {
      return false;
    }

  }

  /**
   * exchange left & right branch<br>
   * if only one branch, no effect.
   *
   * @param branch
   */
  public static void exchangeChildrenPosition(CommonTree branch) {
    CommonTree left = (CommonTree) branch.deleteChild(0);
    branch.addChild(left);
  }

  public static boolean containTableName(String tableName, CommonTree node) {
    Set<String> srcTblAliases = new HashSet<String>();
    getSrcTblAlias(node, srcTblAliases);
    return srcTblAliases.contains(tableName);
  }





  public static String toTypeStringTree(org.antlr.runtime.tree.Tree tree) {

    StringBuilder sb = new StringBuilder();
    toTypeStringBuilder(sb, tree);
    return sb.toString();
  }

  private static void toTypeStringBuilder(StringBuilder sb, org.antlr.runtime.tree.Tree tree) {

    sb.append(" ");
    if (tree.getChildCount() > 0) {
      sb.append("[");
    }

    sb.append(tree.getType());

    if (tree.getChildCount() > 0) {
      for (int i = 0; i < tree.getChildCount(); i++) {
        toTypeStringBuilder(sb, tree.getChild(i));
      }
      sb.append("]");
    }

  }

  /**
   * add CommonTree node to parent's index position
   *
   * @param parent
   * @param index
   * @param child
   */
  public static void addCommonTreeChild(CommonTree parent, int index, CommonTree child) {
    if (parent == null || child == null) {
      return;
    }
    if (parent.getChildren() == null && index == 0) {
      parent.addChild(child);
    } else {
      parent.getChildren().add(index, child);
      child.setParent(parent);
      child.setChildIndex(index);
      parent.freshenParentAndChildIndexes(index + 1);
    }
  }

  public static int getASTNodeChildIndex(ASTNode parent, ASTNode child) {
    List<Node> children = parent.getChildren();
    for (int i = 0; i < children.size(); i++) {
      if (child == children.get(i)) {
        return i;
      }
    }
    return -1;
  }

  // check whether the query is panthera supported.
  // Panthera can support SELECT Statement, EXPLAIN PLAN FOR SELECT Statement, INSERT INTO <table> SELECT Statement.
  public static void checkPantheraSupportQueries(Object tree) throws HiveParseException {
    if (!((((CommonTree)tree).getType() ==  PantheraParser_PLSQLParser.STATEMENTS)
        && (((CommonTree)tree).getChild(0).getType() == PantheraParser_PLSQLParser.SELECT_STATEMENT)
        || (((CommonTree)tree).getType() ==  PantheraParser_PLSQLParser.STATEMENTS)
        && (((CommonTree)tree).getChild(0).getType() == PantheraParser_PLSQLParser.EXPLAIN_STATEMENT)
        && (((CommonTree)tree).getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.SELECT_STATEMENT)
        || (((CommonTree)tree).getType() ==  PantheraParser_PLSQLParser.STATEMENTS)
        && (((CommonTree)tree).getChild(0).getType() ==  PantheraParser_PLSQLParser.SQL92_RESERVED_INSERT)
        && (((CommonTree)tree).getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.SINGLE_TABLE_MODE)
        && (((CommonTree)tree).getChild(0).getChild(0).getChildCount() == 2)
        && (((CommonTree)tree).getChild(0).getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_INTO)
        && (((CommonTree)tree).getChild(0).getChild(0).getChild(1).getType() == PantheraParser_PLSQLParser.SELECT_STATEMENT))){
      throw new HiveParseException("Illegal queries for Panthera");
    }
  }
}
