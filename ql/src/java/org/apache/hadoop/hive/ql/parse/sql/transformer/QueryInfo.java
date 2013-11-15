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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


/**
 * A facility class that keeps information needed for processing.
 *
 * QueryInfo is something similar to Hive's QB structure. For each
 * subquery/query encountered, a new QueryInfo object is created
 * and information is collected within this subquery.
 * Each SqlASTNode has a reference to a QueryInfo object, and all
 * SqlASTNode within the same subquery refers to the same QueryInfo
 * object.
 *
 * The QueryInfo objects of the entire query is organized as a
 * simple tree. Each QueryInfo object has a reference to its
 * parent QueryInfo object. The top QueryInfo is a dummy node.
 * The next layer are the top-level queries. Next next... layers
 * are nested children subqueries.
 *
 * Note that here subquery only refers to subqueries inside
 * top-level query's from clause. Subqueries in other places
 * (e.g. filters) are handled by other data structure
 * (i.e. SubQFilterBlock).
 *
 */
public class QueryInfo {
  /**
   * reference to the parent QueryInfo object
   */
  private QueryInfo parentQInfo;

  private final List<QueryInfo> children = new ArrayList<QueryInfo>();
  /**
   * Each select and its sub nodes forms a subquery.
   * this is the root node of this subquery.
   * The node is of type SQL92_RESERVED_SELECT
   */
  private CommonTree selectKey;
  /**
   * The insert destinations for all children selections
   * Because of the structure of SQL AST tree, insert destinations
   * are located in the parent qInfo of the select queries.
   */
  private Set<CommonTree> insertDestinationsForSubQ;
  /**
   * The from clause in this select query
   */
  private CommonTree from;
  /**
   * Map from select node of a subquery to alias node.
   * alias is either randomly generated or extracted from
   * the original SQL AST.
   */
  private HashMap<CommonTree, ASTNode> subqToAlias;
  /**
   * The root of filter block tree for this query.
   */
  private FilterBlock rootFilterBlock;
  /**
   * Map from select key to src table or aliases referred in that select query
   */
  private final HashMap<CommonTree, Set<String>> selectKeyToSrcTblAlias = new HashMap<CommonTree, Set<String>>();;
  /**
   * Map from select key to src table  alias-name pair referred in that select query
   */
  private final Map<CommonTree, Map<String, String>> selectKeyToSrcTblAliasNamePair = new HashMap<CommonTree, Map<String, String>>();
  /**
   * Map from select or from key to row info.
   */
  private final Map<Integer, List<Column>> keyToRowInfo = new HashMap<Integer, List<Column>>();

  // alias or column in SELECT_LIST
  private final Map<CommonTree, List<String>> selectLists = new HashMap<CommonTree, List<String>>();


  public List<String> getSelectList() {
    return getSelectListForSelectKey(selectKey);
  }

  public List<String> getSelectListForSelectKey(CommonTree select) {
    List<String> selectList = selectLists.get(select);
    if (selectList == null) {
      selectList = new ArrayList<String>();
      selectLists.put(select, selectList);
    }
    return selectList;
  }

  /**
   *
   * Class to Represent A Column.
   *
   */
  public static class Column {
    /** table name */
    String tblName;
    /** column name */
    String colName;

    /**
     * Constructor
     */
    public Column() {
    }

    /**
     * Constructor
     *
     * @param tname
     *          tablename
     * @param cname
     *          column name
     */
    public Column(String tname, String cname) {
      tblName = tname;
      colName = cname;
    }

    public void setTblAlias(String tname) {
      tblName = tname;
    }

    public String getTblAlias() {
      return tblName;
    }

    public void setColAlias(String cname) {
      colName = cname;
    }

    public String getColAlias() {
      return colName;
    }

    @Override
    public String toString() {
      if (tblName == null || tblName.isEmpty()) {
        return colName;
      } else {
        return tblName + "." + colName;
      }
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Column)) {
        return false;
      }
      return tblName.equals(((Column) other).tblName) &&
            colName.equals(((Column) other).colName);
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }
  }

  /**
   * Constructor
   */
  public QueryInfo() {

  }

  /**
   * Set filter block tree root
   *
   * @param root
   */
  public void setFilterBlockTreeRoot(FilterBlock root) {
    this.rootFilterBlock = root;
  }

  /**
   * Get filter block tree root
   *
   * @return
   */
  public FilterBlock getFilterBlockTreeRoot() {
    return rootFilterBlock;
  }

  /**
   * Map select node of a subquery to alias node.
   *
   * @param subq
   *          select node of a subquery
   * @param alias
   *          ASTNode Identifier
   */
  public void setSubQAlias(CommonTree subq, ASTNode alias) {
    if (subqToAlias == null) {
      subqToAlias = new HashMap<CommonTree, ASTNode>();
    }
    subqToAlias.put(subq, alias);
  }

  /**
   * Get mapped alias of select node of a subquery.
   *
   * @param subq
   *          select node of a subquery
   * @return alias ASTNode
   */
  public ASTNode getSubQAlias(SqlASTNode subq) {
    if (subqToAlias == null) {
      return null;
    }
    return subqToAlias.get(subq);
  }

  /**
   * Map an alias to its associated subquery.
   *
   * @param alias
   *          table alias
   * @return select node of the subquery.
   */
  public CommonTree GetSuQFromAlias(String tblAlias) {
    for (Map.Entry<CommonTree, ASTNode> subQAliasMapEntry : subqToAlias.entrySet()) {
      if (subQAliasMapEntry.getValue().getText().equals(tblAlias)) {
        return subQAliasMapEntry.getKey();
      }
    }
    return null;
  }

  /**
   * Add one insert destination.
   *
   * @param into
   *          SqlASTNode INTO node
   */
  public void addInsertDestinationForSubQ(SqlASTNode into) {
    if (insertDestinationsForSubQ == null) {
      insertDestinationsForSubQ = new HashSet<CommonTree>();
    }
    insertDestinationsForSubQ.add(into);
  }

  /**
   * Get all insert destinations
   *
   * destinations may be more than one (if multi-table insert)
   *
   * @return the into node set
   */
  public Set<CommonTree> getInsertDestinationsForSubQ() {
    return insertDestinationsForSubQ;
  }

  /**
   * Get the insert destinations for this query.
   * It is retrieved from parent QueryInfo object.
   *
   * @return
   */
  public Set<CommonTree> getInsertDestinationsForThisQuery() {
    // get the destination from parent
    if (hasParentQueryInfo()) {
      return parentQInfo.getInsertDestinationsForSubQ();
    }
    return null;
  }

  /**
   * Setter method for from clause of this query.
   *
   * @param from
   *          SqlASTNode SQL92_RESERVED_FROM
   */
  public void setFrom(CommonTree from) {
    this.from = from;
  }

  /**
   * Getter method for from clause of this query
   *
   * @return SqlASTNode SQL92_RESERVED_FROM
   */
  public CommonTree getFromClauseForThisQuery() {
    return from;
  }

  /**
   * Setter for parent qInfo.
   *
   * @param parent
   *          parent QInfo
   */
  public void setParentQueryInfo(QueryInfo parent) {
    parentQInfo = parent;
  }

  /**
   * Getter for parent qInfo
   *
   * @return parent qInfo
   */
  public QueryInfo getParentQueryInfo() {
    return parentQInfo;
  }

  /**
   * Check whether this has parent QInfo
   *
   * @return
   */
  public boolean hasParentQueryInfo() {
    return (parentQInfo != null);
  }

  /**
   * Getter for whether this is the root of QInfo Tree
   * If this qInfo doesn't have any parent
   * then it's qInfo tree Root
   *
   * @return
   */
  public boolean isQInfoTreeRoot() {
    return !hasParentQueryInfo();
  }

  /**
   * Set select key of this query
   *
   * @param selectStat
   *          SqlASTNode SQL92_RESERVED_SELECT
   */
  public void setSelectKeyForThisQ(CommonTree selectStat) {
    this.selectKey = selectStat;
  }

  /**
   * Get select key of this query
   *
   * @return
   */
  public CommonTree getSelectKeyForThisQ() {
    return selectKey;
  }

  /**
   * Get all the source tables alias-name pairs referred in from clause
   * in this select query.
   *
   * @return the map of table alias-name pair
   */
  public Map<String, String> getSrcTblAliasNamePairForSelectKey(CommonTree select) {
    Map<String, String> srcTbls = selectKeyToSrcTblAliasNamePair.get(select);
    if (srcTbls == null) {
      srcTbls = new LinkedHashMap<String, String>();
      SqlXlateUtil.getSrcTblAliasNamePair((CommonTree) select
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM), srcTbls);
      selectKeyToSrcTblAliasNamePair.put(select, srcTbls);
    }
    return srcTbls;
  }

  public Map<String, String> getSrcTblAliasNamePair() {
    CommonTree select = this.getSelectKeyForThisQ();
    return getSrcTblAliasNamePairForSelectKey(select);
  }

  /**
   * Get all the source tables and aliases referred in from clause
   * in this select query.
   *
   * @return the set of table names and aliases
   */
  public Set<String> getSrcTblAliasForSelectKey(CommonTree select) {
    Set<String> srcTblAlias = selectKeyToSrcTblAlias.get(select);
    if (srcTblAlias == null) {
      srcTblAlias = new HashSet<String>();
      SqlXlateUtil.getSrcTblAlias((CommonTree) select
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM), srcTblAlias);
      selectKeyToSrcTblAlias.put(select, srcTblAlias);
    }
    return srcTblAlias;
  }

  public Set<String> getSrcTblAlias() {
    CommonTree select = this.getSelectKeyForThisQ();
    return getSrcTblAliasForSelectKey(select);
  }

  public List<Column> getRowInfo(CommonTree key) {
    List<Column> rowInfo = keyToRowInfo.get(key.getCharPositionInLine());
    if(rowInfo == null) {
      rowInfo = new ArrayList<Column>();
      keyToRowInfo.put(key.getCharPositionInLine(), rowInfo);
    }
    return rowInfo;
  }

  public List<Column> getSelectRowInfo() {
    CommonTree select = this.getSelectKeyForThisQ();
    return getRowInfo(select);
  }

  public List<Column> getFromRowInfo() {
    CommonTree from = this.getFromClauseForThisQuery();
    return getRowInfo(from);
  }

  public List<QueryInfo> getChildren() {
    return children;
  }

  public void addChild(QueryInfo child) {
    this.children.add(child);
  }

  public boolean hasChildren() {
    if (this.getChildren().size() != 0) {
      return true;
    }
    return false;
  }

  public QueryInfo findChildQueryInfo(CommonTree select) {
    for (QueryInfo qi : children) {
      if (qi.getSelectKeyForThisQ() == select) {
        return qi;
      }
    }
    return null;
  }

  private void buildTree(QueryInfo qf, StringBuilder sb) {
    if (qf.hasChildren()) {
      sb.append("(");
    }
    if (qf.getSelectKeyForThisQ() == null) {
      sb.append("root");
    } else {
      sb.append(qf.getSrcTblAlias().toString().replace(" ", "").replace("[", "").replace("]", ""));
    }
    if (qf.hasChildren()) {
      for (QueryInfo qc : qf.getChildren()) {
        sb.append(' ');
        qc.buildTree(qc, sb);
      }
      sb.append(")");
    }
  }

  public String toStringTree() {
    StringBuilder sb = new StringBuilder();
    this.buildTree(this, sb);
    return sb.toString();
  }

  public String toFilterBlockStringTree() {

    if (children == null || children.size() == 0) {
      return this.getFilterBlockTreeRoot() == null ? null : this.getFilterBlockTreeRoot()
          .toStringTree();
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(this.getFilterBlockTreeRoot() == null ? "root" : this.getFilterBlockTreeRoot()
        .toStringTree());
    sb.append(" ");
    for (int i = 0; i < children.size(); i++) {
      sb.append(children.get(i).getFilterBlockTreeRoot() == null ? null : children.get(i)
          .getFilterBlockTreeRoot().toStringTree());
      sb.append(" ");
    }
    sb.append("]");
    return sb.toString();

  }

}
