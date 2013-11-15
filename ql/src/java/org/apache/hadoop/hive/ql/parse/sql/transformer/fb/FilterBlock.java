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
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

/**
 * FilterBlocks are facility data structures used to breakdown and transform the filters in
 * WHERE/HAVING clauses. <br>
 * Since subqueries in select expression can be transformed in similar way as filters in
 * WHERE/HAVING, <br>
 * we also use FilterBlocks to represent subquery in select expression.<br>
 * Each FilterBlock object may represent one of the following semantic unit <br>
 * <li>a filter (filter as defined in the previous page) <li>a logical relationship operator (i.e.
 * and/or/not) <li>a query (select... ­from... )<br>
 * FilterBlocks in the same QueryInfo scope forms a Tree structure.<br>
 * Each QueryInfo refers to only one FilterBlock Tree. <br>
 * Each FB Tree can be processed independently and results are assembled to form the complete result
 * AST.<br>
 *
 * FilterBlock.
 *
 */
public interface FilterBlock {
  List<FilterBlock> getChildren();

  void addChild(FilterBlock fb);

  void addAllChildren(List<FilterBlock> fbl);

  FilterBlock getParent();

  void setParent(FilterBlock fb);

  void setASTNode(CommonTree node);

  CommonTree getASTNode();

  void setTransformedNode(CommonTree node);

  CommonTree getTransformedNode();

  String toStringTree();

  void setTransformed();

  boolean hasTransformed();

  /**
   * Nested Subqueries are transformed to inline view with various joins<br>
   * <li>Use equi-joins for all equal conditions<br>
   * Left semi join, Left outer join (if null is involved), Inner join <li>Use cross join for some
   * of the non-equal conditions <li>Use select distinct with consideration of _rowid on the result
   * of outer/cross/inner join. Left Semi-joins are fine. Logical Relationships between Filters are
   * either absorbed or transformed to Set Operations <li>NOT: "not" operator is eliminated from the
   * tree using logical mathematics <li>OR: "or" operator is transformed to "UNION" operator with
   * consideration of _rowid <li>AND: "and" operator is transformed to "INTERSECT" operator with
   * consideration of _rowid Correlated Filters need special treatment according to semantics <li>
   * Most correlated filters find its positions in join conditions <li>Syntax Transformation Results
   * of Correlated Filters and Uncorrelated Filters are often different. E.g. ANY/SOME/ALL <li>We
   * don¡¯t support a nested query referencing columns other than its own source or its direct
   * parent¡¯s source (Only one-level up reference is allowed for correlated filters). The
   * Transformations are ordered <li>First, Unnest subqueries in Where conditions <li>Second, Unnest
   * subquries in select expressions <li>Third, Unest subqueries in Having conditions <li>Subquery
   * in select expressions does not coexist with groupBy/having clauses. Subquery in Having clause
   * need special treatment <li>Typically, unnesting subquery in where clause need only to change
   * FROM clause and WHERE condition<br>
   * E.g. select a, b from t1 where b in (select x from t2) ? select a, b from t1 left semi join
   * (select x as col1 from t2) subq1 on b = subq1.col1 <li>Unnesting subquery in having clause have
   * to be done after groupby is performed. So an enclosing query is needed for that.<br>
   * E.g. select a, sum(b) from t1 group by a having sum(b) in (select x from t2) ? select
   * subq2.col1, subq2.col2 from (select a as col1, sum(b) as col2 from t1 group by a) subq2 left
   * semi join (select x as col1 from t2) subq1 on subq2.col2 = subq1.col1<br>
   *
   * @param fbContext
   * @param context
   * @Inherited
   */
  void process(FilterBlockContext fbContext, TranslateContext context) throws SqlXlateException;

  void prepare(FilterBlockContext fbContext, TranslateContext context, Stack<CommonTree> selectStack) throws SqlXlateException;
}
