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
package org.apache.hadoop.hive.ql.parse.sql.generator.text;

import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.PantheraMap;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class TextGeneratorFactory {
  private static int SQLASTTypeNumber = 1000;
  private static Map<Integer, QueryTextGenerator> genMap = new PantheraMap<QueryTextGenerator>(
      SQLASTTypeNumber);

  static {
    // register generator
    genMap.put(PantheraParser_PLSQLParser.SELECT_STATEMENT, new NothingTextGenerator());
    // genMap.put(PantheraParser_PLSQLParser.EXPLAIN_STATEMENT, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.STATEMENTS, new StatementsTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXPLAIN_STATEMENT, new NothingTextGenerator());//?
    genMap.put(PantheraParser_PLSQLParser.SUBQUERY, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, new SelectTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.TABLE_REF, new NothingTextGenerator());
    //No more t1,t2 after transform, so no comma in table_ref.
    genMap.put(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, new ReverseNothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.TABLE_EXPRESSION, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.DIRECT_MODE, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.TABLEVIEW_NAME, new NameTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ID, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SELECT_LIST, new ListTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SELECT_ITEM, new SelectItemTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXPR, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXPR_LIST, new ListTextGenerator());//?
    genMap.put(PantheraParser_PLSQLParser.CASCATED_ELEMENT, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ANY_ELEMENT, new NameTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.LOGIC_EXPR, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OP, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.LESS_THAN_OP, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.NOT_EQUAL_OP, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.UNSIGNED_INTEGER, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.APPROXIMATE_NUM_LIT, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXACT_NUM_LIT, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.NATIONAL_CHAR_STRING_LIT, new CharStringTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.CHAR_STRING_PERL, new QCharStringTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.CHAR_STRING, new CharStringTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_NULL, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_FALSE, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.DOT_ASTERISK, new DotAsteriskTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP, new GroupTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.GROUP_BY_ELEMENT, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.STANDARD_FUNCTION, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.COUNT_VK, new FuncTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.TRIM_VK, new FuncTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ASTERISK, new AsteriskTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER, new OrderTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ORDER_BY_ELEMENTS, new ListTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ORDER_BY_ELEMENT, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.PARTITION_VK, new PartitionTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ALIAS, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.FUNCTION_ENABLING_OVER, new FuncTextGenerator()); // ordinary functions like sum, avg, etc.
    genMap.put(PantheraParser_PLSQLParser.FUNCTION_ENABLING_WITHIN_OR_OVER, new FuncTextGenerator()); // rank() function
    genMap.put(PantheraParser_PLSQLParser.FIRST_VALUE_VK, new FuncTextGenerator()); // windowing function first_value
    genMap.put(PantheraParser_PLSQLParser.LAST_VALUE_VK, new FuncTextGenerator());  // windowing function last_value
    genMap.put(PantheraParser_PLSQLParser.OVER_VK, new OverTextGenerator());  // over keyword for windowing function
    genMap.put(PantheraParser_PLSQLParser.ARGUMENTS, new ListTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ARGUMENT, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_OR, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_AND, new BiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_NOT, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.EQUALS_OP, new EasyBiOpTextGenerator());
    genMap.put(PantheraExpParser.EQUALS_NS, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ROUTINE_CALL, new RoutineCallTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ROUTINE_NAME, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ON, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SELECT_MODE, new SelectModeTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SIMPLE_CASE, new SimpleCaseTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_WHEN, new WhenTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ELSE, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_LIKE, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.PLUS_SIGN, new EasyBiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.MINUS_SIGN, new BiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SOLIDUS, new BiOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.UNARY_OPERATOR, new UnaryOperatorTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.JOIN_DEF, new JoinTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_UNION, new UnionTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.IS_NOT_NULL, new IsNotNullTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.IS_NULL, new IsNullTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ALL, new FuncTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.INNER_VK, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.FULL_VK, new OuterJoinTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.LEFT_VK, new OuterJoinTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.RIGHT_VK, new OuterJoinTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.CROSS_VK, new NaiveTextGenerator());
    genMap.put(PantheraExpParser.LEFTSEMI_VK, new LeftSemiTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ASC, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_DESC, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.MODEL_EXPRESSION, new ModelExpressionTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_IN, new InTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.NOT_IN, new NotInTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_INSERT, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SINGLE_TABLE_MODE, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_INTO, new IntoTextGenerator());
    //
    // Just ignore EXTRACT_VK node because the first child of this node is the datetime field
    // (year | month | day | hour | minute | second) or time zone field (not supported yet. just
    // passed through into hive and error is expected from hive) and the field will be translated
    // into the corresponding UDF name in the RegularIdRebuilder.
    //
    genMap.put(PantheraParser_PLSQLParser.EXTRACT_VK, new ExtractTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.REGULAR_ID, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SUBSTRING_VK, new MulFuncTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_DATE, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_BETWEEN, new BetweenTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.ROWS_VK, new RootFirstTextGenerator());  // rows keyword, for window specification.
    genMap.put(PantheraParser_PLSQLParser.ROW_VK, new RootFirstTextGenerator());  // row keyword, used in window function related.  e.g. "current row"
    genMap.put(PantheraParser_PLSQLParser.PRECEDING_VK, new RootLastTextGenerator());  // preceding
    genMap.put(PantheraParser_PLSQLParser.FOLLOWING_VK, new RootLastTextGenerator());  // following
    genMap.put(PantheraParser_PLSQLParser.UNBOUNDED_VK, new RootFirstTextGenerator());  // unbounded
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_CURRENT, new RootFirstTextGenerator()); // current
    genMap.put(PantheraExpParser.LIMIT_VK, new RootFirstTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SEARCHED_CASE, new SearchedCaseTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.NOT_LIKE, new NotLikeTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.CAST_VK, new CastTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.NATIVE_DATATYPE, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.INT_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.CHAR_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.STRING_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.BOOLEAN_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.DOUBLE_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.FLOAT_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.PRECISION, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.DECIMAL_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.DEC_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.TIMESTAMP_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SMALLINT_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.YEAR_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.MONTH_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.DAY_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.HOUR_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.MINUTE_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.SECOND_VK, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.LEFT_PAREN, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.RIGHT_PAREN, new NaiveTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.CONCATENATION_OP, new ConcatenationOpTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.CUSTOM_TYPE, new NothingTextGenerator());
    genMap.put(PantheraParser_PLSQLParser.TYPE_NAME, new NothingTextGenerator());
  }

  private TextGeneratorFactory() {
  }

  public static QueryTextGenerator getTextGenerator(CommonTree node) throws SqlXlateException {
    QueryTextGenerator textGenerator = genMap.get(node.getType());
    if (textGenerator == null) {
      throw new SqlXlateException(node, "Undefine TextGenerator for SQL AST node type:" + node.getType());
    }
    return textGenerator;
  }
}
