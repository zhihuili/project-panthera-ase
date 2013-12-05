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
package org.apache.hadoop.hive.ql.parse.sql.generator;


import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.PantheraMap;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Create generator by node(type), generator is singleton instance.<br>
 * If add generator, should register it in the factory.<br>
 * GeneratorFactory.
 *
 */
public class GeneratorFactory {

  private static int SQLASTTypeNumber = 1000;
  private static Map<Integer, HiveASTGenerator> genMap = new PantheraMap<HiveASTGenerator>(
      SQLASTTypeNumber);
  // pre&post generator could have pre&post generator too.
  private static Map<Integer, HiveASTGenerator> preGenMap = new PantheraMap<HiveASTGenerator>(
      SQLASTTypeNumber);
  private static Map<Integer, HiveASTGenerator> postGenMap = new PantheraMap<HiveASTGenerator>(
      SQLASTTypeNumber);

  static {
    // register pre-generator
  }

  static {
    // register post-generator
    postGenMap.put(PantheraParser_PLSQLParser.CASCATED_ELEMENT, new PostCascatedElementGenerator());

  }

  static {
    // register generator
    genMap.put(PantheraParser_PLSQLParser.SELECT_STATEMENT, new SelectStatementGenerator());
    // genMap.put(PantheraParser_PLSQLParser.EXPLAIN_STATEMENT, new ErrorGenerator());
    genMap.put(PantheraParser_PLSQLParser.STATEMENTS, new StatementsGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXPLAIN_STATEMENT, new ExplainGenerator());
    genMap.put(PantheraParser_PLSQLParser.SUBQUERY, new SubQueryGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, new SelectGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, new FromGenerator());
    genMap.put(PantheraParser_PLSQLParser.TABLE_REF, new TableRefGenerator());
    genMap.put(PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, new TableRefElementGenerator());
    genMap.put(PantheraParser_PLSQLParser.TABLE_EXPRESSION, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.DIRECT_MODE, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.TABLEVIEW_NAME, new TableViewNameGenerator());
    genMap.put(PantheraParser_PLSQLParser.ID, new IdGenerator());
    genMap.put(PantheraParser_PLSQLParser.SELECT_LIST, new SelectListGenerator());
    genMap.put(PantheraParser_PLSQLParser.SELECT_ITEM, new SelectItemGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXPR, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXPR_LIST, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.CASCATED_ELEMENT, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.ANY_ELEMENT, new AnyElementGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, new WhereGenerator());
    genMap.put(PantheraParser_PLSQLParser.LOGIC_EXPR, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OP, new GreaterThanOpGenerator());
    genMap.put(PantheraParser_PLSQLParser.LESS_THAN_OP, new LessThanOpGenerator());
    genMap
        .put(PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP, new LessThanOrEqualsOpGenerator());
    genMap.put(PantheraParser_PLSQLParser.NOT_EQUAL_OP, new NotEqualOpGenerator());
    genMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP,
        new GreaterThanOrEqualsOpGenerator());
    genMap.put(PantheraParser_PLSQLParser.UNSIGNED_INTEGER, new UnsignedIntegerGenerator());
    genMap.put(PantheraParser_PLSQLParser.APPROXIMATE_NUM_LIT, new UnsignedIntegerGenerator());
    genMap.put(PantheraParser_PLSQLParser.EXACT_NUM_LIT, new UnsignedIntegerGenerator());
    genMap.put(PantheraParser_PLSQLParser.NATIONAL_CHAR_STRING_LIT, new CharStringGenerator());
    genMap.put(PantheraParser_PLSQLParser.CHAR_STRING_PERL, new CharStringGenerator());
    genMap.put(PantheraParser_PLSQLParser.CHAR_STRING, new CharStringGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_NULL, new NullGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE, new TrueGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_FALSE, new FalseGenerator());
    genMap.put(PantheraParser_PLSQLParser.DOT_ASTERISK, new DotAsteriskGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP, new GroupGenerator());
    genMap.put(PantheraParser_PLSQLParser.GROUP_BY_ELEMENT, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.STANDARD_FUNCTION, new StandardFunctionGenerator());
    genMap.put(PantheraParser_PLSQLParser.COUNT_VK, new CountGenerator());
    genMap.put(PantheraParser_PLSQLParser.TRIM_VK, new TrimGenerator());// maybe could reuse count^
    genMap.put(PantheraParser_PLSQLParser.ASTERISK, new AsteriskGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.ORDER_BY_ELEMENTS, new OrderByElementsGenerator());
    genMap.put(PantheraParser_PLSQLParser.ORDER_BY_ELEMENT, new OrderByElementGenerator());
    genMap.put(PantheraParser_PLSQLParser.PARTITION_VK, new PartitionGenerator());  // partition keyword
    genMap.put(PantheraParser_PLSQLParser.ALIAS, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.FUNCTION_ENABLING_OVER,
        new FunctionEnablingOverGenerator());
    genMap.put(PantheraParser_PLSQLParser.FUNCTION_ENABLING_WITHIN_OR_OVER,  // rank() type is FUNCTION_ENABLING_WITHIN_OR_OVER
        new FunctionEnablingOverGenerator());
    genMap.put(PantheraParser_PLSQLParser.FIRST_VALUE_VK, new FunctionEnablingOverGenerator()); // windowing function first_value(...)
    genMap.put(PantheraParser_PLSQLParser.LAST_VALUE_VK, new FunctionEnablingOverGenerator());  // windowing function last_value(...)
    genMap.put(PantheraParser_PLSQLParser.OVER_VK, new OverGenerator());  // over keyword, e.g. select min(col1) over() from ...
    genMap.put(PantheraParser_PLSQLParser.ARGUMENTS, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.ARGUMENT, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING, new HavingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_OR, new OrGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_AND, new AndGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_NOT, new NotGenerator());
    genMap.put(PantheraParser_PLSQLParser.EQUALS_OP, new EqualsOpGenerator());
    genMap.put(PantheraExpParser.EQUALS_NS, new EqualsNsGenerator());
    genMap.put(PantheraParser_PLSQLParser.ROUTINE_CALL, new StandardFunctionGenerator());
    genMap.put(PantheraParser_PLSQLParser.ROUTINE_NAME, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ON, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SELECT_MODE, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SIMPLE_CASE, new SimpleCaseGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_WHEN, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ELSE, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_LIKE, new LikeGenerator());
    genMap.put(PantheraParser_PLSQLParser.PLUS_SIGN, new PlusSignGenerator());
    genMap.put(PantheraParser_PLSQLParser.MINUS_SIGN, new MinusSignGenerator());
    genMap.put(PantheraParser_PLSQLParser.SOLIDUS, new SolidusGenerator());
    genMap.put(PantheraParser_PLSQLParser.UNARY_OPERATOR, new UnaryOperatorGenerator());
    genMap.put(PantheraParser_PLSQLParser.JOIN_DEF, new JoinDefGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_UNION, new UnionGenerator());
    genMap.put(PantheraParser_PLSQLParser.IS_NOT_NULL, new IsNotNullGenerator());
    genMap.put(PantheraParser_PLSQLParser.IS_NULL, new IsNullGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ALL, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.INNER_VK, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.FULL_VK, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.LEFT_VK, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.RIGHT_VK, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.CROSS_VK, new NothingGenerator());
    genMap.put(PantheraExpParser.LEFTSEMI_VK, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_ASC, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_DESC, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_DISTINCT, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.MODEL_EXPRESSION, new ModelExpressionGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_IN, new InGenerator());
    genMap.put(PantheraParser_PLSQLParser.NOT_IN, new NotInGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_INSERT, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.SINGLE_TABLE_MODE,  new SingleTableModeGenerator());
    //
    // Just ignore EXTRACT_VK node because the first child of this node is the datetime field
    // (year | month | day | hour | minute | second) or time zone field (not supported yet. just
    // passed through into hive and error is expected from hive) and the field will be translated
    // into the corresponding UDF name in the RegularIdGenerator.
    //
    genMap.put(PantheraParser_PLSQLParser.EXTRACT_VK, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.REGULAR_ID, new RegularIdGenerator());
    genMap.put(PantheraParser_PLSQLParser.SUBSTRING_VK, new SubstringFuncGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_DATE, new DateGenerator());
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_BETWEEN, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.ROWS_VK, new RowsGenerator());  // rows keyword, used in over clause.
    genMap.put(PantheraParser_PLSQLParser.ROW_VK, new NothingGenerator()); // row keyword, e.g. "current row"
    genMap.put(PantheraParser_PLSQLParser.PRECEDING_VK, new PrecedingGenerator()); // preceding
    genMap.put(PantheraParser_PLSQLParser.FOLLOWING_VK, new FollowingGenerator()); // following
    genMap.put(PantheraParser_PLSQLParser.UNBOUNDED_VK, new UnboundedGenerator()); // unbounded
    genMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_CURRENT, new CurrentGenerator());
    genMap.put(PantheraExpParser.LIMIT_VK, new LimitGenerator());
    genMap.put(PantheraParser_PLSQLParser.SEARCHED_CASE, new SearchedCaseGenerator());
    genMap.put(PantheraParser_PLSQLParser.NOT_LIKE, new NotLikeGenerator());
    genMap.put(PantheraParser_PLSQLParser.CAST_VK, new CastGenerator());
    genMap.put(PantheraParser_PLSQLParser.NATIVE_DATATYPE, new NothingGenerator());
    genMap.put(PantheraParser_PLSQLParser.INT_VK, new IntGenerator());
    genMap.put(PantheraParser_PLSQLParser.CHAR_VK, new CharGenerator());
    genMap.put(PantheraParser_PLSQLParser.PRECISION, new PrecisionGenerator());
    genMap.put(PantheraParser_PLSQLParser.DECIMAL_VK, new DecimalGenerator());
    genMap.put(PantheraParser_PLSQLParser.DEC_VK, new DecimalGenerator());

    genMap.put(PantheraParser_PLSQLParser.CONCATENATION_OP, new ConcatenationOpGenerator());
  }

  static {
    for (int i = 0; i < genMap.size(); i++) {
      HiveASTGenerator generator = genMap.get(i);
      if (generator != null) {
        generator.setHivePreGenerator(preGenMap.get(i));
        generator.setHivePostGenerator(postGenMap.get(i));
      }
    }
  }

  private GeneratorFactory() {
  }

  public static HiveASTGenerator getGenerator(CommonTree node, int type) throws SqlXlateException {
    HiveASTGenerator generator = genMap.get(type);
    if (generator == null) {
      throw new SqlXlateException(node, "Untransformed SQL AST node type:" + node.getType()
          + " node text:" + node.getText());
    }
    return generator;
  }

  public static HiveASTGenerator getGenerator(CommonTree node) throws SqlXlateException {
    return getGenerator(node, node.getType());
  }

}
