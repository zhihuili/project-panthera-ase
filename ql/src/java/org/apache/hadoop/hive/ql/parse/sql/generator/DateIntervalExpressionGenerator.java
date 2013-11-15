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

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Generator for "date value expression +/- interval value expression".
 *
 * Note: this generator is a workaround for TPC-H queries, not a complete support.
 * Supported "interval value expression" is of form:
 * interval 'quoted_string' (year|month|day) <any following elements are ignored>
 *
 */
public class DateIntervalExpressionGenerator extends BaseHiveASTGenerator {
  private static DateIntervalExpressionGenerator instance = new DateIntervalExpressionGenerator();

  //
  // Singeleton
  //
  private DateIntervalExpressionGenerator() {
  }

  public static DateIntervalExpressionGenerator getInstance() {
    return instance;
  }

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    if (currentSqlNode.getChildCount() >= 5
        &&
        ((CommonTree) currentSqlNode.getChild(0)).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_DATE
        &&
        ((CommonTree) currentSqlNode.getChild(2)).getType() == PantheraParser_PLSQLParser.REGULAR_ID
        &&
        ((CommonTree) currentSqlNode.getChild(2)).getText().equalsIgnoreCase("interval")) {
      CommonTree dateStringNode = (CommonTree) currentSqlNode.getChild(1);
      CommonTree intervalStringNode = (CommonTree) currentSqlNode.getChild(3);
      CommonTree intervalUnitNode = (CommonTree) currentSqlNode.getChild(4);
      if (dateStringNode.getType() != PantheraParser_PLSQLParser.CHAR_STRING) {
        throw new SqlXlateException(dateStringNode, "Unsupported date value expression.");
      } else if (intervalStringNode.getType() != PantheraParser_PLSQLParser.CHAR_STRING ||
          (intervalUnitNode.getType() != PantheraParser_PLSQLParser.YEAR_VK &&
              intervalUnitNode.getType() != PantheraParser_PLSQLParser.MONTH_VK &&
          intervalUnitNode.getType() != PantheraParser_PLSQLParser.DAY_VK)) {
        throw new SqlXlateException(intervalUnitNode, "Unsupported interval value expression.");
      }

      Date date;
      try {
        date = Date.valueOf(dateStringNode.getText().replace('\'', ' ').trim());
      } catch (IllegalArgumentException e) {
        throw new SqlXlateException(dateStringNode, "Unsupported date value expression.");
      }

      int intervalValue;
      try {
        intervalValue = Integer.parseInt(intervalStringNode.getText().replace('\'', ' ').trim());
      } catch (NumberFormatException e) {
        throw new SqlXlateException(intervalStringNode, "Unsupported interval value expression.");
      }

      int field;
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      switch (intervalUnitNode.getType()) {
      case PantheraParser_PLSQLParser.YEAR_VK:
        field = Calendar.YEAR;
        break;
      case PantheraParser_PLSQLParser.MONTH_VK:
        field = Calendar.MONTH;
        break;
      default:
        field = Calendar.DAY_OF_YEAR;
        break;
      }

      if (currentSqlNode.getType() == PantheraParser_PLSQLParser.MINUS_SIGN) {
        intervalValue = -intervalValue;
      }
      calendar.add(field, intervalValue);
      String result = formatter.format(calendar.getTime()).toString();

      //
      // Create a HIVE TOK_FUNCTION node and attach it to the current HIVE node.
      //
      ASTNode func = SqlXlateUtil.newASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
      attachHiveNode(hiveRoot, currentHiveNode, func);
      //
      // Create a HIVE TOK_DATE node as the first child of the TOK_FUNCTION node
      //
      ASTNode dateNode = SqlXlateUtil.newASTNode(HiveParser.TOK_DATE, "TOK_DATE");
      attachHiveNode(hiveRoot, func, dateNode);
      //
      // Create a HIVE StringLiteral node as the second child of the TOK_FUNCTION node
      //
      ASTNode stringLiteral = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, "'" + result + "'");
      attachHiveNode(hiveRoot, func, stringLiteral);

      return true;
    }
    return false;
  }

}
