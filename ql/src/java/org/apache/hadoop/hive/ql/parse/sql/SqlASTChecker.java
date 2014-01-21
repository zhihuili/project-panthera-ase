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

import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class SqlASTChecker {
  public SqlASTErrorNode errorNode = null;
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.sql.SqlParseDriver");

  /**
   * check whether the AST Tree is legal for Panthera
   * @param tree
   * @param originCmd
   *        the origin query
   * @throws SqlParseException
   *         need to use Hive run again, throw SqlParseException
   * @throws SqlXlateException
   *         if do not want to use Hive to run again, throw SqlXlateException
   *         currently, only ID start with PantheraConstants.PANTHERA_PREFIX would throw SqlXlateException
   */
  public void checkSqlAST(Object tree, String originCmd) throws SqlParseException, SqlXlateException{
    if (tree instanceof SqlASTNode) {
      if (((CommonTree) tree).getType() == PantheraParser_PLSQLParser.ID
          //ID begin with PantheraConstants.PANTHERA_PREFIX
          && ((CommonTree) tree).getText().startsWith(PantheraConstants.PANTHERA_PREFIX)) {
        //when SqlXlateException encountered, Panthera would not use Hive to run the queries again
        throw new SqlXlateException((CommonTree) tree, "Table/Column name/alias begin with \"" +
            PantheraConstants.PANTHERA_PREFIX + "\" is reserved by " + PantheraConstants.PANTHERA_ASE);
      }
      try {
        checkSupport((CommonTree) tree);
      } catch (SqlXlateException e) {
        LOG.error("SQL parse error: " + e.toString());
        e.outputException(originCmd);
        throw new SqlParseException(e.getMessage());
      }
      for (int i=0; i < ((SqlASTNode) tree).getChildCount(); i++) {
        ((SqlASTNode) tree).token.setCharPositionInLine(((SqlASTNode) tree).getCharPositionInLine());
        ((SqlASTNode) tree).token.setLine(((SqlASTNode) tree).getLine());
        checkSqlAST( ((SqlASTNode) tree).getChild(i), originCmd);
      }
    } else if (tree instanceof SqlASTErrorNode) {
      errorNode = (SqlASTErrorNode) tree;
      //FIXME better output here
      throw new SqlParseException("Uncompleted input");
    } else {
      throw new SqlParseException("Unknown error: " + tree.toString());
    }
  }

  /**
   *
   * @param tree
   * @throws SqlXlateException
   *         if can get node position info, throw SqlXlateException
   */
  private void checkSupport(CommonTree tree) throws SqlXlateException {
    switch(tree.getType()) {
    case PantheraParser_PLSQLParser.ID:
      // handle "limit"
      // select * from (select s_grade,s_city from staff limit 10)t1; this query will pass PL_SQL Parser
      if (tree.getText().equals("limit")) {
        throw new SqlXlateException(tree, "\"limit\" is reserved by Panthera, not supported");
      }
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT:
      //select in select-list
      if(tree.hasAncestor(PantheraParser_PLSQLParser.SELECT_LIST)) {
        //when SqlXlateException encountered, Panthera would not use Hive to run the queries again.
        throw new SqlXlateException(tree, "Currently Panthera don't support subQuery in select-list!");
      }
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      //exists condition
      //TODO
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_DATE:
    case PantheraParser_PLSQLParser.REGULAR_ID:
      //date-interval
      LOG.info("Warning: Panthera only provide limited support for date interval type");
      break;
    case PantheraParser_PLSQLParser.SEARCHED_CASE:
      //case in where-clause not support
      //TODO
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_ALL:
      //all not support empty set
      LOG.info("Warning: Panthera may output wrong result when the subquery in all generates an empty set");
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      //in (a set contains value null)
      LOG.info("Warning: Panthera may output wrong result when the subquery result set contains null");
      break;
    case PantheraParser_PLSQLParser.OVER_VK:
      //all window functions are not supported when there is subqueries currently
      throw new SqlXlateException(tree, "Currently Panthera don't support window functions!");
    default:
    }
    return ;
  }
}
