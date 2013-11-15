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

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class CharStringGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    String prepare = currentSqlNode.getText();
    String pcopy = prepare;
    boolean qflag = false;
    boolean nflag = false;
    int i = 0;
    if(prepare.charAt(i)=='N' || prepare.charAt(i)=='n') {
      //default quoting mechanism
      i++;
      nflag = true;
    }
    if(prepare.charAt(i)=='Q' || prepare.charAt(i)=='q') {
      //literal quoting mechanism
      i++;
      qflag = true;
    }
    if (qflag) {
      if(checkDelimiter(prepare.charAt(i+1), prepare.charAt(prepare.length()-2))) {
        pcopy = "\'" + prepare.substring(i+2, prepare.length()-2) + "\'";
      } else {
        throw new SqlXlateException(currentSqlNode, "unmatched delimiter in literal quoting");
      }
    } else {
      pcopy = prepare.substring(i).replaceAll("\'\'", "\\\\\'").replaceAll(";", "\\;");
    }
    ASTNode ret = SqlXlateUtil.newASTNode(HiveParser.StringLiteral, nflag ? "n" + pcopy : pcopy);

    //
    // Special handling for Date value expression.
    // If the immediate preceding sibling node is SQL92_RESERVED_DATE, then attach the
    // newly created StringLiteral HIVE AST node to the TOK_FUNCTION HIVE AST node which is
    // the last child of the current HIVE node at present.
    //
    int childIndex = currentSqlNode.getChildIndex();
    if (childIndex > 0) {
      CommonTree parent = (CommonTree) currentSqlNode.getParent();
      assert (parent != null);

      CommonTree preSibling = (CommonTree) parent.getChild(childIndex - 1);
      if (preSibling.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_DATE) {
        int childCount = currentHiveNode.getChildCount();
        assert (childCount > 0);
        ASTNode node = (ASTNode) currentHiveNode.getChild(childCount - 1);
        assert(node.getType() == HiveParser.TOK_FUNCTION && node.getChildCount() == 1);
        assert(((ASTNode) node.getChild(0)).getType() == HiveParser.TOK_DATE);
        attachHiveNode(hiveRoot, node, ret);
        return true;
      }
    }

    super.attachHiveNode(hiveRoot, currentHiveNode, ret);
    return true;
  }

  /*
   * check if the quoto_delimiter is the same character or
   * a pair of () {} <> [] according to pl sql doc
   * */
  private boolean checkDelimiter(char open, char close) {
    if (open == close) {
      return true;
    } else if (open == '(' && close == ')' || open == '{' && close == '}'
        ||open == '[' && close == ']' || open == '<' && close == '>') {
      return true;
    } else {
      return false;
    }
  }

}
