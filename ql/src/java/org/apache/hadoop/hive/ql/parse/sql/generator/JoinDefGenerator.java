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
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class JoinDefGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {
    ASTNode join;
    if (currentSqlNode.getChild(0).getType() == PantheraParser_PLSQLParser.FULL_VK) {
      join = super.newHiveASTNode(HiveParser.TOK_FULLOUTERJOIN, "TOK_FULLOUTERJOIN");
    } else if (currentSqlNode.getChild(0).getType() == PantheraParser_PLSQLParser.LEFT_VK) {
      join = super.newHiveASTNode(HiveParser.TOK_LEFTOUTERJOIN, "TOK_LEFTOUTERJOIN");
    } else if (currentSqlNode.getChild(0).getType() == PantheraParser_PLSQLParser.RIGHT_VK) {
      join = super.newHiveASTNode(HiveParser.TOK_RIGHTOUTERJOIN, "TOK_RIGHTOUTERJOIN");
    } else if (currentSqlNode.getChild(0).getType() == PantheraParser_PLSQLParser.CROSS_VK) {
      join = super.newHiveASTNode(HiveParser.TOK_CROSSJOIN, "TOK_CROSSJOIN");
    } else if (currentSqlNode.getChild(0).getType() == PantheraExpParser.LEFTSEMI_VK) {
      join = super.newHiveASTNode(HiveParser.TOK_LEFTSEMIJOIN, "TOK_LEFTSEMIJOIN");
    } else {
      join = super.newHiveASTNode(HiveParser.TOK_JOIN, "TOK_JOIN");
    }
    ASTNode firstTableRefElement = (ASTNode) currentHiveNode.deleteChild(0);
    super.attachHiveNode(hiveRoot, join, firstTableRefElement);
    super.attachHiveNode(hiveRoot, currentHiveNode, join);
    currentHiveNode = join;
    return super.generateChildren(hiveRoot, sqlRoot, currentHiveNode, currentSqlNode, context);

  }

}
