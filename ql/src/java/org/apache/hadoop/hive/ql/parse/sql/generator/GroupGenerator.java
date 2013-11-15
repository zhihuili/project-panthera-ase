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

import java.util.Iterator;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class GroupGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws SqlXlateException {

    ASTNode ret = SqlXlateUtil.newASTNode(HiveParser.TOK_GROUPBY, "TOK_GROUPBY");
    super.attachHiveNode(hiveRoot, currentHiveNode, ret);

    Iterator i = currentSqlNode.getChildren().iterator();
    if (i == null) {
      return true;
    }
    while (i.hasNext()) {
      Object o = i.next();
      CommonTree node = (CommonTree) o;
      HiveASTGenerator generator = GeneratorFactory.getGenerator(node);
      if (generator == null) {
        throw new SqlXlateException(node, "Untransformed sql AST node:" + node);
      }
      if (node.getType() == PantheraParser_PLSQLParser.GROUP_BY_ELEMENT) {
        if (!generator.generateHiveAST(hiveRoot, sqlRoot,
            ret, node, context)) {
          return false;
        }
      }
      if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING) {
        if (!generator.generateHiveAST(hiveRoot, sqlRoot,
            currentHiveNode, node, context)) {
          return false;
        }
      }
    }
    return true;


  }

}
