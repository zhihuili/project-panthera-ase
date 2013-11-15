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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor;

import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

/**
 * Process correlated NOT IN in WHERE subquery<br>
 * NotInProcessor4C.
 *
 */
public class NotInProcessor4C extends CommonFilterBlockProcessor {
  private static final Log LOG = LogFactory.getLog(NotInProcessor4C.class);

  @Override
  void processFB() throws SqlXlateException {


    CommonTree leftIn = super.getSubQOpElement();
    buildAnyElement(leftIn, topSelect);

    CommonTree rightIn = (CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST)).getChild(0).getChild(0).getChild(0);
    buildAnyElement(rightIn, bottomSelect);

    CommonTree equal = FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.EQUALS_OP, "=");
    equal.addChild(leftIn);
    equal.addChild(rightIn);

    CommonTree where = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE);
    if (where == null) {
      where = FilterBlockUtil.createSqlASTNode(equal, PantheraExpParser.SQL92_RESERVED_WHERE, "where");
      CommonTree logicEx = FilterBlockUtil.createSqlASTNode(equal, PantheraExpParser.LOGIC_EXPR, "LOGIC_EXPR");
      where.addChild(logicEx);
    }
    CommonTree logicExpr = (CommonTree) where.getChild(0);
    CommonTree oldCondition = (CommonTree) logicExpr.deleteChild(0);
    if (oldCondition != null) {
      CommonTree and = FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.SQL92_RESERVED_AND, "and");
      logicExpr.addChild(and);
      and.addChild(oldCondition);
      and.addChild(equal);

      fb.setASTNode(and);
    } else {
      logicExpr.addChild(equal);

      fb.setASTNode(equal);
    }

    LOG.info("Transform NOT IN to NOT EXIST:"
        + where.toStringTree().replace('(', '[').replace(')', ']'));

    CommonTree selectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    int index = selectList.getChildIndex();
    bottomSelect.deleteChild(index);
    SqlXlateUtil.addCommonTreeChild(bottomSelect, index, FilterBlockUtil.createSqlASTNode(
        subQNode, PantheraExpParser.ASTERISK, "*"));

    /**
     * NOT IN correlated subQ will transformed into NOT EXISTS correlated subQ
     * IMPORTANT: The following two line use the two method to handle NOT EXISTS Correlated subQ
     * IMPORTANT: Here is a switch to use either method. Just Comment one line will use another method.
     * "processNotExistsCByLeftJoin" function use the method like faceBook transformed TPC-H Q21 and Q22
     * "processNotExistsC" use the method in ASE design document, which will transformed into EXISTS and use MINUS to reduce the result.
     */
    //super.processNotExistsCByLeftJoin();
    super.processNotExistsC();
  }

  void buildAnyElement(CommonTree leftIn, CommonTree select) {
    if (leftIn.getType() == PantheraExpParser.CASCATED_ELEMENT) {
      CommonTree anyElement = (CommonTree) leftIn.getChild(0);
      if (anyElement.getChildCount() == 1) {
        CommonTree topFrom = (CommonTree) select
            .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_FROM);
        CommonTree tableRefElement = (CommonTree) topFrom.getChild(0).getChild(0);
        CommonTree tableName;
        if (tableRefElement.getChildCount() > 1) {
          tableName = FilterBlockUtil.cloneTree((CommonTree) tableRefElement.getChild(0)
              .getChild(0));
        } else {
          tableName = FilterBlockUtil.cloneTree((CommonTree) tableRefElement.getChild(0)
              .getChild(0).getChild(0).getChild(0));
        }
        SqlXlateUtil.addCommonTreeChild(anyElement, 0, tableName);
      }
    }
  }
}
