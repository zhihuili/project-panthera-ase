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
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.PLSQLFilterBlockFactory;

/**
 * Process correlated ALL in WHERE subquery<br>
 * AllProcessor4C.
 *
 */
public class AllProcessor4C extends CommonFilterBlockProcessor {
  private static final Log LOG = LogFactory.getLog(AllProcessor4C.class);

  @Override
  void processFB() throws SqlXlateException {

    CommonTree leftCp = super.getSubQOpElement();
    CommonTree keepTop = FilterBlockUtil.cloneTree(topSelect);
    // check if leftCp is from the upper by 1 level.
    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), leftCp) != 0) {
      throw new SqlXlateException(leftCp, "not support element from outter query as compare sub-query node");
    }
    fbContext.getSelectStack().push(sq);
    CommonTree oldLeft = FilterBlockUtil.cloneTree(leftCp);
    super.buildAnyElement(leftCp, topSelect, false);

    if (bottomSelect.getFirstChildWithType(PantheraExpParser.SELECT_LIST).getChildCount() > 1) {
      throw new SqlXlateException((CommonTree) bottomSelect.getFirstChildWithType(PantheraExpParser
          .SELECT_LIST), "compare subQuery select-list should have only one column");
    }
    CommonTree rightCp = FilterBlockUtil.cloneTree((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST)).getChild(0).getChild(0).getChild(0));
    CommonTree oldRight = FilterBlockUtil.cloneTree(rightCp);
    // rightCp do not have to check level
    super.buildAnyElement(rightCp, bottomSelect, true);

    CommonTree reverseCompare = FilterBlockUtil.reverseClone(subQNode);
    assert (reverseCompare != null);
    reverseCompare.addChild(leftCp);
    reverseCompare.addChild(rightCp);

    CommonTree topand = FilterBlockUtil.createSqlASTNode(subQNode, PantheraExpParser.SQL92_RESERVED_AND, "and");
    topand.addChild(reverseCompare);
    topand.addChild(fb.getASTNode());
    fb.setASTNode(topand);

    LOG.info("Transform ALL to NOT EXISTS:"
        + topand.toStringTree().replace('(', '[').replace(')', ']'));

    FilterBlockUtil.speedUpSelect(bottomSelect, context);
    CommonTree keepBottom = FilterBlockUtil.cloneTree(bottomSelect);

    // first not exists, not exists the reversed condition
    super.processNotExistsC();

    // if bottomSelect contains NULL, that means ALL requires a column to compare with NULL
    // always false
    // use a copy, keepBottom still need later
    bottomSelect = FilterBlockUtil.cloneTree(keepBottom);
    CommonTree isNull = FilterBlockUtil.createSqlASTNode(reverseCompare, PantheraExpParser.IS_NULL, "IS_NULL");
    isNull.addChild(oldRight);
    FilterBlockUtil.addConditionToSelect(bottomSelect, isNull);

    // second not exists, not exists NULL in ALL-SubQuery
    super.processNotExistsUC();

    // if topSelect contains NULL
    // if bottomSelect generates nothing, keep this NULL
    // else not keep the NULL
    bottomSelect = keepBottom;
    super.buildNaiveAnyElement(oldLeft);
    super.processAllWindUp(oldLeft, keepTop);
  }

}
