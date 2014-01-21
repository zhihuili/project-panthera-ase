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
 * Process uncorrelated ANY in WHERE subquery<br>
 * AnyProcessor4UC.
 *
 */
public class AnyProcessor4UC extends CommonFilterBlockProcessor {
  private static final Log LOG = LogFactory.getLog(AnyProcessor4UC.class);

  @Override
  void processFB() throws SqlXlateException {

    CommonTree leftCp = super.getSubQOpElement();
    // check if leftCp is from the upper by 1 level.
    CommonTree sq = fbContext.getSelectStack().pop();
    if (PLSQLFilterBlockFactory.getInstance().isCorrelated(this.fbContext.getqInfo(),
        this.fbContext.getSelectStack(), leftCp) != 0) {
      throw new SqlXlateException(leftCp, "not support element from outter query as compare sub-query node");
    }
    fbContext.getSelectStack().push(sq);
    super.buildAnyElement(leftCp, topSelect, false);

    if (bottomSelect.getFirstChildWithType(PantheraExpParser.SELECT_LIST).getChildCount() > 1) {
      throw new SqlXlateException((CommonTree) bottomSelect.getFirstChildWithType(PantheraExpParser
          .SELECT_LIST), "compare subQuery select-list should have only one column");
    }
    CommonTree rightCp = FilterBlockUtil.cloneTree((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST)).getChild(0).getChild(0).getChild(0));
    // rightCp do not have to check level
    super.buildAnyElement(rightCp, bottomSelect, true);

    CommonTree compare = FilterBlockUtil.dupNode(subQNode);
    compare.addChild(leftCp);
    compare.addChild(rightCp);

    fb.setASTNode(compare);

    LOG.info("Transform ANY to EXISTS:"
        + compare.toStringTree().replace('(', '[').replace(')', ']'));

    FilterBlockUtil.speedUpSelect(bottomSelect, context);

    super.processExistsC();
  }

}
