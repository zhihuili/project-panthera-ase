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
package org.apache.hadoop.hive.ql.parse.sql.transformer;

import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

/**
 * Do something before or after transform.
 * BaseSqlASTTransformer.
 *
 */
public abstract class BaseSqlASTTransformer implements SqlASTTransformer {
  private static final Log LOG = LogFactory.getLog(BaseSqlASTTransformer.class);


  @Override
  public void transformAST(CommonTree tree, TranslateContext context) throws SqlXlateException {
    String myself = this.getClass().getSimpleName();
    long begin = System.currentTimeMillis();

    transform(tree, context);

    long end = System.currentTimeMillis();

    // performance log
    LOG.info(myself + " spend time(ms):" + (end - begin));

    // track log
    QueryInfo qf = context.getQInfoRoot();
    LOG.info("After " + myself + ", sql ast is:"
        + tree.toStringTree().replace('(', '[').replace(')', ']'));
    LOG.info("After " + myself + ", sql ast type is:"
        + SqlXlateUtil.toTypeStringTree(tree));
    try {
      LOG.info("After " + myself + ", query info is:"
          + (qf == null ? "null" : qf.toStringTree().replace('(', '[').replace(')', ']')));
    } catch (Exception e) {
      LOG.info("After " + myself + ", query info is: oops!");
    }
    LOG.info("After " + myself + ", filterBlock is:"
        + (qf == null ? "null" : qf.toFilterBlockStringTree().replace('(', '[').replace(')', ']')));
  }

  protected abstract void transform(CommonTree tree, TranslateContext context)
      throws SqlXlateException;

}
