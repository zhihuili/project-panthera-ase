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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 *
 * SQLMagicUtil.Translate SQL(92) to HQL.
 *
 */
public class SQLMagicUtil {

  public static SQLMagicResult translateSQL(String sql) {
    SQLMagicResult result = new SQLMagicResult();

    try {
      HiveConf conf = new HiveConf(Driver.class);
      SessionState.start(conf);
      Context ctx;
      ctx = new Context(conf);
      SqlParseDriver pd = new SqlParseDriver(conf);
      try {
        pd.parse(sql, ctx);
        result.setResult(SqlTextSession.get() == null ? sql : SqlTextSession.get());
      } catch (Exception e) {
        result.setResult(e.getMessage());
        result.setSyntaxError(true);
      }
    } catch (Exception e1) {
      result.setResult(e1.getMessage());
      result.setSystemError(true);
    } finally {
      SqlTextSession.clean();
    }

    return result;
  }

  public static class SQLMagicResult {
    String result;
    boolean systemError;
    boolean syntaxError;

    public String getResult() {
      return result;
    }

    public void setResult(String result) {
      this.result = result;
    }

    public boolean isSystemError() {
      return systemError;
    }

    public void setSystemError(boolean systemError) {
      this.systemError = systemError;
    }

    public boolean isSyntaxError() {
      return syntaxError;
    }

    public void setSyntaxError(boolean syntaxError) {
      this.syntaxError = syntaxError;
    }


  }
}
