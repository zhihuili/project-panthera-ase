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
package org.apache.hadoop.hive.ql.parse.sql.generator.text;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

/**
 * TextGenerator for "partition" keyword.
 * write down the symbol and then pass down.
 * PartitionTextGenerator.
 *
 */
public class PartitionTextGenerator extends BaseTextGenerator {

  @Override
  protected String textGenerate(CommonTree root, TranslateContext context) throws Exception {
    String ret = "partition by ";
    for (int i = 0; i < root.getChildCount(); i++) {
      // on odd child node, it's "EXPR" node, and on even child node will be the partition column.
      if (i % 2 != 0 && i != 1) {
        ret += ", ";
      }
      CommonTree childNode = (CommonTree) root.getChild(i);
      QueryTextGenerator childGen = TextGeneratorFactory.getTextGenerator(childNode);
      ret += childGen.textGenerateQuery(childNode, context);
    }
    return ret;
  }

}