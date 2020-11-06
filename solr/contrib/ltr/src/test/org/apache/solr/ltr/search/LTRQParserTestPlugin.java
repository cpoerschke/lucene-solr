/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.ltr.search;

import java.util.Random;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;

/**
 * A LTRQParserPlugin that has predictable randomness for use in tests.
 */
public class LTRQParserTestPlugin extends LTRQParserPlugin {

  private static Random RANDOM;

  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random(); // TODO: Forbidden method invocation: java.util.Random#<init>() [Use RandomizedRunner's random() instead]
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new LTRQParser(qstr, localParams, params, req) {
      protected Random getRandom() {
        return LTRQParserTestPlugin.RANDOM;
      }
    };
  }

  public static void setRANDOM(Random RANDOM) {
    LTRQParserTestPlugin.RANDOM = RANDOM;
  }

}
