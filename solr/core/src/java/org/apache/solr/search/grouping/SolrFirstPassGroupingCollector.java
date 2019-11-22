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
package org.apache.solr.search.grouping;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.CollectedSearchGroup;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.search.grouping.SearchGroup;

public class SolrFirstPassGroupingCollector<T> extends FirstPassGroupingCollector<T> {

  public SolrFirstPassGroupingCollector(GroupSelector<T> groupSelector, Sort groupSort, int topNGroups) {
    super(groupSelector, groupSort, topNGroups);
  }

  @Override
  protected SearchGroup<T> newSearchGroup() {
    return new SolrSearchGroup<>();
  }

  @Override
  protected SearchGroup<T> newSearchGroupFromCollectedSearchGroup(
      CollectedSearchGroup<T> group,
      FieldComparator<?>[] comparators,
      int sortFieldCount) {

    final SearchGroup<T> searchGroup = super.newSearchGroupFromCollectedSearchGroup(group, comparators, sortFieldCount);
    final SolrSearchGroup<T> solrSearchGroup = (SolrSearchGroup<T>)searchGroup;

    solrSearchGroup.topDocLuceneId = group.topDoc;

    // TODO: this is out-of-date w.r.t. the pull request
    if (sortFieldCount > 0 && comparators[0] instanceof FieldComparator.RelevanceComparator ){
      solrSearchGroup.topDocScore = (Float)comparators[0].value(group.comparatorSlot);
    } else {
      solrSearchGroup.topDocScore = -1;
    }

    return solrSearchGroup;
  }

}
