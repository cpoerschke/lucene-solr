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
package org.apache.solr.search.grouping.distributed.shardresultserializer;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommand;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommandResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation for transforming {@link SearchGroup} into a {@link NamedList} structure and visa versa.
 */
public abstract class SearchGroupsResultTransformer implements ShardResultTransformer<List<Command>, Map<String, SearchGroupsFieldCommandResult>> {

  private static final String TOP_GROUPS = "topGroups";
  private static final String GROUP_COUNT = "groupCount";

  protected final SolrIndexSearcher searcher;

  private SearchGroupsResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  @Override
  public NamedList transform(List<Command> data) throws IOException {
    final NamedList<NamedList> result = new NamedList<>(data.size());
    for (Command command : data) {
      final NamedList<Object> commandResult = new NamedList<>(2);
      if (SearchGroupsFieldCommand.class.isInstance(command)) {
        SearchGroupsFieldCommand fieldCommand = (SearchGroupsFieldCommand) command;
        final SearchGroupsFieldCommandResult fieldCommandResult = fieldCommand.result();
        final Collection<SearchGroup<BytesRef>> searchGroups = fieldCommandResult.getSearchGroups();
        if (searchGroups != null) {
          commandResult.add(TOP_GROUPS, serializeSearchGroup(searchGroups, fieldCommand));
        }
        final Integer groupedCount = fieldCommandResult.getGroupCount();
        if (groupedCount != null) {
          commandResult.add(GROUP_COUNT, groupedCount);
        }
      } else {
        continue;
      }
      result.add(command.getKey(), commandResult);
    }
    return result;
  }

  // given a search group will return an object with its serialization
  protected abstract Object serializeOneSearchGroup(SearchGroup<BytesRef> searchGroup, SearchGroupsFieldCommand command);

  // given a raw search group will return a SearchGroup
  protected abstract SearchGroup<BytesRef> deserializeOneSearchGroup(String groupField, Sort groupSort, String groupKey, Object rawSearchGroup);

  private NamedList serializeSearchGroup(Collection<SearchGroup<BytesRef>> data, SearchGroupsFieldCommand command) {
    final NamedList<Object> result = new NamedList<>(data.size());

    for (SearchGroup<BytesRef> searchGroup : data) {
      Object convertedSortValues = serializeOneSearchGroup(searchGroup, command);
      SchemaField field = searcher.getSchema().getFieldOrNull(command.getKey());
      String groupValue = searchGroup.groupValue != null ? field.getType().indexedToReadable(searchGroup.groupValue, new CharsRefBuilder()).toString() : null;
      result.add(groupValue, convertedSortValues);
    }
    return result;
  }

  @Override
  public Map<String, SearchGroupsFieldCommandResult> transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort withinGroupSort, String shard) {
    final Map<String, SearchGroupsFieldCommandResult> result = new HashMap<>(shardResponse.size());
    for (Map.Entry<String, NamedList> command : shardResponse) {
      List<SearchGroup<BytesRef>> searchGroups = new ArrayList<>();
      NamedList topGroupsAndGroupCount = command.getValue();
      @SuppressWarnings("unchecked")
      final NamedList<List<Comparable>> rawSearchGroups = (NamedList<List<Comparable>>) topGroupsAndGroupCount.get(TOP_GROUPS);
      if (rawSearchGroups != null) {
        for (Map.Entry<String, List<Comparable>> rawSearchGroup : rawSearchGroups){
          searchGroups.add(deserializeOneSearchGroup(command.getKey(), groupSort, rawSearchGroup.getKey(), rawSearchGroup.getValue()));
        }
      }
      final Integer groupCount = (Integer) topGroupsAndGroupCount.get(GROUP_COUNT);
      result.put(command.getKey(), new SearchGroupsFieldCommandResult(groupCount, searchGroups));
    }
    return result;
  }

  public static class DefaultSearchResultResultTransformer extends SearchGroupsResultTransformer {

    public DefaultSearchResultResultTransformer(SolrIndexSearcher searcher) {
      super(searcher);
    }

    // given a raw search group will return its array sort values.
    protected Object[] getSortValues(Object groupDocs){
      List docs = (List<Comparable>)groupDocs;
      return docs.toArray(new Comparable[docs.size()]);
    }

    @Override
    protected SearchGroup<BytesRef> deserializeOneSearchGroup(String groupField, Sort groupSort, String groupKey, Object rawSearchGroup){
      SearchGroup<BytesRef> searchGroup = new SearchGroup<>();
      SchemaField schemaGroupField = groupKey != null? searcher.getSchema().getFieldOrNull(groupField) : null;
      searchGroup.groupValue = null;
      if (groupKey != null) {
        if (schemaGroupField != null) {
          BytesRefBuilder builder = new BytesRefBuilder();
          schemaGroupField.getType().readableToIndexed(groupKey, builder);
          searchGroup.groupValue = builder.get();
        } else {
          searchGroup.groupValue = new BytesRef(groupKey);
        }
      }
      searchGroup.sortValues = getSortValues(rawSearchGroup);
      for (int i = 0; i < searchGroup.sortValues.length; i++) {
        final String sortField = groupSort.getSort()[i].getField();
        SchemaField field = sortField != null ? searcher.getSchema().getFieldOrNull(sortField) : null;
        searchGroup.sortValues[i] = ShardResultTransformerUtils.unmarshalSortValue(searchGroup.sortValues[i], field);
      }
      return searchGroup;
    }

    @Override
    protected Object serializeOneSearchGroup(SearchGroup<BytesRef> searchGroup, SearchGroupsFieldCommand command) {
      Object[] convertedSortValues = new Object[searchGroup.sortValues.length];
      for (int i = 0; i < searchGroup.sortValues.length; i++) {
        Object sortValue = searchGroup.sortValues[i];
        SchemaField field = command.getGroupSort().getSort()[i].getField() != null ?
            searcher.getSchema().getFieldOrNull(command.getGroupSort().getSort()[i].getField()) : null;
        convertedSortValues[i] = ShardResultTransformerUtils.marshalSortValue(sortValue, field);
      }
      return convertedSortValues;
    }
  }

  public static class SkipSecondStepSearchResultResultTransformer extends DefaultSearchResultResultTransformer {

    private static final String TOP_DOC_SOLR_ID_KEY = "topDocSolrId";
    private static final String TOP_DOC_SCORE_KEY = "topDocScore";
    private static final String SORTVALUES_KEY  = "sortValues";

    private final SchemaField uniqueField;

    public SkipSecondStepSearchResultResultTransformer(SolrIndexSearcher searcher) {
      super(searcher);
      this.uniqueField = searcher.getSchema().getUniqueKeyField();
    }

    @Override
    protected Object[] getSortValues(Object groupDocs){
      NamedList<Object> groupInfo = (NamedList) groupDocs;
      final ArrayList<?> sortValues = (ArrayList<?>)groupInfo.get(SORTVALUES_KEY);
      return sortValues.toArray(new Comparable[sortValues.size()]);
    }

    @Override
    protected SearchGroup<BytesRef> deserializeOneSearchGroup(String groupField, Sort groupSort, String groupKey, Object groupDocs){
      SearchGroup<BytesRef> searchGroup = super.deserializeOneSearchGroup(groupField, groupSort, groupKey, groupDocs);
      NamedList<Object> groupInfo = (NamedList) groupDocs;
      searchGroup.topDocLuceneId = DocIdSetIterator.NO_MORE_DOCS;
      searchGroup.topDocScore = (float) groupInfo.get(TOP_DOC_SCORE_KEY);
      searchGroup.topDocSolrId = groupInfo.get(TOP_DOC_SOLR_ID_KEY);
      return searchGroup;
    }

    @Override
    protected Object serializeOneSearchGroup(SearchGroup<BytesRef> searchGroup, SearchGroupsFieldCommand command) {

      Document luceneDoc = null;
      /** Use the lucene id to get the unique solr id so that it can be sent to the federator.
       * The lucene id of a document is not unique across all shards i.e. different documents
       * in different shards could have the same lucene id, whereas the solr id is guaranteed
       * to be unique so this is what we need to return to the federator
       **/
      try {
        luceneDoc =  searcher.doc(searchGroup.topDocLuceneId, Collections.singleton(uniqueField.getName()));
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot retrieve document for unique field " + uniqueField + " ("+e.toString()+")");
      }
      Object topDocSolrId = uniqueField.getType().toExternal(luceneDoc.getField(uniqueField.getName()));
      NamedList<Object> groupInfo = new NamedList<>(5); // TODO: comment re: why 5 (and not 3)
      groupInfo.add(TOP_DOC_SCORE_KEY, searchGroup.topDocScore);
      groupInfo.add(TOP_DOC_SOLR_ID_KEY, topDocSolrId);

      Object convertedSortValues = super.serializeOneSearchGroup(searchGroup, command);
      groupInfo.add(SORTVALUES_KEY, convertedSortValues);
      return groupInfo;
    }
  }
}
