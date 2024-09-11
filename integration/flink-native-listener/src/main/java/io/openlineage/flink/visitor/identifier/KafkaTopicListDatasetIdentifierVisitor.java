/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.util.KafkaDatasetFacetUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;

/** Visitor */
@Slf4j
public class KafkaTopicListDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
  @Override
  public boolean isDefinedAt(LineageDataset dataset) {
    return KafkaDatasetFacetUtil.isOnClasspath() && !getTopicList(dataset).isEmpty();
  }

  private Collection<String> getTopicList(LineageDataset dataset) {
    return KafkaDatasetFacetUtil.getFacet(dataset)
        .map(f -> f.getTopicIdentifier())
        .map(i -> i.getTopics())
        .orElse(Collections.emptyList());
  }

  @Override
  public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
    return getTopicList(dataset).stream()
        .map(topic -> new DatasetIdentifier(topic, dataset.namespace()))
        .collect(Collectors.toList());
  }
}
