package org.apache.solr.search.facet;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.util.SimpleOrderedMap;

public class TermFrequencyCounter {
  private final Map<String, Integer> counters;

  public TermFrequencyCounter() {
    this.counters = new HashMap<>();
  }

  public Map<String, Integer> getCounters() {
    return this.counters;
  }

  public void add(String value) {
    counters.merge(value, 1, Integer::sum);
  }

  public Map<String, Integer> serialize(int limit) {
    if (limit < Integer.MAX_VALUE && limit < counters.size()) {
      return counters.entrySet()
        .stream()
        .sorted((l, r) -> r.getValue() - l.getValue()) // sort by value descending
        .limit(limit)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } else {
      return counters;
    }
  }

  public TermFrequencyCounter merge(Map<String, Integer> serialized) {
    serialized.forEach((value, freq) -> counters.merge(value, freq, Integer::sum));

    return this;
  }
}
