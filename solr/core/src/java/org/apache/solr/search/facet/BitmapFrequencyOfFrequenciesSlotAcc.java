package org.apache.solr.search.facet;

import java.util.Collections;

import org.apache.lucene.queries.function.ValueSource;

public class BitmapFrequencyOfFrequenciesSlotAcc extends BitmapFrequencySlotAcc {
  public BitmapFrequencyOfFrequenciesSlotAcc(ValueSource values, FacetContext fcontext, int numSlots, int maxFrequency) {
    super(values, fcontext, numSlots, maxFrequency);
  }

  @Override
  public Object getFinalValue(BitmapFrequencyCounter result) {
    if (result != null) {
      result.normalize();
      return result.toFrequencyOfFrequencies();
    } else {
      return Collections.emptyMap();
    }
  }
}
