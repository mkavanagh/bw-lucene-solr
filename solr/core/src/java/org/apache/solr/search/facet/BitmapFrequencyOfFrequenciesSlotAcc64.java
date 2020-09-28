package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.IntFunction;

import org.apache.lucene.queries.function.ValueSource;

public class BitmapFrequencyOfFrequenciesSlotAcc64 extends BitmapFrequencySlotAcc64 {
  public BitmapFrequencyOfFrequenciesSlotAcc64(ValueSource values, FacetContext fcontext, int numSlots, int maxFrequency) {
    super(values, fcontext, numSlots, maxFrequency);
  }

  @Override
  public Object getFinalValue(BitmapFrequencyCounter64 result) {
    if (result != null) {
      result.normalize();
      return result.toFrequencyOfFrequencies();
    } else {
      return Collections.emptyMap();
    }
  }
}
