package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.IntFunction;

import org.apache.lucene.queries.function.ValueSource;

public class BitmapFrequencySlotAcc64 extends FuncSlotAcc {
  private BitmapFrequencyCounter64[] result;
  private final int maxFrequency;

  public BitmapFrequencySlotAcc64(ValueSource values, FacetContext fcontext, int numSlots, int maxFrequency) {
    super(values, fcontext, numSlots);

    this.result = new BitmapFrequencyCounter64[numSlots];
    this.maxFrequency = maxFrequency;
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    if (result[slot] == null) {
      result[slot] = new BitmapFrequencyCounter64(this.maxFrequency);
    }
    result[slot].add(values.longVal(doc));
  }

  @Override
  public int compare(int slotA, int slotB) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getValue(int slotNum) {
    if (fcontext.isShard()) {
      return getShardValue(result[slotNum]);
    } else {
      return getFinalValue(result[slotNum]);
    }
  }

  public Object getShardValue(BitmapFrequencyCounter64 result) {
    if (result != null) {
      return result.serialize();
    } else {
      return Collections.emptyList();
    }
  }

  public Object getFinalValue(BitmapFrequencyCounter64 result) {
    result.normalize();
    return getShardValue(result);
  }

  @Override
  public void reset() {
    Arrays.fill(result, null);
  }

  @Override
  public void resize(Resizer resizer) {
    result = resizer.resize(result, null);
  }
}
