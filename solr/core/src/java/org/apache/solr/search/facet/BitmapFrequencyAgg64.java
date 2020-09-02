package org.apache.solr.search.facet;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

/**
 * Calculates the frequency of ordinal values using Roaring Bitmaps.
 *
 * The response is a map with the following fields:
 * - bitmaps: an array of bitmaps, where the frequency of a value x is given by the sum of {@code 2^i} for all values
 *   of {@code i} where {@code bitmaps[i].contains(x)}
 * - overflow: a map of ordinal values to frequencies, for values with {@code frequency >= 2^(bitmaps.length)}
 *
 * Lacking a coherent definition of magnitude other than the raw count, this aggregate cannot be used for sorting.
 */
public class BitmapFrequencyAgg64 extends SimpleAggValueSource {
  private final int size;

  public BitmapFrequencyAgg64(ValueSource vs, int size) {
    super("bitmapfreq64", vs);

    this.size = size;
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) {
    return new BitmapFrequencySlotAcc64(getArg(), fcontext, numSlots, size);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger(size);
  }

  public static class Parser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      ValueSource valueSource = fp.parseValueSource();

      int size = 16;
      if (fp.hasMoreArguments()) {
        size = fp.parseInt();
      }

      return new BitmapFrequencyAgg64(valueSource, size);
    }
  }

  private static class Merger extends FacetMerger {
    private final int size;
    private BitmapFrequencyCounter64 result;

    public Merger(int size) {
      this.size = size;
      this.result = new BitmapFrequencyCounter64(size);
    }

    @Override
    public void merge(Object facetResult, Context mcontext) {
      if (facetResult instanceof SimpleOrderedMap) {
        BitmapFrequencyCounter64 deserialized = new BitmapFrequencyCounter64(size);
        deserialized.deserialize((SimpleOrderedMap<Object>) facetResult);

        result = result.merge(deserialized);
      }
    }

    @Override
    public void finish(Context mcontext) {
      // never called
    }

    @Override
    public Object getMergedResult() {
      result.normalize();
      return result.serialize();
    }
  }
}
