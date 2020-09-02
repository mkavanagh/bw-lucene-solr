package org.apache.solr.search.facet;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

/**
 * Calculates the frequency-of-frequencies (number of values occurring x times) of ordinal values.
 *
 * The response is a map where the keys are frequencies (x = number of times a value occurred), and the values are
 * the frequency-of-frequencies (number of values which occurred x times).
 *
 * Lacking a coherent definition of magnitude other than the raw count, this aggregate cannot be used for sorting.
 */
public class FrequencyOfFrequenciesAgg64 extends SimpleAggValueSource {
  private final int size;

  public FrequencyOfFrequenciesAgg64(ValueSource vs, Integer size) {
    super("bitmapfreqfreq64", vs);

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

      int size = 8;
      if (fp.hasMoreArguments()) {
        size = fp.parseInt();
      }

      return new FrequencyOfFrequenciesAgg64(valueSource, size);
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
      Map<Integer, Integer> map = new LinkedHashMap<>();

      result.normalize();

      int[] lowFrequencies = result.decode();
      for (int i = 0; i < lowFrequencies.length; i++) {
        int value = lowFrequencies[i];
        if (value > 0) {
          map.put(i, value);
        }
      }

      result.getOverflow()
        .forEach((value, freq) -> map.merge(freq, 1, Integer::sum));

      return map;
    }
  }
}
