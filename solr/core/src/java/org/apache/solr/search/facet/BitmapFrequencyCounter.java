package org.apache.solr.search.facet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.SimpleOrderedMap;
import org.roaringbitmap.RoaringBatchIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Counts frequencies of ordinal values using Roaring Bitmaps.
 */
public class BitmapFrequencyCounter {
  private final RoaringBitmap[] bitmaps;
  private final Map<Integer, Integer> overflow;

  /**
   * Constructs a new frequency counter. Frequencies greater than {@code (2^size)-1} will be represented as a HashMap
   * (rather than a compact bitmap encoding), and for efficiency should not represent a large fraction of the distinct
   * values to be counted.
   *
   * @param size The maximum size of the frequencies list
   */
  public BitmapFrequencyCounter(int size) {
    this.bitmaps = new RoaringBitmap[size];
    this.overflow = new HashMap<>();
  }

  /**
   * An array of bitmaps encoding frequencies of values: the frequency of a value x is given by the sum of {@code 2^i}
   * for all values of {@code i} where {@code bitmaps[i].contains(x)}.
   *
   * @return The encoded frequencies
   */
  public RoaringBitmap[] getBitmaps() {
    return this.bitmaps;
  }

  /**
   * A map of high-frequency values (with {@code frequency >= 2^(bitmaps.length)}).
   *
   * @return The map of high-frequency values.
   */
  public Map<Integer, Integer> getOverflow() {
    return this.overflow;
  }

  /**
   * Adds one occurrence of the given value to the counter.
   *
   * @param value The value to add
   */
  public void add(int value) {
    final Integer overflowCount = overflow.computeIfPresent(value, (v, f) -> f + 1);
    if (overflowCount != null) {
      return;
    }

    // This is just binary addition x+1=y - we carry the value till we find an empty column
    for (int i = 0; i < bitmaps.length; i++) {
      RoaringBitmap bitmap = bitmaps[i];
      if (bitmap == null) {
        bitmap = bitmaps[i] = new RoaringBitmap();
      }

      if (!bitmap.checkedRemove(value)) {
        bitmap.add(value);
        return;
      }
    }

    // If we reach this point, the frequency of this value has reached 2^(bitmaps.length)

    overflow.put(value, 1 << bitmaps.length);
  }

  /**
   * Serializes the counter.
   *
   * @return The serialized data
   */
  public SimpleOrderedMap<Object> serialize() {
    SimpleOrderedMap<Object> serialized = new SimpleOrderedMap<>();

    List<byte[]> serializedBitmaps = new ArrayList<>(bitmaps.length);

    int i = 0;
    while (i < bitmaps.length) {
      RoaringBitmap bitmap = bitmaps[i];
      if (bitmap == null) {
        break;
      }

      bitmap.runOptimize();
      serializedBitmaps.add(BitmapUtil.bitmapToBytes(bitmap));

      i++;
    }

    if (i > 0) {
      serialized.add("bitmaps", serializedBitmaps);
    }

    if (!overflow.isEmpty()) {
      serialized.add("overflow", overflow);
    }

    return serialized;
  }

  /**
   * Populates the counter from the given serialized data.
   *
   * The counter must be fresh (with no values previously added), and have the same size as the counter from which the
   * serialized data was generated.
   *
   * @param serialized The serialized data
   */
  public void deserialize(SimpleOrderedMap<Object> serialized) {
    List<byte[]> serializedBitmaps = (List<byte[]>) serialized.get("bitmaps");
    if (serializedBitmaps != null) {
      for (int i = 0; i < serializedBitmaps.size(); i++) {
        bitmaps[i] = BitmapUtil.bytesToBitmap(serializedBitmaps.get(i));
      }
    }

    Map<Integer, Integer> overflow = (Map<Integer, Integer>) serialized.get("overflow");
    if (overflow != null) {
      this.overflow.putAll(overflow);
    }
  }

  /**
   * Merges this counter with another (in-place).
   *
   * The other counter must have the same size as this counter. After this operation, the returned counter will contain
   * the values from both counters with their frequencies added together, and references to either of the original
   * counters should be discarded (since either may now be invalid, and one will have been modified and returned).
   *
   * @param other The counter to merge in
   * @return The merged counter
   */
  public BitmapFrequencyCounter merge(BitmapFrequencyCounter other) {
    // The algorithm here is a ripple-carry adder in two dimensions, built from half-adders that are adapted from the
    // standard (where s is the sum, and c the carried value):
    //
    // s = x xor y
    // c = x and y
    //
    // to:
    //
    // s = x xor y
    // c = y andnot s
    //
    // which allows in-place modification of bitmaps (x modified into s, y modified into c).

    if (bitmaps.length == 0) {
      other.overflow.forEach((value, freq) -> overflow.merge(value, freq, Integer::sum));

      return this;
    }

    RoaringBitmap c;

    int i = 0;

    RoaringBitmap x = bitmaps[i];
    RoaringBitmap y = other.bitmaps[i];
    if (x == null) {
      return other;
    } else if (y == null) {
      return this;
    }

    x.xor(y); // x2 = x1 xor y1
    y.andNot(x); // y2 = y1 andnot x2

    c = y; // c1 = y2

    i++;

    while (i < bitmaps.length) {
      x = bitmaps[i];
      y = other.bitmaps[i];
      if (x == null || y == null) {
        break;
      }

      x.xor(y); // x2 = x1 xor y1
      y.andNot(x); // y2 = y1 andnot x2
      x.xor(c); // x3 = x2 xor c1

      c.andNot(x); // c2 = c1 andnot x3
      c.or(y); // c3 = c2 or y2

      i++;
    }

    while (i < bitmaps.length) {
      x = bitmaps[i];
      if (x == null) {
        break;
      }

      x.xor(c); // x2 = x1 xor c1
      c.andNot(x); // c2 = c1 andnot x2

      i++;
    }

    while (i < bitmaps.length) {
      x = other.bitmaps[i];
      if (x == null) {
        break;
      }

      x.xor(c); // x2 = x1 xor c1
      c.andNot(x); // c2 = c1 andnot x2

      bitmaps[i] = x;

      i++;
    }

    if (i == bitmaps.length) {
      other.overflow.forEach((value, freq) -> overflow.merge(value, freq, Integer::sum));

      RoaringBatchIterator iter = c.getBatchIterator();
      int[] batch = new int[128];
      while (iter.hasNext()) {
        int batchSize = iter.nextBatch(batch);
        for (int j = 0; j < batchSize; j++) {
          int value = batch[j];
          int freq = 1 << bitmaps.length;
          overflow.merge(value, freq, Integer::sum);
        }
      }
    }

    return this;
  }

  public void normalize() {
    overflow.replaceAll((value, freq) -> {
      for (int k = 0; k < bitmaps.length; k++) {
        if (bitmaps[k].checkedRemove(value)) {
          freq += 1 << k;
        }
      }
      return freq;
    });
  }

  public Map<Integer, Integer> toFrequencyOfFrequencies() {
    Map<Integer, Integer> map = new LinkedHashMap<>();

    int[] lowFrequencies = decode();
    for (int i = 0; i < lowFrequencies.length; i++) {
      int value = lowFrequencies[i];
      if (value > 0) {
        map.put(i, value);
      }
    }

    overflow.forEach((value, freq) -> map.merge(freq, 1, Integer::sum));

    return map;
  }

  public int[] decode() {
    int endIndex = 0;
    while (endIndex < bitmaps.length && bitmaps[endIndex] != null) {
      endIndex++;
    }

    if (endIndex == 0) {
      return new int[0];
    }

    int[] result = new int[1 << endIndex];

    endIndex--;

    if (endIndex == 0) {
      result[1] = bitmaps[0].getCardinality();
    } else {
      RoaringBitmap highBits = bitmaps[endIndex];

      decodeLowest(highBits, endIndex - 1, result);
      decode(highBits, endIndex - 1, result, 1 << endIndex);
    }

    return result;
  }

  private void decodeLowest(
    RoaringBitmap excludedBits,
    int endIndex,
    int[] result
  ) {
    if (endIndex == 0) {
      result[1] = RoaringBitmap.andNotCardinality(bitmaps[0], excludedBits);
    } else {
      RoaringBitmap highBits = RoaringBitmap.andNot(bitmaps[endIndex], excludedBits);
      excludedBits = RoaringBitmap.or(bitmaps[endIndex], excludedBits);

      decodeLowest(excludedBits, endIndex - 1, result);
      decode(highBits, endIndex - 1, result, 1 << endIndex);
    }
  }

  private void decode(
    RoaringBitmap includedBits,
    int endIndex,
    int[] result,
    int resultOffset
  ) {
    if (endIndex == 0) {
      result[resultOffset] = RoaringBitmap.andNotCardinality(includedBits, bitmaps[0]);
      result[resultOffset + 1] = RoaringBitmap.andCardinality(includedBits, bitmaps[0]);
    } else {
      RoaringBitmap highBits = RoaringBitmap.and(includedBits, bitmaps[endIndex]);
      RoaringBitmap lowBits = RoaringBitmap.andNot(includedBits, highBits);

      decode(lowBits, endIndex - 1, result, resultOffset);
      decode(highBits, endIndex - 1, result, resultOffset + (1 << endIndex));
    }
  }
}
