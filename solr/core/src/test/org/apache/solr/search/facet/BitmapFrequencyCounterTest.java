package org.apache.solr.search.facet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class BitmapFrequencyCounterTest extends LuceneTestCase {
  private static final int TEST_ORDINAL = 5;

  @Test
  public void testAddValue() {
    int iters = 10 * RANDOM_MULTIPLIER;

    for (int i = 0; i < iters; i++) {
      int size = random().nextInt(8);

      BitmapFrequencyCounter counter = new BitmapFrequencyCounter(size);

      int numValues = random().nextInt(100);
      Map<Integer, Integer> values = new HashMap<>();
      for (int j = 0; j < numValues; j++) {
        int value = random().nextInt();
        int count = random().nextInt(256);

        addCount(counter, value, count);

        values.put(value, count);
      }

      values.forEach((value, count) -> assertCount(counter, value, count));

      counter.normalize();

      values.forEach((value, count) -> assertCountNormalized(counter, value, count));
    }
  }

  @Test
  public void testMerge() {
    int iters = 10 * RANDOM_MULTIPLIER;

    for (int i = 0; i < iters; i++) {
      int size = random().nextInt(8);

      BitmapFrequencyCounter x = new BitmapFrequencyCounter(size);

      int numXValues = random().nextInt(100);
      Map<Integer, Integer> xValues = new HashMap<>();
      for (int j = 0; j < numXValues; j++) {
        int value = random().nextInt();
        int count = random().nextInt(256);

        addCount(x, value, count);

        xValues.put(value, count);
      }

      xValues.forEach((value, count) -> assertCount(x, value, count));

      BitmapFrequencyCounter y = new BitmapFrequencyCounter(size);

      int numYValues = random().nextInt(100);
      Map<Integer, Integer> yValues = new HashMap<>();
      for (int j = 0; j < numYValues; j++) {
        int value = random().nextInt();
        int count = random().nextInt(256);

        addCount(y, value, count);

        yValues.put(value, count);
      }

      yValues.forEach((value, count) -> assertCount(y, value, count));

      if (random().nextBoolean()) {
        x.normalize();
      }

      if (random().nextBoolean()) {
        y.normalize();
      }

      BitmapFrequencyCounter merged = x.merge(y);

      yValues.forEach((value, count) -> xValues.merge(value, count, Integer::sum));

      xValues.forEach((value, count) -> assertCount(merged, value, count));
    }
  }

  @Test(expected = NegativeArraySizeException.class)
  public void givenNegativeSize_whenConstructingCounter() {
    new BitmapFrequencyCounter(-1);
  }

  @Test
  public void givenSize0_whenAddingValue_withFrequency1() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(0);

    addCount(counter, TEST_ORDINAL, 1);

    assertCount(counter, TEST_ORDINAL, 1);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 1);
  }

  @Test
  public void givenSize0_whenAddingValue_withFrequency2() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(0);

    addCount(counter, TEST_ORDINAL, 2);

    assertCount(counter, TEST_ORDINAL, 2);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 2);
  }

  @Test
  public void givenSize1_whenAddingValue_withFrequency1() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(1);

    addCount(counter, TEST_ORDINAL, 1);

    assertCount(counter, TEST_ORDINAL, 1);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 1);

    int[] decoded = counter.decode();

    assertEquals(decoded.length, 2);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 1);
  }

  @Test
  public void givenSize1_whenAddingValue_withFrequency2() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(1);

    addCount(counter, TEST_ORDINAL, 2);

    assertCount(counter, TEST_ORDINAL, 2);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 2);

    int[] decoded = counter.decode();

    assertEquals(decoded.length, 2);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 0);
  }

  @Test
  public void givenSize2_whenAddingValue_withFrequency1() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(2);

    addCount(counter, TEST_ORDINAL, 1);

    assertCount(counter, TEST_ORDINAL, 1);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 1);

    int[] decoded = counter.decode();

    assertEquals(decoded.length, 2);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 1);
  }

  @Test
  public void givenSize2_whenAddingValue_withFrequency2() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(2);

    addCount(counter, TEST_ORDINAL, 2);

    assertCount(counter, TEST_ORDINAL, 2);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 2);

    int[] decoded = counter.decode();

    assertEquals(decoded.length, 4);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 0);
    assertEquals(decoded[2], 1);
    assertEquals(decoded[3], 0);
  }

  @Test
  public void givenSize2_whenAddingValue_withFrequency3() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(2);

    addCount(counter, TEST_ORDINAL, 3);

    assertCount(counter, TEST_ORDINAL, 3);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 3);

    int[] decoded = counter.decode();

    assertEquals(decoded.length, 4);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 0);
    assertEquals(decoded[2], 0);
    assertEquals(decoded[3], 1);
  }

  @Test
  public void givenSize2_whenAddingValue_withFrequency4() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(2);

    addCount(counter, TEST_ORDINAL, 4);

    assertCount(counter, TEST_ORDINAL, 4);

    counter.normalize();

    assertCountNormalized(counter, TEST_ORDINAL, 4);

    int[] decoded = counter.decode();

    assertEquals(decoded.length, 4);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 0);
    assertEquals(decoded[2], 0);
    assertEquals(decoded[3], 0);
  }

  @Test
  public void givenSize2_whenAddingMultipleValues() {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(2);

    addCount(counter, 101, 1);
    addCount(counter, 102, 2);
    addCount(counter, 202, 2);
    addCount(counter, 103, 3);
    addCount(counter, 203, 3);
    addCount(counter, 303, 3);

    assertCount(counter, 101, 1);
    assertCount(counter, 102, 2);
    assertCount(counter, 202, 2);
    assertCount(counter, 103, 3);
    assertCount(counter, 203, 3);
    assertCount(counter, 303, 3);

    counter.normalize();

    assertCountNormalized(counter, 101, 1);
    assertCountNormalized(counter, 102, 2);
    assertCountNormalized(counter, 202, 2);
    assertCountNormalized(counter, 103, 3);
    assertCountNormalized(counter, 203, 3);
    assertCountNormalized(counter, 303, 3);

    int[] decoded = counter.decode();

    assertEquals(decoded.length, 4);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 1);
    assertEquals(decoded[2], 2);
    assertEquals(decoded[3], 3);
  }

  @Test
  public void givenSize2_whenMergingNonnormalizedValues() {
    BitmapFrequencyCounter x = new BitmapFrequencyCounter(2);
    BitmapFrequencyCounter y = new BitmapFrequencyCounter(2);

    addCount(x, TEST_ORDINAL, 2);
    addCount(y, TEST_ORDINAL, 2);

    assertCount(x, TEST_ORDINAL, 2);
    assertCount(y, TEST_ORDINAL, 2);

    x = x.merge(y);

    assertCount(x, TEST_ORDINAL, 4);
  }

  @Test
  public void givenSize2_whenMergingNormalizedValues() {
    BitmapFrequencyCounter x = new BitmapFrequencyCounter(2);
    BitmapFrequencyCounter y = new BitmapFrequencyCounter(2);

    addCount(x, TEST_ORDINAL, 2);
    addCount(y, TEST_ORDINAL, 2);

    assertCount(x, TEST_ORDINAL, 2);
    assertCount(y, TEST_ORDINAL, 2);

    x.normalize();
    y.normalize();

    assertCountNormalized(x, TEST_ORDINAL, 2);
    assertCountNormalized(y, TEST_ORDINAL, 2);

    x = x.merge(y);

    assertCount(x, TEST_ORDINAL, 4);
  }

  @Test
  public void givenSize4_whenMergingNonnormalizedValues() {
    BitmapFrequencyCounter x = new BitmapFrequencyCounter(4);
    BitmapFrequencyCounter y = new BitmapFrequencyCounter(4);

    addCount(x, TEST_ORDINAL, 10);
    addCount(y, TEST_ORDINAL, 5);

    assertCount(x, TEST_ORDINAL, 10);
    assertCount(y, TEST_ORDINAL, 5);

    x = x.merge(y);

    assertCount(x, TEST_ORDINAL, 15);

    x.normalize();

    int[] decoded = x.decode();

    assertEquals(decoded.length, 16);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 0);
    assertEquals(decoded[2], 0);
    assertEquals(decoded[3], 0);
    assertEquals(decoded[4], 0);
    assertEquals(decoded[5], 0);
    assertEquals(decoded[6], 0);
    assertEquals(decoded[7], 0);
    assertEquals(decoded[8], 0);
    assertEquals(decoded[9], 0);
    assertEquals(decoded[10], 0);
    assertEquals(decoded[11], 0);
    assertEquals(decoded[12], 0);
    assertEquals(decoded[13], 0);
    assertEquals(decoded[14], 0);
    assertEquals(decoded[15], 1);
  }

  @Test
  public void givenSize4_whenMergingNormalizedValues() {
    BitmapFrequencyCounter x = new BitmapFrequencyCounter(4);
    BitmapFrequencyCounter y = new BitmapFrequencyCounter(4);

    addCount(x, TEST_ORDINAL, 10);
    addCount(y, TEST_ORDINAL, 5);

    assertCount(x, TEST_ORDINAL, 10);
    assertCount(y, TEST_ORDINAL, 5);

    x.normalize();
    y.normalize();

    assertCountNormalized(x, TEST_ORDINAL, 10);
    assertCountNormalized(y, TEST_ORDINAL, 5);

    x = x.merge(y);

    assertCount(x, TEST_ORDINAL, 15);

    x.normalize();

    int[] decoded = x.decode();

    assertEquals(decoded.length, 16);
    assertEquals(decoded[0], 0);
    assertEquals(decoded[1], 0);
    assertEquals(decoded[2], 0);
    assertEquals(decoded[3], 0);
    assertEquals(decoded[4], 0);
    assertEquals(decoded[5], 0);
    assertEquals(decoded[6], 0);
    assertEquals(decoded[7], 0);
    assertEquals(decoded[8], 0);
    assertEquals(decoded[9], 0);
    assertEquals(decoded[10], 0);
    assertEquals(decoded[11], 0);
    assertEquals(decoded[12], 0);
    assertEquals(decoded[13], 0);
    assertEquals(decoded[14], 0);
    assertEquals(decoded[15], 1);
  }

  @Test
  public void testSerialization() throws IOException {
    BitmapFrequencyCounter counter = new BitmapFrequencyCounter(2);

    addCount(counter, 101, 1);
    addCount(counter, 102, 2);
    addCount(counter, 202, 2);
    addCount(counter, 103, 3);
    addCount(counter, 203, 3);
    addCount(counter, 303, 3);

    counter.normalize();

    JavaBinCodec codec = new JavaBinCodec();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    codec.marshal(counter.serialize(), out);

    InputStream in = new ByteArrayInputStream(out.toByteArray());
    counter = new BitmapFrequencyCounter(2);
    counter.deserialize((SimpleOrderedMap<Object>) codec.unmarshal(in));

    assertCount(counter, 101, 1);
    assertCount(counter, 102, 2);
    assertCount(counter, 202, 2);
    assertCount(counter, 103, 3);
    assertCount(counter, 203, 3);
    assertCount(counter, 303, 3);
  }

  private static void addCount(BitmapFrequencyCounter counter, int value, int count) {
    for (int i = 0; i < count; i++) {
      counter.add(value);
    }
  }

  private static void assertCount(BitmapFrequencyCounter counter, int value, int count) {
    RoaringBitmap[] bitmaps = counter.getBitmaps();

    if (count >= (1 << bitmaps.length)) {
      int overflowCount = count;
      for (int i = 0; i < bitmaps.length; i++) {
        if (bitmaps[i] != null && bitmaps[i].contains(value)) {
          overflowCount -= 1 << i;
        }
      }

      assertEquals(
          "Overflow should contain value " + value + " with overflow count " + overflowCount + " (for count " + count + ")",
          overflowCount, (int) counter.getOverflow().getOrDefault(value, 0)
      );
    } else {
      for (int i = 0; i < bitmaps.length; i++) {
        if (((count >> i) & 1) == 1) {
          assertTrue(
              "bitmap " + i + " should contain value " + value + " (for count " + count + ")",
              bitmaps[i].contains(value)
          );
        } else if (bitmaps[i] != null) {
          assertFalse(
              "bitmap " + i + " should not contain value " + value + " (for count " + count + ")",
              bitmaps[i].contains(value)
          );
        }
      }
    }
  }

  private static void assertCountNormalized(BitmapFrequencyCounter counter, int value, int count) {
    RoaringBitmap[] bitmaps = counter.getBitmaps();

    if (count >= (1 << bitmaps.length)) {
      for (int i = 0; i < bitmaps.length; i++) {
        if (bitmaps[i] != null) {
          assertFalse(
              "bitmap " + i + " should not contain value " + value + " (for count " + count + ")",
              bitmaps[i].contains(value)
          );
        }
      }

      assertEquals(
          "Overflow should contain value " + value + " (for count " + count + ")",
          (int) counter.getOverflow().get(value), count
      );
    } else {
      for (int i = 0; i < bitmaps.length; i++) {
        if (((count >> i) & 1) == 1) {
          assertTrue(
              "bitmap " + i + " should contain value " + value + " (for count " + count + ")",
              bitmaps[i].contains(value)
          );
        } else if (bitmaps[i] != null) {
          assertFalse(
              "bitmap " + i + " should not contain value " + value + " (for count " + count + ")",
              bitmaps[i].contains(value)
          );
        }
      }
    }
  }
}
