package org.apache.solr.search.facet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class BitmapUtil {
  public static byte[] bitmapToBytes(ImmutableBitmapDataProvider bitmap) {
    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    return buffer.array();
  }

  public static RoaringBitmap bytesToBitmap(byte[] bytes) {
    try {
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(ByteBuffer.wrap(bytes));
      return bitmap;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to deserialise RoaringBitmap from bytes", ioe);
    }
  }

  public static byte[] bitmapToBytes64(ImmutableLongBitmapDataProvider bitmap) {
    try (
      ByteArrayOutputStream baos = new ByteArrayOutputStream((int) bitmap.serializedSizeInBytes());
      DataOutputStream dos = new DataOutputStream(baos)
    ) {
      bitmap.serialize(dos);
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to serialise RoaringBitmap to bytes", ioe);
    }
  }

  public static Roaring64NavigableMap bytesToBitmap64(byte[] bytes) {
    try (
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bais)
    ) {
      Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
      bitmap.deserialize(dis);
      return bitmap;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to deserialise RoaringBitmap from bytes", ioe);
    }
  }
}
