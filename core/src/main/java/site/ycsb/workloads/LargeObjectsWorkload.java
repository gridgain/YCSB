package site.ycsb.workloads;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

/**
 * Workload optimized for large objects: reuses prebuilt templates and embeds a small
 * key-dependent signature at the beginning of each value for quick verification.
 */
public class LargeObjectsWorkload extends CoreWorkload {
  /** Shared pool of prebuilt byte templates by length. */
  private static final Map<Integer, byte[]> TEMPLATE_POOL = new ConcurrentHashMap<>();

  /** Size of the signature prefix at the beginning of each value. */
  private static final int SIGNATURE_LENGTH = 64;

  /** Build values for all fields using pooled templates and signature prefix. */
  @Override
  protected HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>(fieldnames.size() * 2);
    for (String fieldKey : fieldnames) {
      int size = fieldlengthgenerator.nextValue().intValue();
      byte[] base = getTemplate(size);
      byte[] valueBytes = Arrays.copyOf(base, base.length);

      if (dataintegrity) {
        int prefixLen = Math.min(SIGNATURE_LENGTH, size);
        byte[] sig = buildSignatureBytes(key, fieldKey);
        System.arraycopy(sig, 0, valueBytes, 0, prefixLen);
      }

      values.put(fieldKey, new ByteArrayByteIterator(valueBytes));
    }

    return values;
  }

  /** Signature-only verification to avoid scanning entire large values. */
  @Override
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long start = System.nanoTime();

    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> e : cells.entrySet()) {
        String fieldKey = e.getKey();
        ByteIterator fieldValue = e.getValue();

        // Ensure we verify against the full array if the iterator was partially read.
        if (fieldValue instanceof ByteArrayByteIterator) {
          fieldValue.reset();
        }

        byte[] returned = fieldValue.toArray();

        if (returned.length == 0) {
          verifyStatus = Status.ERROR;
          break;
        }

        int checkLen = Math.min(SIGNATURE_LENGTH, returned.length);
        byte[] actual = Arrays.copyOfRange(returned, 0, checkLen);
        byte[] expected = Arrays.copyOf(buildSignatureBytes(key, fieldKey), checkLen);

        if (!Arrays.equals(actual, expected)) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      verifyStatus = Status.ERROR;
    }

    long end = System.nanoTime();
    measurements.measure("VERIFY", (end - start) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
  }

  /** Build deterministic signature for key+field. */
  private static byte[] buildSignatureBytes(String key, String fieldKey) {
    byte[] input = (key + ":" + fieldKey).getBytes(StandardCharsets.UTF_8);
    byte[] sig = new byte[SIGNATURE_LENGTH];
    int copyLen = Math.min(input.length, SIGNATURE_LENGTH);
    System.arraycopy(input, 0, sig, 0, copyLen);

    for (int i = copyLen; i < SIGNATURE_LENGTH; i++) {
      sig[i] = (byte) (i * 31); // deterministic padding
    }

    return sig;
  }

  /** Get cached template buffer for a given size. */
  private static byte[] getTemplate(int size) {
    return TEMPLATE_POOL.computeIfAbsent(size, len -> {
        byte[] buf = new byte[len];

        for (int i = 0; i < len; i++) {
          buf[i] = (byte) (i * 13 + 7);
        }

        return buf;
      });
  }
}