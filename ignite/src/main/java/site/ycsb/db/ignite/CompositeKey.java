package site.ycsb.db.ignite;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * DAO for composite key.
 */
public class CompositeKey implements Serializable {
  private String tagName;

  private String tagValue;

  @AffinityKeyMapped
  private String ycsbKey;

  public CompositeKey(String key) {
    tagName = "tagn" + key.substring(4);
    tagValue = "tagv" + key.substring(4);
    ycsbKey = key;
  }

  public String getTagName() {
    return tagName;
  }

  public void setTagName(String tagName) {
    this.tagName = tagName;
  }

  public String getTagValue() {
    return tagValue;
  }

  public void setTagValue(String tagValue) {
    this.tagValue = tagValue;
  }

  public String getYcsbKey() {
    return ycsbKey;
  }

  public void setYcsbKey(String ycsbKey) {
    this.ycsbKey = ycsbKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompositeKey that = (CompositeKey) o;
    return Objects.equals(tagName, that.tagName) && Objects.equals(tagValue, that.tagValue) && Objects.equals(
        ycsbKey, that.ycsbKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tagName, tagValue, ycsbKey);
  }
}
