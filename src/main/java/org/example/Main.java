package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Main {
  private static final String BUCKET_NAME = "videos";
  private static final String VOLUME_NAME = "assets";
  private static final String KEY_NAME_PREFIX = "foo_";
  private static final ReplicationConfig REPLICATION_CONFIG =
      ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

  public static void main(String[] args) {
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setStrings(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "localhost");

    try (OzoneClient rpcClient = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore objectStore = rpcClient.getObjectStore();
      OzoneVolume volume = lazyGetVolume(objectStore);
      OzoneBucket bucket = lazyGetBucket(volume, createBucketArgs());

      int count = 1000;
      deleteKeys(bucket, count);
      createKeys(bucket, count);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static OzoneBucket lazyGetBucket(OzoneVolume volume, BucketArgs bucketArgs) throws IOException {
    try {
      return volume.getBucket(BUCKET_NAME);
    } catch (IOException e) {
      volume.createBucket(BUCKET_NAME, bucketArgs);
      return volume.getBucket(BUCKET_NAME);
    }
  }

  private static BucketArgs createBucketArgs() {
    return BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(REPLICATION_CONFIG))
        .build();
  }

  private static OzoneVolume lazyGetVolume(ObjectStore objectStore) throws IOException {
    try {
      return objectStore.getVolume(VOLUME_NAME);
    } catch (IOException e) {
      objectStore.createVolume(VOLUME_NAME);
      return objectStore.getVolume(VOLUME_NAME);
    }
  }

  private static void createKeys(OzoneBucket bucket, int count) throws IOException {
    for (int i = 0; i < count; ++i) {
      String data = UUID.randomUUID().toString();
      byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
      OzoneOutputStream foo = bucket.createKey(KEY_NAME_PREFIX + i, bytes.length);
      foo.write(bytes);
      foo.close();
    }
  }

  private static void deleteKeys(OzoneBucket bucket, int count) throws IOException {
    for (int i = 0; i < count; ++i) {
      bucket.deleteKey(KEY_NAME_PREFIX + i);
    }
  }

//  private static void writeFileWithHDFS(String volumeName, String bucketName) throws IOException {
//    String uri = String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucketName, volumeName);
//    Configuration conf = new Configuration();
//    conf.set("fs.defaultFS", uri);
//    conf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);
//
//    FileSystem fs = FileSystem.get(conf);
//
//    Path file1 = new Path("key1");
//    FSDataOutputStream outputStream = fs.create(file1, true);
//    outputStream.write(1);
//    outputStream.close();
//  }
}
