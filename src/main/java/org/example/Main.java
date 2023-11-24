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
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

public class Main {
  private static final String BUCKET_NAME = "videos1";
  private static final String VOLUME_NAME = "assets";
  private static final String OZONE_URI_SCHEME = "o3fs";
  private static final String OZONE_FS_HSYNC_ENABLED = "ozone.fs.hsync.enabled";
  private static final ReplicationConfig REPLICATION_CONFIG =
      ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);


  // todo:
  //  Got conf form TestOzoneRpcClientWithRatis
  //  https://github.com/apache/ozone/blob/master/hadoop-ozone/integration-test/src/test/java/org/apache/hadoop/ozone/client/rpc/TestOzoneRpcClientWithRatis.java

  public static void main(String[] args) {
    try {
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
      conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
      conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
      conf.setBoolean(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, true);
      conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
      conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS, OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);

      OzoneClient rpcClient = OzoneClientFactory.getRpcClient(conf);
      ObjectStore objectStore = rpcClient.getObjectStore();
      OzoneVolume volume = lazyGetVolume(objectStore);
      OzoneBucket bucket = lazyGetBucket(volume, createBucketArgs());

//      writeFileWithHDFS(VOLUME_NAME, BUCKET_NAME);
      writeFileWithOzoneOutputStream(bucket);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void writeFileWithHDFS(String volumeName, String bucketName) throws IOException {
    String uri = String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucketName, volumeName);
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", uri);
    conf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);

    FileSystem fs = FileSystem.get(conf);

    Path file1 = new Path("key1");
    FSDataOutputStream outputStream = fs.create(file1, true);
    outputStream.write(1);
    outputStream.close();
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

  private static void writeFileWithOzoneOutputStream(OzoneBucket bucket) throws IOException {
    // read data from the file, this is a user provided function.
    String value = "sample value";
    String keyName = UUID.randomUUID().toString();

    OzoneOutputStream out = bucket.createKey(keyName, value.getBytes(StandardCharsets.UTF_8).length,
        REPLICATION_CONFIG,
        new HashMap<>());
    out.write(value.getBytes(StandardCharsets.UTF_8));
    out.close();
  }
}
