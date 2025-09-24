package com.dzzxjl.mammut.spark.rocksdb;

import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class UserFeatureColumnFamilyDemo {
    static {
        RocksDB.loadLibrary();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws RocksDBException {
        String dbPath = "/tmp/rocksdb_user_features";

        // ==== 1. 配置 ColumnFamily ====
        try (final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {

            // 打开 DB 时要指定 ColumnFamily 列表
            List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()), // 默认 CF
                    new ColumnFamilyDescriptor("click_seq".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                    new ColumnFamilyDescriptor("play_seq".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions())
            );

            List<ColumnFamilyHandle> cfHandles = new java.util.ArrayList<>();

            try (final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, cfHandles)) {
                ColumnFamilyHandle defaultCF = cfHandles.get(0);
                ColumnFamilyHandle clickCF = cfHandles.get(1);
                ColumnFamilyHandle playCF = cfHandles.get(2);

                // ==== 2. 写入数据 ====
                // 点击序列
                db.put(clickCF, bytes("user:1001:20250924T0901"), bytes("item_click_A"));
                db.put(clickCF, bytes("user:1001:20250924T0905"), bytes("item_click_B"));
                db.put(clickCF, bytes("user:1002:20250924T0902"), bytes("item_click_X"));

                // 播放序列
                db.put(playCF, bytes("user:1001:20250924T0910"), bytes("video_play_A"));
                db.put(playCF, bytes("user:1001:20250924T0915"), bytes("video_play_B"));
                db.put(playCF, bytes("user:1002:20250924T0912"), bytes("video_play_Y"));

                // ==== 3. 扫描某个用户的点击序列 ====
                System.out.println("用户 user:1001 的点击序列：");
                try (RocksIterator iter = db.newIterator(clickCF)) {
                    for (iter.seek(bytes("user:1001:")); iter.isValid(); iter.next()) {
                        String key = new String(iter.key(), StandardCharsets.UTF_8);
                        if (!key.startsWith("user:1001:")) break;
                        String value = new String(iter.value(), StandardCharsets.UTF_8);
                        System.out.printf("  %s -> %s%n", key, value);
                    }
                }

                // ==== 4. 扫描某个用户的播放序列 ====
                System.out.println("用户 user:1001 的播放序列：");
                try (RocksIterator iter = db.newIterator(playCF)) {
                    for (iter.seek(bytes("user:1001:")); iter.isValid(); iter.next()) {
                        String key = new String(iter.key(), StandardCharsets.UTF_8);
                        if (!key.startsWith("user:1001:")) break;
                        String value = new String(iter.value(), StandardCharsets.UTF_8);
                        System.out.printf("  %s -> %s%n", key, value);
                    }
                }

                // ==== 5. 单条查询 ====
                byte[] clickEvent = db.get(clickCF, bytes("user:1001:20250924T0905"));
                System.out.println("单条点击查询: " +
                        new String(clickEvent, StandardCharsets.UTF_8));

                byte[] playEvent = db.get(playCF, bytes("user:1001:20250924T0915"));
                System.out.println("单条播放查询: " +
                        new String(playEvent, StandardCharsets.UTF_8));

                // 释放 ColumnFamilyHandle
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }
}
