package com.dzzxjl.mammut.spark.rocksdb;

import org.rocksdb.*;

import java.nio.charset.StandardCharsets;

public class UserClickDemo {
    static {
        RocksDB.loadLibrary(); // 加载 RocksDB JNI 库
    }

    // 工具方法：字节数组和字符串互转
    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws RocksDBException {
        String dbPath = "/tmp/rocksdb_user_clicks"; // RocksDB 数据目录

        // RocksDB 配置
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB db = RocksDB.open(options, dbPath)) {

                // ==== 1. 写入一些模拟的用户点击数据 ====
                // key: userId:timestamp, value: itemId
                db.put(bytes("user:1001:202509240901"), bytes("video_A"));
                db.put(bytes("user:1001:202509240903"), bytes("video_B"));
                db.put(bytes("user:1001:202509240905"), bytes("video_C"));
                db.put(bytes("user:1002:202509240902"), bytes("video_X"));
                db.put(bytes("user:1002:202509240906"), bytes("video_Y"));

                // ==== 2. 扫描一个用户的点击序列 ====
                String userId = "user:1001";
                System.out.println("扫描用户 " + userId + " 的点击历史：");

                try (RocksIterator iterator = db.newIterator()) {
                    // 定位到该用户的 key 前缀
                    for (iterator.seek(bytes(userId + ":")); iterator.isValid(); iterator.next()) {
                        String key = new String(iterator.key(), StandardCharsets.UTF_8);
                        if (!key.startsWith(userId + ":")) {
                            break; // 该用户的点击序列扫描完毕
                        }
                        String value = new String(iterator.value(), StandardCharsets.UTF_8);
                        System.out.printf("  %s -> %s%n", key, value);
                    }
                }

                // ==== 3. 取单条数据 ====
                byte[] singleClick = db.get(bytes("user:1001:202509240903"));
                if (singleClick != null) {
                    System.out.println("单条查询: user:1001:202509240903 -> " +
                            new String(singleClick, StandardCharsets.UTF_8));
                }
            }
        }
    }
}
