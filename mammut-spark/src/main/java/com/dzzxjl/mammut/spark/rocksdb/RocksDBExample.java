package com.dzzxjl.mammut.spark.rocksdb;

import org.rocksdb.*;

public class RocksDBExample {
    static {
        RocksDB.loadLibrary(); // 加载 RocksDB 的 JNI 库
    }

    public static void main(String[] args) throws RocksDBException {
        // RocksDB 数据目录
        String dbPath = "/tmp/rocksdb_example";

        // RocksDB 配置
        Options options = new Options().setCreateIfMissing(true);

        // 打开或创建 DB
        try (RocksDB db = RocksDB.open(options, dbPath)) {
            // 写入数据
            db.put("user:1".getBytes(), "Alice".getBytes());
            db.put("user:2".getBytes(), "Bob".getBytes());

            // 读取数据
            byte[] value = db.get("user:1".getBytes());
            System.out.println("user:1 = " + new String(value));

            // 删除数据
            db.delete("user:2".getBytes());
        }
    }
}
