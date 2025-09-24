package com.dzzxjl.mammut.spark.rocksdb;

import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class RocksDBFlowDemo {
    static {
        RocksDB.loadLibrary();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void printSSTFiles(String dbPath) {
        File dir = new File(dbPath);
        if (!dir.exists()) return;

        System.out.println("==== 当前 SST 文件列表 ====");
        for (File f : dir.listFiles()) {
            if (f.getName().endsWith(".sst")) {
                System.out.println(f.getName());
            }
        }
    }

    public static void main(String[] args) throws RocksDBException, InterruptedException {
        String dbPath = "/tmp/rocksdb_demo";

        // 清理历史数据
        File dir = new File(dbPath);
        if (dir.exists()) {
            for (File f : dir.listFiles()) f.delete();
        }

        // 1. DB 配置
        try (DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundCompactions(2) // 后台 compaction
        ) {

            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();

            ColumnFamilyDescriptor defaultCF = new ColumnFamilyDescriptor(
                    RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions);

            try (RocksDB db = RocksDB.open(dbOptions, dbPath,
                    java.util.Arrays.asList(defaultCF),
                    new java.util.ArrayList<>())) {

                // 2. 写入 MemTable 数据
                System.out.println("写入 MemTable 数据...");
                for (int i = 0; i < 10; i++) {
                    String key = "user:1001:" + (9000 + i);
                    String value = "click_" + i;
                    db.put(bytes(key), bytes(value));
                    System.out.println("写入: " + key + " -> " + value);
                }

                // 查看 MemTable 还没有 Flush 时 SST 文件
                printSSTFiles(dbPath);

                // 3. 强制 Flush MemTable
                System.out.println("\n执行 flush...");
                db.flush(new FlushOptions().setWaitForFlush(true));

                // Flush 后 SST 文件已经生成
                printSSTFiles(dbPath);

                // 4. 触发 Compaction
                System.out.println("\n触发 compaction...");
                db.compactRange();

                // 查看 Compaction 后 SST 文件
                printSSTFiles(dbPath);

                // 5. 前缀扫描
                System.out.println("\n前缀扫描 user:1001:");
                ReadOptions readOptions = new ReadOptions().setPrefixSameAsStart(true);
                try (RocksIterator iter = db.newIterator(readOptions)) {
                    iter.seek(bytes("user:1001:"));
                    while (iter.isValid()) {
                        System.out.printf("  %s -> %s%n",
                                new String(iter.key(), StandardCharsets.UTF_8),
                                new String(iter.value(), StandardCharsets.UTF_8));
                        iter.next();
                    }
                }
            }
        }

        System.out.println("\n流程结束，文件存储路径：" + dbPath);
    }
}
