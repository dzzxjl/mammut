package com.dzzxjl.mammut.spark.rocksdb;

import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RocksDBMultiSSTDemo {

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

    public static void main(String[] args) throws Exception {
        String dbPath = "/tmp/rocksdb_demo_multi";

        // 清理历史数据
        File dir = new File(dbPath);
        if (dir.exists()) {
            for (File f : dir.listFiles()) f.delete();
        }

        // 配置 RocksDB
        try (Options options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(1024) // 1KB 小 buffer，容易触发 flush
                .setMaxBackgroundCompactions(2)
                .setLevel0FileNumCompactionTrigger(2) // L0 超过 2 个文件就触发 compaction
        ) {

            try (RocksDB db = RocksDB.open(options, dbPath)) {

                // 写入大量数据，触发多次 flush
                System.out.println("写入大量数据...");
                for (int i = 0; i < 200; i++) {
                    String key = "user:1001:" + i;
                    String value = "click_" + i;
                    db.put(bytes(key), bytes(value));

                    // 每写 10 条数据，强制 flush 一次，让 SST 文件生成更快
                    if (i % 10 == 0) {
                        db.flush(new FlushOptions().setWaitForFlush(true));
                    }
                }

                // 打印 L0 SST 文件
                System.out.println("\n写入完成后 SST 文件 (L0)：");
                printSSTFiles(dbPath);

                // 触发 compaction，把 L0 合并到 L1
                System.out.println("\n触发 compaction...");
                db.compactRange();

                // 再次打印 SST 文件，观察 Level-1 生成
                System.out.println("\nCompaction 后 SST 文件 (L1)：");
                printSSTFiles(dbPath);

                // 遍历所有 key-value
                System.out.println("\n迭代打印 key-value:");
                try (RocksIterator iter = db.newIterator()) {
                    iter.seekToFirst();
                    while (iter.isValid()) {
                        System.out.printf("  %s -> %s%n",
                                new String(iter.key(), StandardCharsets.UTF_8),
                                new String(iter.value(), StandardCharsets.UTF_8));
                        iter.next();
                    }
                }
            }
        }

        System.out.println("\n流程结束，SST 文件存储路径：" + dbPath);
    }
}
