import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseInvertedIndex {

    // =============================================================
    // 1. Mapper 类
    // =============================================================
    public static class IndexMapper extends TableMapper<Text, Text> {
        
        // 假设你的列族名为 'info'，如果不同请修改这里
        private static final byte[] FAMILY = Bytes.toBytes("info");
        private static final byte[] QUALIFIER_TITLE = Bytes.toBytes("seg_title");
        private static final byte[] QUALIFIER_CONTENT = Bytes.toBytes("seg_content");

        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context) 
                throws IOException, InterruptedException {
            
            // 1. 获取 RowKey (URL)
            String url = Bytes.toString(row.get());
            
            // 2. 获取标题和正文内容
            String titleStr = Bytes.toString(value.getValue(FAMILY, QUALIFIER_TITLE));
            String contentStr = Bytes.toString(value.getValue(FAMILY, QUALIFIER_CONTENT));
            
            // 防止空指针
            if (titleStr == null) titleStr = "";
            if (contentStr == null) contentStr = "";

            // 3. 分词 (假设以空格分隔)
            String[] titleTokens = titleStr.trim().split("\\s+");
            String[] contentTokens = contentStr.trim().split("\\s+");

            int titleTotal = (titleStr.trim().isEmpty()) ? 0 : titleTokens.length;
            int contentTotal = (contentStr.trim().isEmpty()) ? 0 : contentTokens.length;

            // 4. 统计词频
            Map<String, Integer> titleCounts = new HashMap<>();
            for (String t : titleTokens) {
                if (!t.isEmpty()) titleCounts.put(t, titleCounts.getOrDefault(t, 0) + 1);
            }

            Map<String, Integer> contentCounts = new HashMap<>();
            for (String t : contentTokens) {
                if (!t.isEmpty()) contentCounts.put(t, contentCounts.getOrDefault(t, 0) + 1);
            }

            // 5. 合并所有出现的词 (去重)
            Map<String, Boolean> allWords = new HashMap<>();
            titleCounts.keySet().forEach(k -> allWords.put(k, true));
            contentCounts.keySet().forEach(k -> allWords.put(k, true));

            // 6. 输出结果
            // Key: Word
            // Value: URL : TitleCount : TitleTotal : ContentCount : ContentTotal
            for (String word : allWords.keySet()) {
                int tCount = titleCounts.getOrDefault(word, 0);
                int cCount = contentCounts.getOrDefault(word, 0);

                // 构建 Value 字符串
                String outputValue = String.format("%s:%d:%d:%d:%d", 
                        url, tCount, titleTotal, cCount, contentTotal);
                
                context.write(new Text(word), new Text(outputValue));
            }
        }
    }

    // =============================================================
    // 2. Reducer 类
    // =============================================================
    public static class IndexReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        private static final double W_TITLE = 5.0;
        private static final double W_CONTENT = 1.0;
        private long totalDocs = 1;

        @Override
        protected void setup(Context context) {
            totalDocs = context.getConfiguration().getLong("total.docs", 1);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            List<String> cache = new ArrayList<>();
            for (Text val : values) {
                cache.add(val.toString());
            }

            long df = cache.size();
            double idf = Math.log((double) totalDocs / (df + 1));

            for (String valStr : cache) {
                String[] parts = valStr.split(":");
                if (parts.length < 5) continue;

                String url = parts[0];
                int titleCount = Integer.parseInt(parts[1]);
                int titleTotal = Integer.parseInt(parts[2]);
                int contentCount = Integer.parseInt(parts[3]);
                int contentTotal = Integer.parseInt(parts[4]);

                double tfTitle = (titleTotal == 0) ? 0 : (double) titleCount / titleTotal;
                double tfContent = (contentTotal == 0) ? 0 : (double) contentCount / contentTotal;

                double score = (W_TITLE * tfTitle + W_CONTENT * tfContent) * idf;

                // [修改] 使用 copyBytes() 避免获取到 Text 复用缓冲区里的脏数据
                Put put = new Put(key.copyBytes());
                
                put.addColumn(Bytes.toBytes("p"), Bytes.toBytes(url), Bytes.toBytes(score));
                
                context.write(null, put);
            }
        }
    }

    // =============================================================
    // 3. Driver (Main)
    // =============================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        
        // 步骤 0: 统计源表 files 的总行数 N，用于计算 IDF
        // 在实际生产中这通常从元数据读取，这里为了作业完整性手动统计
        long totalDocs = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf("files"));
             ResultScanner scanner = table.getScanner(new Scan().setLimit(100000))) { // 预估限制或全表扫描
             // 注意：全表 count 在大数据量下很慢，作业场景数据量少可直接扫
             for (Result r : scanner) {
                 totalDocs++;
             }
        }
        System.out.println("Total Documents (N): " + totalDocs);
        conf.setLong("total.docs", totalDocs);

        // 步骤 1: 配置 Job
        Job job = Job.getInstance(conf, "HBase Inverted Index Builder");
        job.setJarByClass(HBaseInvertedIndex.class);

        // 步骤 2: 配置 Scan (读取源表)
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        // 如果只关心特定的列，可以 addColumn
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("seg_title"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("seg_content"));

        // 步骤 3: 初始化 Mapper
        TableMapReduceUtil.initTableMapperJob(
                "files",        // 源表名
                scan,           // Scan 实例
                IndexMapper.class,     // Mapper 类
                Text.class,     // Mapper 输出 Key 类型
                Text.class,     // Mapper 输出 Value 类型
                job);

        // 步骤 4: 初始化 Reducer
        TableMapReduceUtil.initTableReducerJob(
                "index",        // 目标表名
                IndexReducer.class,    // Reducer 类
                job);

        // 步骤 5: 提交运行
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}