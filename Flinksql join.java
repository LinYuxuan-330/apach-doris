import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
            10014,
            4,
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
        
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env,
                       StreamTableEnvironment tEnv) {
        // 因为有 join: 默认所有表的数据都会一致存储到内存中. 所以要加 ttl
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        
        // 1. 读取 topic_db
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        // 2. 过滤出 insert 数据
        Table orderDetail = tEnv.sqlQuery(
            "select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "ts " +
                "from topic_db " +
                "where `database`='database' " +
                "and `table`='table' " +
                "and `type`='insert' ");
        tEnv.createTemporaryView("table", table);

        // 3. 过滤出 insert 数据
        Table orderDetailCoupon = tEnv.sqlQuery(
            "select " +
                "data['order_detail_id'] order_detail_id, " +
                "from topic_db " +
                "where `database`='database' " +
                "and `table`='table_detail' " +
                "and `type`='insert' ");
        tEnv.createTemporaryView("table_detail", table_detail);
        //orderDetailCoupon.execute().print();
        
        // 6. join:
        Table result = tEnv.sqlQuery(
            "select " +
                "od.id," +
                "od.order_id," +
                "od.ts " +
                "from table od " +
                "join table_detail oi on od.order_id=oi.order_detail_id ");
        
        result.executeInsert(DETAIL);
    }
}
