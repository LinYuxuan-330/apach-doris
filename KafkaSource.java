import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FlinkSourceUtil {
    /**
     * 封装 KafkaSource 的构建逻辑
     * @param groupId 消费组ID，用于标识 Kafka 消费组。
     * @param topic Kafka主题，指定要消费的主题。
     * @return 返回构建好的 KafkaSource，用于消费 Kafka 中的消息。
     */
    public static KafkaSource<String> getKafkaSource(String groupId,
                                                     String topic) {
        // 使用 KafkaSource 构建器创建 KafkaSource 实例
        return KafkaSource.<String>builder()
                // 设置 Kafka 集群的地址，这里需要指定 Kafka 的节点IP和端口号
                .setBootstrapServers(KAFKA_BROKERS)
                // 设置 Kafka 消费组ID
                .setGroupId(groupId)
                // 设置要消费的 Kafka 主题
                .setTopics(topic)
                // 设置从 Kafka 最新的偏移量开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                // 仅反序列化 Kafka 消息的 Value 部分，不考虑消息的 Key
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    
                    /**
                     * 反序列化方法，将 Kafka 的字节消息反序列化为字符串
                     * @param message 从 Kafka 接收到的消息字节数组
                     * @return 返回反序列化后的字符串
                     * @throws IOException 如果发生 I/O 异常，则抛出
                     */
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        // 判断消息是否为 null，如果不为 null，则将字节数据转换为 UTF-8 字符串
                        if (message != null) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        // 如果消息为 null，返回 null
                        return null;
                    }

                    /**
                     * 判断是否为流的末尾
                     * @param nextElement 下一条消息
                     * @return 如果为 false，表示流没有结束
                     */
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        // 此处始终返回 false，因为 Kafka 流是无界的
                        return false;
                    }

                    /**
                     * 获取生产的类型信息
                     * @return 返回生产的字符串类型
                     */
                    @Override
                    public TypeInformation<String> getProducedType() {
                        // 返回 Flink 内部的类型信息，表示处理的数据是字符串类型
                        return Types.STRING;
                    }
                })
                // 构建并返回 KafkaSource 实例
                .build();
    }
}
