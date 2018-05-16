package com.linus.spark_streaming;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author yuxuecheng
 * @version 1.0
 * @contact yuxuecheng@baicdata.com
 * @time 2018 May 14 15:08
 */
public class RtbLogStatRedisVersion {

    private static final String SHOW_FIELD = "show";
    private static final String CLICK_FIELD = "click";

    public static class AdState implements KryoSerializable, Comparable<AdState> {
        private static final String INIT_STATE_STR = "\u0000";
        private static final byte CREATIVED_STATE_MASK = 0x01;
        private static final byte SHOWED_STATE_MASK = 0x02;
        private static final byte CLICKED_STATE_MASK = 0x04;

        private static final String CREATIVED_STATE_STR = convertToCreatived (INIT_STATE_STR);
        private static final String SHOWED_STATE_STR = convertToShowed (INIT_STATE_STR);
        private static final String CLICKED_STATE_STR = convertToClicked (INIT_STATE_STR);

        private static final AdState INIT_STATE = new AdState (INIT_STATE_STR);
        private static final AdState CREATIVED_STATE = new AdState (CREATIVED_STATE_STR);
        private static final AdState SHOWED_STATE = new AdState (SHOWED_STATE_STR);
        private static final AdState CLICKED_STATE = new AdState (CLICKED_STATE_STR);

        private String state;

        private AdState () {
            this.state = INIT_STATE_STR;
        }

        private AdState (String state) {
            this.state = state;
        }

        /**
         * 判断状态当前是否可以切换到CREATIVE_STATE
         *
         * @return 状态当前是否可以切换到CREATIVE_STATE
         */
        public boolean isValidCreative () {
            return (state.getBytes ()[0] & CREATIVED_STATE_MASK) == 0;
        }

        /**
         * 判断当前状态是否可以切换到SHOWED_STATE
         *
         * @return 当前状态是否可以切换到SHOWED_STATE
         */
        public boolean isValidShow () {
            return (state.getBytes ()[0] & SHOWED_STATE_MASK) == 0;
        }

        /**
         * 判断当前状态是否可以切换到CLICKED_STATE
         *
         * @return 当前状态是否可以切换到CLICKED_STATE
         */
        public boolean isValidClick () {
            return (state.getBytes ()[0] & CLICKED_STATE_MASK) == 0;
        }

        /**
         * 判断当前状态中是否有CREATIVED_STATE
         *
         * @return 当前状态中是否有CREATIVED_STATE
         */
        private boolean hasCreative () {
            return (state.getBytes ()[0] & CREATIVED_STATE_MASK) != 0;
        }

        /**
         * 判断当前状态中是否有SHOWED_STATE
         *
         * @return 当前状态中是否有SHOWED_STATE
         */
        private boolean hasShow () {
            return (state.getBytes ()[0] & SHOWED_STATE_MASK) != 0;
        }

        /**
         * 判断当前状态中是否有CLICKED_STATE
         *
         * @return 当前状态中是否有CLICKED_STATE
         */
        private boolean hasClick () {
            return (state.getBytes ()[0] & CLICKED_STATE_MASK) != 0;
        }

        /**
         * 从当前状态切换到CREATIVE_STATE
         */
        public void convertToCreatived () {
            this.state = new String (new byte[]{(byte) (state.getBytes ()[0] | CREATIVED_STATE_MASK)});
        }

        /**
         * 从当前状态切换到SHOWED_STATE
         */
        public void convertToShowed () {
            this.state = new String (new byte[]{(byte) (state.getBytes ()[0] | SHOWED_STATE_MASK)});
        }

        /**
         * 从当前状态切换到CLICKED_STATE
         */
        public void convertToClicked () {
            this.state = new String (new byte[]{(byte) (state.getBytes ()[0] | CLICKED_STATE_MASK)});
        }

        /**
         * 设置CREATIVE_STATE
         *
         * @return 设置CREATIVE_STATE状态后的String
         */
        private static String convertToCreatived (String state) {
            return new String (new byte[]{(byte) (state.getBytes ()[0] | CREATIVED_STATE_MASK)});
        }

        /**
         * 设置SHOWED_STATE
         *
         * @return 设置SHOWED_STATE状态后的String
         */
        private static String convertToShowed (String state) {
            return new String (new byte[]{(byte) (state.getBytes ()[0] | SHOWED_STATE_MASK)});
        }

        /**
         * 设置CLICKED_STATE
         *
         * @return 设置CLICKED_STATE状态后的String
         */
        private static String convertToClicked (String state) {
            return new String (new byte[]{(byte) (state.getBytes ()[0] | CLICKED_STATE_MASK)});
        }

        @Override
        public void write (Kryo kryo, Output output) {
            kryo.writeObject (output, state);
        }

        @Override
        public void read (Kryo kryo, Input input) {
            state = kryo.readObject (input, String.class);
        }

        @Override
        public int compareTo (AdState o) {
            if (o == null) {
                return 1;
            }
            if (o == this) {
                return 0;
            }

            if (state.equals (o.state)) {
                return 0;
            }

            // 这里不能用Integer.parseInt，因为state是按byte操作的，生成的字符串并不是数字字符串，形式像'\u0003'
            int thisValue = this.state.getBytes ()[0];
            int otherValue = o.state.getBytes ()[0];

            if (thisValue == otherValue) {
                return 0;
            }

            return thisValue > otherValue ? 1 : -1;
        }

        @Override
        public boolean equals (Object o) {
            if (this == o) return true;
            if (o == null || getClass () != o.getClass ()) return false;
            AdState adState = (AdState) o;
            return Objects.equals (state, adState.state);
        }

        @Override
        public int hashCode () {

            return Objects.hash (state);
        }

        @Override
        public String toString () {
            return "AdState {" +
                    "has creative state: " + hasCreative () +
                    ", has show state: " + hasShow () +
                    ", has click state: " + hasClick () +
                    '}';
        }

        public static void main (String[] args) {
            AdState a = new AdState ();
            a.convertToCreatived ();
            a.convertToShowed ();
            System.out.println (a.equals (AdState.INIT_STATE));
            System.out.println (a.compareTo (AdState.INIT_STATE));
        }
    }


    static class ExtractAdPushIDAndState implements PairFunction<ConsumerRecord<String, String>, String, Tuple2<AdState, AdState>> {

        /**
         * 从每行bidder日志中抽取出adId和pushId作为key，根据keyword分别设置为CREATIVED、SHOWED和CLICKED状态
         *
         * @param record the log of bidder, key is null, value is one line of log
         * @return a Tuple2, first is the adId_pushId, second a Tuple2,
         *         first is the current state, always INIT, second is the event according to log keyword.
         * @throws Exception throws Exception
         */
        @Override
        public Tuple2<String, Tuple2<AdState, AdState>> call (ConsumerRecord<String, String> record) throws Exception {
            String value = record.value ();
            String[] tokens = value.split ("\u0001");
            System.out.println ("key: " + record.key () + ", value: " + record.value ());
            String key = tokens[6] + "_" + tokens[7];
            String logKey = tokens[0].trim ();
            switch (logKey) {
                case "rtb_creative":
                    System.out.println ("emit " + key + " creative event");
                    return new Tuple2<> (key, new Tuple2<> (new AdState (), new AdState (AdState.CREATIVED_STATE_STR)));
                case "rtb_show":
                    System.out.println ("emit " + key + " show event");
                    return new Tuple2<> (key, new Tuple2<> (new AdState (), new AdState (AdState.SHOWED_STATE_STR)));
                case "rtb_click":
                    System.out.println ("emit " + key + " click event");
                    return new Tuple2<> (key, new Tuple2<> (new AdState (), new AdState (AdState.CLICKED_STATE_STR)));
                default:
                    return new Tuple2<> ("", new Tuple2<> (new AdState (), new AdState ()));
            }
        }
    }

    static class UpdatePushIdState implements Function2<List<Tuple2<AdState, AdState>>, Optional<Tuple2<AdState, AdState>>, Optional<Tuple2<AdState, AdState>>> {
        /**
         * 更新状态信息
         *
         * @param events 从日志中解析出来的一系列更新状态的事件信息
         * @param state  当前这个key上一次窗口操作执行结束时的状态, Tuple中第一个元素是当前状态，第二个元素是切换过的状态
         * @return 当前这个key执行完本次窗口操作时的状态
         * @throws Exception throws some exception
         */
        @Override
        public Optional<Tuple2<AdState, AdState>> call (List<Tuple2<AdState, AdState>> events, Optional<Tuple2<AdState, AdState>> state) throws Exception {
            Tuple2<AdState, AdState> finalState = state.orElse (new Tuple2<> (new AdState (), new AdState ()));
            finalState = new Tuple2<> (finalState._1, new AdState ());
            List<Tuple2<AdState, AdState>> sortedEvents = new ArrayList<> (events);
            sortedEvents.sort ((o1, o2) -> {
                if (o1._1.equals (o2._1)) {
                    return o1._1.compareTo (o2._1);
                } else {
                    return o1._2.compareTo (o2._2);
                }
            });

            for (Tuple2<AdState, AdState> event : sortedEvents) {
                if (event._2.equals (AdState.CREATIVED_STATE) && finalState._1.isValidCreative ()) {
                    System.out.println ("Add creative state");
                    finalState._1.convertToCreatived ();
                    finalState._2.convertToCreatived ();
                } else if (event._2.equals (AdState.SHOWED_STATE) && finalState._1.isValidShow ()) {
                    System.out.println ("Add show state");
                    finalState._1.convertToShowed ();
                    finalState._2.convertToShowed ();
                } else if (event._2.equals (AdState.CLICKED_STATE) && finalState._1.isValidClick ()) {
                    System.out.println ("Add click state");
                    finalState._1.convertToClicked ();
                    finalState._2.convertToClicked ();
                }
            }

            System.out.println ("After update by key, current state: " + finalState._1 + ", updated state: " + finalState._2);
            return Optional.of (finalState);
        }
    }

    public static void main (String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.err.println ("Usage: kafka_spark_redis <brokers> <topics> <redisServer>\n" +
                    "  <brokers> Kafka broker列表\n" +
                    "  <topics> 要消费的topic列表\n" +
                    " <redisServer> redis 服务器地址 \n\n");
            System.exit (1);
        }

        /* 解析参数 */
        String brokers = args[0];
        String topics = args[1];
        String redisServer = args[2];

        String checkPointDir = "file:///tmp/redis_check_point";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate (checkPointDir, (Function0<JavaStreamingContext>) () -> {
            SparkConf conf = new SparkConf ()
                    .setAppName ("SparkStreamingRedisOp")
                    .set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            return new JavaStreamingContext (conf, Durations.seconds (1));
        });
        jssc.checkpoint (checkPointDir);
        JavaSparkContext jsc = jssc.sparkContext ();

        HashSet<String> topicSet = new HashSet<> (Arrays.asList (topics.split (",")));
        HashMap<String, Object> kafkaParams = new HashMap<> ();
        kafkaParams.put ("bootstrap.servers", brokers);
        kafkaParams.put ("key.deserializer", StringDeserializer.class);
        kafkaParams.put ("value.deserializer", StringDeserializer.class);
        kafkaParams.put ("group.id", "redis_test");
        kafkaParams.put ("auto.offset.reset", "latest");
//        kafkaParams.put ("auto.offset.reset", "earliest");
        kafkaParams.put ("enable.auto.commit", false);

        //创建redis连接池管理类
        RedisClient redisClient = new RedisClient (redisServer);

        // 创建Redis连接池管理对象
        final Broadcast<RedisClient> broadcastRedis = jsc.broadcast (redisClient);

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream (
                        jssc,
                        LocationStrategies.PreferConsistent (),
                        ConsumerStrategies.Subscribe (topicSet, kafkaParams)
                );

        Function2<List<Tuple2<AdState, AdState>>, Optional<Tuple2<AdState, AdState>>, Optional<Tuple2<AdState, AdState>>> updateKeyFunc = new UpdatePushIdState ();
        JavaPairDStream<String, Tuple2<AdState, AdState>> adPushIdState = stream
                .mapToPair (
                        new ExtractAdPushIDAndState ()
                ).window (Durations.seconds (30), Durations.seconds (10)
                ).filter ((Function<Tuple2<String, Tuple2<AdState, AdState>>, Boolean>) v1 -> !v1._1.isEmpty ()
                ).updateStateByKey (
                        updateKeyFunc
                ).filter ((Function<Tuple2<String, Tuple2<AdState, AdState>>, Boolean>) v1 -> {
                    AdState updatedState = v1._2._2;
                    System.out.println ("filter key : " + v1._1 + ", current state: " + v1._2._1 + ", updated state: " + updatedState);
                    System.out.println ("filtered value: " + updatedState.equals (AdState.INIT_STATE));
                    return !updatedState.equals (AdState.INIT_STATE);
                });

        adPushIdState.foreachRDD ((VoidFunction2<JavaPairRDD<String, Tuple2<AdState, AdState>>, Time>) (v1, v2) -> {
            System.out.println ("-------------------------------------------");
            System.out.println ("Time: " + v2);
            System.out.println ("-------------------------------------------");
            v1.foreachPartition ((VoidFunction<Iterator<Tuple2<String, Tuple2<AdState, AdState>>>>) valueIterator -> {
                String adPushId;
                RedisClient redisClient1 = broadcastRedis.getValue ();
                Jedis jedis = redisClient1.getResource ();
                Map<String, BigDecimal[]> adShowClickCount = new HashMap<> ();
                while (valueIterator.hasNext ()) {
                    Tuple2<String, Tuple2<AdState, AdState>> value = valueIterator.next ();
                    adPushId = value._1;
                    String[] tokens = adPushId.split ("_");
                    String adId = tokens[0];
                    AdState updatedState = value._2._2;
                    if (updatedState.hasShow ()) {
                        System.out.println ("Add show count for " + adId);
                        if (adShowClickCount.containsKey (adId)) {
                            BigDecimal[] showClickStat = adShowClickCount.get (adId);
                            showClickStat[0] = showClickStat[0].add (BigDecimal.ONE);
                        } else {
                            adShowClickCount.put (adId, new BigDecimal[]{BigDecimal.ONE, BigDecimal.ZERO});
                        }
                    }
                    if (updatedState.hasClick ()) {
                        System.out.println ("Add click count for " + adId);
                        if (adShowClickCount.containsKey (adId)) {
                            BigDecimal[] showClickStat = adShowClickCount.get (adId);
                            showClickStat[1] = showClickStat[1].add (BigDecimal.ONE);
                        } else {
                            adShowClickCount.put (adId, new BigDecimal[]{BigDecimal.ZERO, BigDecimal.ONE});
                        }
                    }
                }
                for (Map.Entry<String, BigDecimal[]> entry : adShowClickCount.entrySet ()) {
                    BigDecimal[] adShowClickStat = entry.getValue ();
                    System.out.println ("Add show count " + adShowClickStat[0].longValue () + " for adid " + entry.getKey ());
                    System.out.println ("Add click count " + adShowClickStat[1].longValue () + " for adid " + entry.getKey ());
                    jedis.hincrBy (entry.getKey (), SHOW_FIELD, adShowClickStat[0].longValue ());
                    jedis.hincrBy (entry.getKey (), CLICK_FIELD, adShowClickStat[1].longValue ());
                }
                jedis.close ();
            });
        });

        jssc.start ();
        jssc.awaitTermination ();
    }
}
