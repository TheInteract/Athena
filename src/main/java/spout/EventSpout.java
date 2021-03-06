package spout;

import common.JsonMapper;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import schema.AthenaInterface;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Chao on 3/31/2017 AD.
 */
public class EventSpout extends BaseRichSpout {

    static final long serialVersionUID = 737015318988609460L;
    static Logger LOG = Logger.getLogger(EventSpout.class);

    SpoutOutputCollector _collector;
    final String host;
    final int port;
    final String pattern;
    LinkedBlockingQueue<String> queue;
    JedisPool pool;
    ListenerThread listener;
    JsonMapper mapper;
    Class<? extends AthenaInterface> targetClass;
    String[] fields;
    Random rand;

    public EventSpout(String host, int port, String pattern, Class<? extends AthenaInterface> targetClass, String[] fields) {
        this.host = host;
        this.port = port;
        this.pattern = pattern;
        this.targetClass = targetClass;
        this.fields = fields;
    }

    class ListenerThread extends Thread {
        LinkedBlockingQueue<String> queue;
        JedisPool pool;
        String pattern;


        public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String pattern) {
            this.queue = queue;
            this.pool = pool;
            this.pattern = pattern;
        }

        public void run() {

            JedisPubSub listener = new JedisPubSub() {

                @Override
                public void onMessage(String channel, String message) {
                    queue.offer(message);
                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    queue.offer(message);
                }

                @Override
                public void onPSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onPUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }
            };

            Jedis jedis = this.pool.getResource();
            try {
                jedis.psubscribe(listener, this.pattern);
            } finally {
                jedis.close();
            }
        }
    };

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
        this.queue = new LinkedBlockingQueue<>(1000);
        this.pool = new JedisPool(this.host, this.port);
        this.mapper = new JsonMapper();
        this.rand = new Random();
        this.listener = new ListenerThread(queue,pool,pattern);
        this.listener.start();
    }


    public void close() {
        this.pool.destroy();
    }

    public void nextTuple() {
        String ret = this.queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            this._collector.emit(mapper.toValues(ret, targetClass), MessageId.generateId(rand));
        }
    }

    public void ack(Object msgId) {
        // TODO Auto-generated method stub

    }

    public void fail(Object msgId) {
        // TODO Auto-generated method stub

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.fields));
    }

    public boolean isDistributed() {
        return false;
    }
}
