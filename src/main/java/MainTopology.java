import bolt.*;
import common.AthenaLookupMapper;
import common.AthenaQueryFilterCreator;
import common.MongoLookupMapper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import schema.Load;
import schema.MouseClick;
import spout.EventSpout;

/**
 * Created by Chao on 3/31/2017 AD.
 */
public class MainTopology {
    public static void main(String[] args) {
        int mongoPort = 27017;
        int redisPort = 6379;
        boolean isProduction = args[0] == null;
        String dbName = isProduction ? "/Interact" : "/interact";
        String redisHost = args[0] != null ? args[0] : System.getenv("MONGO_PORT_27017_TCP_ADDR");
        String mongoHost = args[1] != null ? args[1] : System.getenv("REDIS_MASTER_PORT_6379_TCP_ADDR");
        String url = "mongodb://" + mongoHost + ":" + mongoPort + dbName;
        TopologyBuilder builder = new TopologyBuilder();


        String[] mouseClickField = {"issueTime", "type", "API_KEY_PUBLIC", "deviceCode", "userCode", "timeStamp", "target"};
        builder.setSpout("mouseClickSpout", new EventSpout(redisHost,redisPort,"onclick", MouseClick.class, mouseClickField));

        String[] loadField = {"issueTime", "type", "API_KEY_PUBLIC", "deviceCode", "userCode", "timeStamp", "url", "scrollX", "scrollY", "innerHeight", "innerWidth", "appCode", "appName", "appVersion"};
        builder.setSpout("loadSpout", new EventSpout(redisHost,redisPort,"onload", Load.class, loadField));

        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields("userCode", "deviceCode", "productId", "issueTime", "target", "timeStamp");
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("productFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 1).shuffleGrouping("mouseClickSpout");


        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields("deviceCode", "productId", "issueTime", "target", "timeStamp", "userId");
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("userFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 1).shuffleGrouping("productFinderBolt");


        MongoLookupMapper mouseClickMapper = new AthenaLookupMapper()
                .withFields("userId", "productId", "deviceCode", "issueTime", "target", "timeStamp");
        AthenaQueryFilterCreator mouseClickFilterCreator = new AthenaQueryFilterCreator()
                .withField("userId", "deviceCode", "productId");
        builder.setBolt("userClickBolt", new UserProcessBolt(url, "processresult",
                mouseClickFilterCreator, mouseClickMapper), 1).shuffleGrouping("userFinderBolt");


        MongoLookupMapper mouseClickBoltMapper = new AthenaLookupMapper()
                .withFields("userId", "productId", "deviceCode", "issueTime", "target", "timeStamp");
        MongoLookupMapper resultUpdateMapper = new AthenaLookupMapper().withFields("userId", "productId", "deviceCode", "actionId");
        builder.setBolt("actionClickBolt", new MouseClickBolt(url, "mouseclickaction", mouseClickBoltMapper, resultUpdateMapper), 1).shuffleGrouping("userClickBolt");


        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId");
        builder.setBolt("updateClickBolt", new ResultUpdateBolt(url, "processresult", updateFilterCreator, "mouseclick").withUpsert(false).withMany(false), 1).shuffleGrouping("actionClickBolt");



        Config conf = new Config();
        conf.setDebug(true);


        if (!isProduction) {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("AthenaLocal", conf, builder.createTopology());
            Utils.sleep(1500000);
            cluster.killTopology("test");
            cluster.shutdown();
        } else {
            try {
                StormSubmitter.submitTopology("AthenaProduction", conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}
