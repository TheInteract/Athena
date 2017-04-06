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
    private static final int mongoPort = 27017;
    private static final int redisPort = 6379;
    private static boolean isProduction;
    private static String dbName;
    private static String redisHost;
    private static String mongoHost;
    private static String url;
    private static TopologyBuilder builder;
    private static String[] mouseClickField, loadField;


    private static void setupSpout() {
        builder.setSpout("mouseClickSpout", new EventSpout(redisHost,redisPort,"onclick", MouseClick.class, mouseClickField));
        builder.setSpout("loadSpout", new EventSpout(redisHost,redisPort,"onload", Load.class, loadField));
    }

    private static String[] replaceFieldElement(String[] input, String from, String to) {
        String[] tempArray = new String[input.length];
        for(int i = 0; i < input.length; i++) {
            tempArray[i] = input[i];
            if (from != null && to != null && tempArray[i].equals(from)) {
                tempArray[i] = to;
            }
        }
        return tempArray;

    }

    private static String[] addFieldElement(String[] array, String push) {
        String[] longer = new String[array.length + 1];
        for (int i = 0; i < array.length; i++)
            longer[i] = array[i];
        longer[array.length] = push;
        return longer;
    }

    public static void main(String[] args) {
        isProduction = args[0] == null;
        dbName = isProduction ? "/Interact" : "/interact";
        redisHost = args[0] != null ? args[0] : System.getenv("MONGO_PORT_27017_TCP_ADDR");
        mongoHost = args[1] != null ? args[1] : System.getenv("REDIS_MASTER_PORT_6379_TCP_ADDR");
        url = "mongodb://" + mongoHost + ":" + mongoPort + dbName;
        builder = new TopologyBuilder();

        mouseClickField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "deviceCode", "userCode", "timeStamp", "target"};
        loadField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "deviceCode", "userCode", "timeStamp", "url", "scrollX", "scrollY", "innerHeight", "innerWidth", "appCode", "appName", "appVersion"};

        setupSpout();


        String[] productFinderFields = replaceFieldElement(loadField, "API_KEY_PUBLIC", "productId");
        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields(productFinderFields);
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("productFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 1).shuffleGrouping("loadSpout");


        String[] userFinderFields = replaceFieldElement(productFinderFields, "userCode", "userId");
        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields(userFinderFields);
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("userFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 1).shuffleGrouping("productFinderBolt");

        String[] sessionTypeCreatorFields = addFieldElement(productFinderFields, "sessionTypeId");
        MongoLookupMapper mouseClickMapper = new AthenaLookupMapper()
                .withFields(sessionTypeCreatorFields);
        AthenaQueryFilterCreator mouseClickFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "url", "type");
        builder.setBolt("sessionCreatorBolt", new SessionTypeCreatorBolt(url, "sessionTypes",
                mouseClickFilterCreator, mouseClickMapper), 1).shuffleGrouping("userFinderBolt");
//        MongoLookupMapper productMapper = new AthenaLookupMapper()
//                .withFields("userCode", "deviceCode", "productId", "issueTime", "target", "timeStamp");
//        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
//                .withField("API_KEY_PUBLIC");
//        builder.setBolt("productFinderBolt", new ProductFinderBolt(url, "product",
//                procuctFilterCreator, productMapper), 1).shuffleGrouping("mouseClickSpout");
//
//
//        MongoLookupMapper userMapper = new AthenaLookupMapper()
//                .withFields("deviceCode", "productId", "issueTime", "target", "timeStamp", "userId");
//        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
//                .withField("deviceCode", "userCode");
//        builder.setBolt("userFinderBolt", new UserFinderBolt(url, "user",
//                userFilterCreator, userMapper), 1).shuffleGrouping("productFinderBolt");
//
//
//        MongoLookupMapper mouseClickMapper = new AthenaLookupMapper()
//                .withFields("userId", "productId", "deviceCode", "issueTime", "target", "timeStamp");
//        AthenaQueryFilterCreator mouseClickFilterCreator = new AthenaQueryFilterCreator()
//                .withField("userId", "deviceCode", "productId");
//        builder.setBolt("userClickBolt", new UserProcessBolt(url, "processresult",
//                mouseClickFilterCreator, mouseClickMapper), 1).shuffleGrouping("userFinderBolt");
//
//
//        MongoLookupMapper mouseClickBoltMapper = new AthenaLookupMapper()
//                .withFields("userId", "productId", "deviceCode", "issueTime", "target", "timeStamp");
//        MongoLookupMapper resultUpdateMapper = new AthenaLookupMapper().withFields("userId", "productId", "deviceCode", "actionId");
//        builder.setBolt("actionClickBolt", new MouseClickBolt(url, "mouseclickaction", mouseClickBoltMapper, resultUpdateMapper), 1).shuffleGrouping("userClickBolt");
//
//
//        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId");
//        builder.setBolt("updateClickBolt", new ResultUpdateBolt(url, "processresult", updateFilterCreator, "mouseclick").withUpsert(false).withMany(false), 1).shuffleGrouping("actionClickBolt");
//


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
