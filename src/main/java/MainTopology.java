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
import org.apache.storm.shade.org.apache.commons.lang.ArrayUtils;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import schema.*;
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
    private static String[] mouseClickField, loadField, APICallField, focusField, blurField, unloadField;


    private static void setupSpout() {

        builder.setSpout("loadSpout", new EventSpout(redisHost,redisPort,"onload", Load.class, loadField));
        builder.setSpout("focusSpout", new EventSpout(redisHost,redisPort,"onfocus", Focus.class, focusField));
        builder.setSpout("mouseClickSpout", new EventSpout(redisHost,redisPort,"onclick", MouseClick.class, mouseClickField));
        builder.setSpout("APICallSpout", new EventSpout(redisHost,redisPort,"onAPICall", APICall.class, APICallField));
        builder.setSpout("blurSpout", new EventSpout(redisHost,redisPort,"onblur", Blur.class, blurField));
//        builder.setSpout("unloadSpout", new EventSpout(redisHost,redisPort,"onunload", Unload.class, unloadField));
    }

    private static void setupLoadBolts() {
        String[] productFinderFields = replaceFieldElement(loadField, "API_KEY_PUBLIC", "productId");
        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields(productFinderFields);
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("loadProductFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 2).shuffleGrouping("loadSpout");


        String[] userFinderFields = replaceFieldElement(productFinderFields, "userCode", "userId");
        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields(userFinderFields);
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("loadUserFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 2).shuffleGrouping("loadProductFinderBolt");


        String[] actionTypeCreatorFields = addFieldElement(userFinderFields, "actionTypeId");
        MongoLookupMapper actionTypeMapper = new AthenaLookupMapper()
                .withFields(actionTypeCreatorFields);
        AthenaQueryFilterCreator actionTypeFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "type", "url");
        builder.setBolt("loadActionTypeCreatorBolt", new ActionTypeCreatorBolt(url, "actionType",
                actionTypeFilterCreator, actionTypeMapper), 2).shuffleGrouping("loadUserFinderBolt");


        String[] sessionTypeCreatorFields = addFieldElement(actionTypeCreatorFields, "sessionTypeId");
        MongoLookupMapper sessionTypeMapper = new AthenaLookupMapper()
                .withFields(sessionTypeCreatorFields);
        AthenaQueryFilterCreator sessionTypeFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "url");
        builder.setBolt("loadSessionTypeCreatorBolt", new SessionTypeCreatorBolt(url, "sessionType",
                sessionTypeFilterCreator, sessionTypeMapper), 2).shuffleGrouping("loadActionTypeCreatorBolt");


        String[] sessionInsertFields = {"userId", "productId", "deviceCode", "issueTime", "sessionTypeId", "versions", "sessionCode"};
        String[] sessionCreatorFields = addFieldElement(sessionTypeCreatorFields, "_id");
        MongoLookupMapper sessionInsertMapper = new AthenaLookupMapper()
                .withFields(sessionInsertFields);
        MongoLookupMapper sessionEmitMapper = new AthenaLookupMapper().withFields(sessionCreatorFields);
        builder.setBolt("loadSessionCreatorBolt", new SessionCreatorBolt(url, "session", sessionInsertMapper, sessionEmitMapper), 2).shuffleGrouping("loadSessionTypeCreatorBolt");


        String[] actionFields = {"issueTime", "type", "actionTypeId", "timeStamp", "url", "scrollX", "scrollY", "innerHeight", "innerWidth", "appCode", "appName", "appVersion"};
        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId", "sessionCode");
        builder.setBolt("loadUpdateClickBolt", new ActionPusherBolt(url, "session", updateFilterCreator, actionFields).withUpsert(false).withMany(false), 2).shuffleGrouping("loadSessionCreatorBolt");
    }

    private static void setupFocusBolts() {
        String[] productFinderFields = replaceFieldElement(focusField, "API_KEY_PUBLIC", "productId");
        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields(productFinderFields);
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("focusProductFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 1).shuffleGrouping("focusSpout");


        String[] userFinderFields = replaceFieldElement(productFinderFields, "userCode", "userId");
        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields(userFinderFields);
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("focusUserFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 1).shuffleGrouping("focusProductFinderBolt");


        String[] actionTypeCreatorFields = addFieldElement(userFinderFields, "actionTypeId");
        MongoLookupMapper actionTypeMapper = new AthenaLookupMapper()
                .withFields(actionTypeCreatorFields);
        AthenaQueryFilterCreator actionTypeFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "type", "url");
        builder.setBolt("focusActionTypeCreatorBolt", new ActionTypeCreatorBolt(url, "actionType",
                actionTypeFilterCreator, actionTypeMapper), 1).shuffleGrouping("focusUserFinderBolt");

        String[] actionFields = {"issueTime", "type", "actionTypeId", "timeStamp", "url", "innerHeight", "innerWidth"};
        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId", "sessionCode");
        builder.setBolt("focusUpdateClickBolt", new ActionPusherBolt(url, "session", updateFilterCreator, actionFields).withUpsert(false).withMany(false), 1).shuffleGrouping("focusActionTypeCreatorBolt");
    }

    private static void setupMouseClickBolts() {
        String[] productFinderFields = replaceFieldElement(mouseClickField, "API_KEY_PUBLIC", "productId");
        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields(productFinderFields);
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("mouseClickProductFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 1).shuffleGrouping("mouseClickSpout");


        String[] userFinderFields = replaceFieldElement(productFinderFields, "userCode", "userId");
        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields(userFinderFields);
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("mouseClickUserFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 1).shuffleGrouping("mouseClickProductFinderBolt");


        String[] actionTypeCreatorFields = addFieldElement(userFinderFields, "actionTypeId");
        MongoLookupMapper actionTypeMapper = new AthenaLookupMapper()
                .withFields(actionTypeCreatorFields);
        AthenaQueryFilterCreator actionTypeFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "type", "target");
        builder.setBolt("mouseClickActionTypeCreatorBolt", new ActionTypeCreatorBolt(url, "actionType",
                actionTypeFilterCreator, actionTypeMapper), 1).shuffleGrouping("mouseClickUserFinderBolt");


        String[] actionFields = {"issueTime", "type", "actionTypeId", "timeStamp", "target"};
        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId", "sessionCode");
        builder.setBolt("mouseClickUpdateClickBolt", new ActionPusherBolt(url, "session", updateFilterCreator, actionFields).withUpsert(false).withMany(false), 1).shuffleGrouping("mouseClickActionTypeCreatorBolt");
    }

    private static void setupAPICallBolts() {
        String[] productFinderFields = replaceFieldElement(APICallField, "API_KEY_PUBLIC", "productId");
        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields(productFinderFields);
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("APICallProductFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 1).shuffleGrouping("APICallSpout");


        String[] userFinderFields = replaceFieldElement(productFinderFields, "userCode", "userId");
        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields(userFinderFields);
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("APICallUserFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 1).shuffleGrouping("APICallProductFinderBolt");


        String[] actionTypeCreatorFields = addFieldElement(userFinderFields, "actionTypeId");
        MongoLookupMapper actionTypeMapper = new AthenaLookupMapper()
                .withFields(actionTypeCreatorFields);
        AthenaQueryFilterCreator actionTypeFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "type", "endpoint");
        builder.setBolt("APICallActionTypeCreatorBolt", new ActionTypeCreatorBolt(url, "actionType",
                actionTypeFilterCreator, actionTypeMapper), 1).shuffleGrouping("APICallUserFinderBolt");


        String[] actionFields = {"issueTime", "type", "actionTypeId", "endpoint", "method"};
        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId", "sessionCode");
        builder.setBolt("APICallUpdateClickBolt", new ActionPusherBolt(url, "session", updateFilterCreator, actionFields).withUpsert(false).withMany(false), 1).shuffleGrouping("APICallActionTypeCreatorBolt");
    }

    private static void setupBlurBolts() {
        String[] productFinderFields = replaceFieldElement(blurField, "API_KEY_PUBLIC", "productId");
        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields(productFinderFields);
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("blurProductFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 1).shuffleGrouping("blurSpout");


        String[] userFinderFields = replaceFieldElement(productFinderFields, "userCode", "userId");
        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields(userFinderFields);
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("blurUserFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 1).shuffleGrouping("blurProductFinderBolt");


        String[] actionTypeCreatorFields = addFieldElement(userFinderFields, "actionTypeId");
        MongoLookupMapper actionTypeMapper = new AthenaLookupMapper()
                .withFields(actionTypeCreatorFields);
        AthenaQueryFilterCreator actionTypeFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "type", "url");
        builder.setBolt("blurActionTypeCreatorBolt", new ActionTypeCreatorBolt(url, "actionType",
                actionTypeFilterCreator, actionTypeMapper), 1).shuffleGrouping("blurUserFinderBolt");


        String[] actionFields = {"issueTime", "type", "actionTypeId", "timeStamp", "url"};
        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId", "sessionCode");
        builder.setBolt("blurUpdateClickBolt", new ActionPusherBolt(url, "session", updateFilterCreator, actionFields).withUpsert(false).withMany(false), 1).shuffleGrouping("blurActionTypeCreatorBolt");
    }

    private static void setupUnloadBolts() {
        String[] productFinderFields = replaceFieldElement(unloadField, "API_KEY_PUBLIC", "productId");
        MongoLookupMapper productMapper = new AthenaLookupMapper()
                .withFields(productFinderFields);
        AthenaQueryFilterCreator procuctFilterCreator = new AthenaQueryFilterCreator()
                .withField("API_KEY_PUBLIC");
        builder.setBolt("unloadProductFinderBolt", new ProductFinderBolt(url, "product",
                procuctFilterCreator, productMapper), 1).shuffleGrouping("unloadSpout");


        String[] userFinderFields = replaceFieldElement(productFinderFields, "userCode", "userId");
        MongoLookupMapper userMapper = new AthenaLookupMapper()
                .withFields(userFinderFields);
        AthenaQueryFilterCreator userFilterCreator = new AthenaQueryFilterCreator()
                .withField("deviceCode", "userCode");
        builder.setBolt("unloadUserFinderBolt", new UserFinderBolt(url, "user",
                userFilterCreator, userMapper), 1).shuffleGrouping("unloadProductFinderBolt");


        String[] actionTypeCreatorFields = addFieldElement(userFinderFields, "actionTypeId");
        MongoLookupMapper actionTypeMapper = new AthenaLookupMapper()
                .withFields(actionTypeCreatorFields);
        AthenaQueryFilterCreator actionTypeFilterCreator = new AthenaQueryFilterCreator()
                .withField("productId", "type");
        builder.setBolt("unloadActionTypeCreatorBolt", new ActionTypeCreatorBolt(url, "actionType",
                actionTypeFilterCreator, actionTypeMapper), 1).shuffleGrouping("unloadUserFinderBolt");


        String[] actionFields = {"issueTime", "type", "actionTypeId"};
        AthenaQueryFilterCreator updateFilterCreator = new AthenaQueryFilterCreator().withField("userId", "deviceCode", "productId", "sessionCode");
        builder.setBolt("unloadUpdateClickBolt", new ActionPusherBolt(url, "session", updateFilterCreator, actionFields).withUpsert(false).withMany(false), 1).shuffleGrouping("unloadActionTypeCreatorBolt");
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

    private static String[] combineField(String[] one, String[] two) {
        return (String[]) ArrayUtils.addAll(one, two);
    }

    public static void main(String[] args) {
        isProduction = args.length == 0;
        dbName = isProduction ? "/Interact" : "/interact";
        redisHost = args.length > 0 ? args[0] : System.getenv("REDIS_MASTER_PORT_6379_TCP_ADDR");
        mongoHost = args.length > 0 ? args[1] : System.getenv("MONGO_PORT_27017_TCP_ADDR");
        url = "mongodb://" + mongoHost + ":" + mongoPort + dbName;
        builder = new TopologyBuilder();
        System.out.println("Redis host = " + redisHost);
        System.out.println("Mongo host = " + mongoHost);
        System.out.println("Mongo url = " + url);

        loadField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "versions", "deviceCode", "userCode", "sessionCode", "timeStamp", "url", "scrollX", "scrollY", "innerHeight", "innerWidth", "appCode", "appName", "appVersion"};
        focusField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "versions", "deviceCode", "userCode", "sessionCode", "timeStamp", "url", "innerHeight", "innerWidth"};
        mouseClickField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "versions", "deviceCode", "userCode", "sessionCode", "timeStamp", "target"};
        APICallField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "versions", "deviceCode", "userCode", "sessionCode", "endpoint", "method"};
        blurField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "versions", "deviceCode", "userCode", "sessionCode", "timeStamp", "url"};
        unloadField = new String[] {"issueTime", "type", "API_KEY_PUBLIC", "versions", "deviceCode", "userCode", "sessionCode"};

        setupSpout();

        setupLoadBolts();
        setupFocusBolts();
        setupMouseClickBolts();
        setupAPICallBolts();
        setupBlurBolts();
//        setupUnloadBolts();


        Config conf = new Config();
        if (!isProduction) {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("AthenaLocal", conf, builder.createTopology());
            Utils.sleep(1500000);
            cluster.killTopology("AthenaLocal");
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
