import bolt.MouseClickBolt;
import bolt.UserProcessBolt;
import common.AthenaLookupMapper;
import common.AthenaQueryFilterCreator;
import common.MongoLookupMapper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import spout.MouseClickSpout;

/**
 * Created by Chao on 3/31/2017 AD.
 */
public class MainTopology {
    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String url = "mongodb://127.0.0.1:27017/interact";
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("mouseClickSpout", new MouseClickSpout(host,port,"onclick"));

        MongoLookupMapper mouseClickMapper = new AthenaLookupMapper()
                .withFields("userId", "deviceCode", "productId", "issueTime", "target", "timeStamp", "_id");
        AthenaQueryFilterCreator mouseClickFilterCreator = new AthenaQueryFilterCreator()
                .withField("userId", "deviceCode", "productId");

        builder.setBolt("userClickBolt", new UserProcessBolt(url, "processresult",
                mouseClickFilterCreator, mouseClickMapper), 2).shuffleGrouping("mouseClickSpout");

        MongoLookupMapper mouseClickBoltMapper = new AthenaLookupMapper()
                .withFields("userId", "deviceCode", "productId", "issueTime", "target", "timeStamp");
        builder.setBolt("actionClickBolt", new MouseClickBolt(url, "mouseclickaction", mouseClickBoltMapper), 2).shuffleGrouping("userClickBolt");
        Config conf = new Config();
        conf.setDebug(true);


        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(150000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
