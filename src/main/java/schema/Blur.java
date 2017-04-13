package schema;

import org.apache.storm.tuple.Values;
import schema.actions.BlurAction;

import java.util.Arrays;

/**
 * Created by Chao on 4/11/2017 AD.
 */
public class Blur extends AthenaObject implements AthenaInterface {
    private BlurAction action;

    public Values getValueList() {
        Values values = new Values();
        values.add(this.getIssueTime());
        values.add(this.getType().replace("on", ""));
        values.add(this.getAPI_KEY_PUBLIC());
        values.add(Arrays.asList(this.getVersions()));
        values.add(this.getDeviceCode());
        values.add(this.getUserCode());
        values.add(this.action.getTimeStamp());
        values.add(this.action.getHref());
        return values;
    }
}
