package schema;

import org.apache.storm.tuple.Values;
import schema.actions.LoadAction;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by Chao on 4/6/2017 AD.
 */
public class Load extends AthenaObject implements AthenaInterface {
    private LoadAction action;

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
        values.add(this.action.getScrollX());
        values.add(this.action.getScrollY());
        values.add(this.action.getInnerHeight());
        values.add(this.action.getInnerWidth());
        values.add(this.action.getAppCode());
        values.add(this.action.getAppName());
        values.add(this.action.getAppVersion());

        return values;
    }
}