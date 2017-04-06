package schema;

import org.apache.storm.tuple.Values;
import schema.actions.LoadAction;

/**
 * Created by Chao on 4/6/2017 AD.
 */
public class Load implements AthenaObject {
    private String issueTime;
    private String type;
    private String API_KEY_PUBLIC;
    private String deviceCode;
    private String userCode;
    private LoadAction action;

    public Values getValueList() {
        Values values = new Values();
        values.add(this.issueTime);
        values.add(this.type.replace("on", ""));
        values.add(this.API_KEY_PUBLIC);
        values.add(this.deviceCode);
        values.add(this.userCode);
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