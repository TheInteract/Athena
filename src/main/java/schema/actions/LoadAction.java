package schema.actions;

import java.util.Date;

/**
 * Created by Chao on 4/6/2017 AD.
 */
public class LoadAction {
    private Date timeStamp;
    private String href;
    private int scrollX;
    private int scrollY;
    private int innerHeight;
    private int innerWidth;
    private String appCode;
    private String appName;
    private String appVersion;

    public Date getTimeStamp() {
        return timeStamp;
    }

    public String getHref() {
        return href;
    }

    public int getScrollX() {
        return scrollX;
    }

    public int getScrollY() {
        return scrollY;
    }

    public int getInnerHeight() {
        return innerHeight;
    }

    public int getInnerWidth() {
        return innerWidth;
    }

    public String getAppCode() {
        return appCode;
    }

    public String getAppName() {
        return appName;
    }

    public String getAppVersion() {
        return appVersion;
    }
}
