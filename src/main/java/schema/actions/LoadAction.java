package schema.actions;

/**
 * Created by Chao on 4/6/2017 AD.
 */
public class LoadAction {
    private String timeStamp;
    private String href;
    private String scrollX;
    private String scrollY;
    private String innerHeight;
    private String innerWidth;
    private String appCode;
    private String appName;
    private String appVersion;

    public String getTimeStamp() {
        return this.timeStamp;
    }

    public String getHref() {
        return this.href;
    }

    public String getScrollX() {
        return scrollX;
    }

    public String getScrollY() {
        return scrollY;
    }

    public String getInnerHeight() {
        return innerHeight;
    }

    public String getInnerWidth() {
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
