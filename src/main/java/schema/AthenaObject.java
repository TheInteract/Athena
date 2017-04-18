package schema;

import java.util.Date;

/**
 * Created by Chao on 4/11/2017 AD.
 */
public class AthenaObject {
    private Date issueTime;
    private String type;
    private String API_KEY_PUBLIC;
    private String deviceCode;
    private String userCode;
    private String[] versions;
    private String sessionCode;

    public Date getIssueTime() {
        return issueTime;
    }

    public String getType() {
        return type;
    }

    public String getAPI_KEY_PUBLIC() {
        return API_KEY_PUBLIC;
    }

    public String getDeviceCode() {
        return deviceCode;
    }

    public String getUserCode() {
        return userCode;
    }

    public String[] getVersions() {
        return versions;
    }

    public String getSessionCode() {
        return sessionCode;
    }
}
