package common;

/**
 * Created by Chao on 4/4/2017 AD.
 */

import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.tuple.ITuple;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

import java.io.Serializable;

public class AthenaQueryFilterCreator implements QueryFilterCreator {

    private String[] fields;

    @Override
    public Bson createFilter(ITuple input) {
        Bson baseFilter = null;
        for(String field: fields) {
            if(input.contains(field)) {
                if (baseFilter == null) {
                    baseFilter = Filters.eq(field, input.getValueByField(field));
                } else {
                    baseFilter = Filters.and(baseFilter, Filters.eq(field, input.getValueByField(field)));
                }
            }
        }
        return baseFilter;
    }

    public Bson createFilter(ITuple input, String from, String to) {
        Bson baseFilter = null;
        for(String field: fields) {
            String tempField = null;
            if (from != null && to != null) {
                if (field.equals(from)) {
                    tempField = field;
                    field = to;
                }
            }
            if(input.contains(tempField == null ? field : tempField)) {
                if (baseFilter == null) {
                    baseFilter = Filters.eq(field, input.getValueByField(tempField == null ? field : tempField));
                } else {
                    baseFilter = Filters.and(baseFilter, Filters.eq(field, input.getValueByField(tempField == null ? field : tempField)));
                }
            }
        }
        return baseFilter;
    }

    public Bson createFilterOr(ITuple input) {
        Bson baseFilter = null;
        for(String field: fields) {
            if(input.contains(field)) {
                if (baseFilter == null) {
                    baseFilter = Filters.eq("userIdentity", input.getValueByField(field));
                } else {
                    baseFilter = Filters.or(baseFilter, Filters.eq("userIdentity", input.getValueByField(field)));
                }
            }
        }
        return baseFilter;
    }

    public Bson customCreateSession(ITuple input) {
        Bson baseFilter = null;
        for(String field: fields) {
            if(input.contains(field)) {
                if (baseFilter == null) {
                    baseFilter = Filters.eq(field, input.getValueByField(field));
                } else {
                    baseFilter = Filters.and(baseFilter, Filters.eq(field, input.getValueByField(field)));
                }
            }
        }
        Bson timeFilter = Filters.lt("issueTime", input.getValueByField("issueTime"));
        baseFilter = Filters.and(baseFilter, timeFilter);
        return baseFilter;
    }

    public AthenaQueryFilterCreator withField(String... field) {
        this.fields = field;
        return this;
    }

    public String[] getFields() {
        return this.fields;
    }

}
