package org.n52.sw.suite.db.dao;

import java.sql.Timestamp;

import org.hibernate.Criteria;
import org.hibernate.criterion.Projections;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.shetland.ogc.gml.time.TimePeriod;

public interface TimeCreator {

    enum MinMax {
        MIN,
        MAX
    }

    /**
     * Creates a time period object from sources
     *
     * @param minStart Min start timestamp
     * @param maxStart Max start timestamp
     * @param maxEnd Max end timestamp
     *
     * @return Time period object
     */
    default TimePeriod createTimePeriod(Timestamp minStart, Timestamp maxStart, Timestamp maxEnd) {
        DateTime start = new DateTime(minStart, DateTimeZone.UTC);
        DateTime end = new DateTime(maxStart, DateTimeZone.UTC);
        if (maxEnd != null) {
            DateTime endTmp = new DateTime(maxEnd, DateTimeZone.UTC);
            if (endTmp.isAfter(end)) {
                end = endTmp;
            }
        }
        return new TimePeriod(start, end);
    }
    
    default TimePeriod createTimePeriod(Timestamp minStart, Timestamp maxEnd) {
        DateTime start = new DateTime(minStart, DateTimeZone.UTC);
        DateTime end = new DateTime(maxEnd, DateTimeZone.UTC);
        return new TimePeriod(start, end);
    }

    /**
     * Add min/max projection to criteria
     *
     * @param criteria Hibernate Criteria to add projection
     * @param minMax Min/Max identifier
     * @param property Property to apply projection to
     */
    default void addMinMaxProjection(Criteria criteria, MinMax minMax, String property) {
        // TODO move this to a better location, maybe with Java 8 in an own Interface with Multiple Inheritance
        switch (minMax) {
            case MIN:
                criteria.setProjection(Projections.min(property));
                break;
            case MAX:
                criteria.setProjection(Projections.max(property));
                break;
            default:
                throw new Error();
        }
    }
}
