package com.cloudest.connect.hbase.transformer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by terry on 2017/6/8.
 */
public class TS8601ToLongTransformer implements Transformer<String, Long> {

    private static final Logger logger = LoggerFactory.getLogger(TS8601ToLongTransformer.class);
    private static final DateFormat GMT_SDF;

    static {
        GMT_SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        GMT_SDF.setTimeZone(timeZone);
    }

    @Override
    public Long transform(String oType) {

        if (StringUtils.isBlank(oType))
            return null;

        try {
            return GMT_SDF.parse(oType).getTime();
        } catch (ParseException e) {
            logger.error("parse ISO8601 time fail. input string is {}.", oType, e);
            return null;
        }

    }


}
