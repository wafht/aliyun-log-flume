package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.aliyun.loghub.flume.Constants.*;

public class PlainDelimitedEventDeserializer implements EventDeserializer {
    private static final Logger LOG = LoggerFactory.getLogger(PlainDelimitedEventDeserializer.class);

    static final String ALIAS = "PLAIN_DELIMITED";

    private Map<String, Integer> fieldIndexMapping;
    private boolean              useRecordTime;
    private boolean              appendTimestamp;
    private char                 separatorChar;
    private String               lineEnd;
    private String               logstore;
    private int                  dropPercent;

    private static final String DEFAULT_LINE_END = "";

    private boolean appendLocalTime;
    private int     localTimeIndex = 0;

    @Override
    public List<Event> deserialize(FastLogGroup logGroup) {
        int count = logGroup.getLogsCount();
        int width = fieldIndexMapping.size();
        if (appendTimestamp) {
            width++;
        }
        String[] record = new String[width];
        List<Event> events = new ArrayList<>(count);

        for (int idx = 0; idx < count; ++idx) {
            if (dropPercent > 0) {
                int random = ThreadLocalRandom.current().nextInt(100);
                if (random < dropPercent) {
                    continue;
                }
            }

            FastLog log = logGroup.getLogs(idx);
            for (int i = 0; i < log.getContentsCount(); i++) {
                FastLogContent content = log.getContents(i);
                final String key = content.getKey();
                Integer index = fieldIndexMapping.get(key);
                if (index != null) {
                    // otherwise ignore this field
                    String value = content.getValue();
                    if (value != null && value.contains("\n")) {
                        value = value.replace('\n', ' ');
                    }
                    record[index] = value;
                }
            }
            int recordTime = log.getTime();
            String localTime = String.valueOf(System.currentTimeMillis());
            String timestamp;
            if (useRecordTime) {
                timestamp = String.valueOf(((long) recordTime) * 1000);
            } else {
                timestamp = localTime;
            }
            if (appendTimestamp) {
                record[width - 1] = timestamp;
            }
            if (appendLocalTime) {
                record[localTimeIndex] = localTime;
            }
            StringBuilder sb = new StringBuilder();
            for (int recordIndex = 0; recordIndex < width; recordIndex++) {
                String rcd = record[recordIndex];
                sb.append(rcd);
                if (recordIndex != width - 1) {
                    sb.append(this.separatorChar);
                } else {
                    sb.append(this.lineEnd);
                }
            }
            Event event = EventBuilder.withBody(sb.toString(), charset,
                    ImmutableMap.of(TIMESTAMP, timestamp, LOGSTORE_KEY, logstore));
            events.add(event);
            for (int i = 0; i < width; i++) {
                record[i] = null;
            }
        }
        return events;
    }

    private static char getChar(Context context, String key, char defaultValue) {
        String value = context.getString(key);
        if (value == null) {
            return defaultValue;
        }
        value = value.trim();
        if (value.length() != 1) {
            throw new IllegalArgumentException(key + " is invalid for CSV serializer: " + value);
        }
        return value.charAt(0);
    }

    @Override
    public void configure(Context context) {
        useRecordTime = context.getBoolean(USE_RECORD_TIME, false);
        String columns = context.getString(COLUMNS);
        if (StringUtils.isBlank(columns)) {
            throw new IllegalArgumentException("Missing parameters: " + COLUMNS);
        }
        separatorChar = getChar(context, SEPARATOR_CHAR, CSVWriter.DEFAULT_SEPARATOR);
        logstore = context.getString(LOGSTORE_KEY);
        if (StringUtils.isBlank(logstore)) {
            throw new IllegalArgumentException("Missing parameters: " + LOGSTORE_KEY);
        }
        dropPercent = context.getInteger(SOURCE_DROP_PERCENT, 0);

        LOG.info("separatorChar=[" + separatorChar + "]");
        lineEnd = context.getString(LINE_END, DEFAULT_LINE_END);
        appendTimestamp = context.getBoolean(APPEND_TIMESTAMP, false);
        String[] fields = columns.split(",", -1);
        int width = fields.length;
        fieldIndexMapping = new HashMap<>(width);
        for (int i = 0; i < width; i++) {
            fieldIndexMapping.put(fields[i], i);
        }
        appendLocalTime = context.getBoolean(APPEND_LOCAL_TIME, false);
        if (appendLocalTime) {
            String localTimeFieldName = context.getString(LOCAL_TIME_FIELD_NAME);
            if (StringUtils.isBlank(localTimeFieldName)) {
                throw new IllegalArgumentException("Missing parameter: " + LOCAL_TIME_FIELD_NAME);
            }
            if (!fieldIndexMapping.containsKey(localTimeFieldName)) {
                throw new IllegalArgumentException("Field '" + localTimeFieldName + "' not exist in columns");
            }
            localTimeIndex = fieldIndexMapping.get(localTimeFieldName);
        }
    }
}


