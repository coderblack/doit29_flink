package eagle.pojo;

/**
 * Copyright 2022 bejson.com
 */
import lombok.*;

import java.util.Date;
import java.util.Map;


@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class EventBean {

    private String account;
    private String appid;
    private String appversion;
    private String carrier;
    private String deviceid;
    private String devicetype;
    private String eventid;
    private String ip;
    private double latitude;
    private double longitude;
    private String nettype;
    private String osname;
    private String osversion;
    private Map<String,String> properties;
    private String releasechannel;
    private String resolution;
    private String sessionid;
    private long timestamp;


}