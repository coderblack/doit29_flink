package cn.doitedu.flink.pojos;

import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class EventLog implements Serializable {

    private long guid;
    private String eventId;
    private String channel;
    private long timeStamp;
    private long stayLong;

}
