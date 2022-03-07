package pojos;


import lombok.*;

import java.io.Serializable;

// {"guid":1,"eventid":"a","timestamp":138374844,"channel":"app"}
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class AccessLog implements Serializable {
    private long guid;
    private String eventid;
    private long timestamp;
    private String channel;

}
