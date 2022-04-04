package eagle.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/4
 **/

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TrafficAnalyseBean implements Serializable {
    // guid,  访问总时长,  渠道,  终端， 省市区 ，页面， 页面访问时长
    private long guid;
    private String sessionId;
    private long sessionTimeLong;
    private String releaseChannel;
    private String deviceType;
    private String province;
    private String city;
    private String region;
    private String pageId;
    private long pageTimeLong;
    private long pageLoadTime;

}
