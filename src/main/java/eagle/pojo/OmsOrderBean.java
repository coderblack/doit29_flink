package eagle.pojo;

import lombok.*;

import java.io.Serializable;
import java.time.LocalDateTime;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/7
 **/
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class OmsOrderBean implements Serializable {
    private int id;
    private int memeber_id;
    private double amount;
    private int pay_type;
    private int order_source;
    private int order_status;
    private LocalDateTime create_time;
    private LocalDateTime update_time;
}
