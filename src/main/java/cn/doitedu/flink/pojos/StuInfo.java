package cn.doitedu.flink.pojos;

import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class StuInfo implements Serializable {
    private int id;
    private String phone;
    private String city;

}
