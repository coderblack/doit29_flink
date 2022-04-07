package cn.doitedu.flink.pojos;

import lombok.*;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Pojo {
    private int id;
    private String name;
    private String gender;
    private double score;
}
