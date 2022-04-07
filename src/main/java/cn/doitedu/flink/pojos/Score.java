package cn.doitedu.flink.pojos;

import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class Score implements Serializable {
    private int id;
    private String gender;
    private double score;
    private long timeStamp;

}
