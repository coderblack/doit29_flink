package cn.doitedu.flink.pojos;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class StudentStat implements Serializable {
    private String gender;
    private double minScore;
    private double maxScore;
    private double avgScore;
    private double userCount;

}
