package eagle.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class AccountIdMapBean implements Serializable {
    private long guid;
    private long registerTime;
}
