package eagle.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class DeviceIdMapBean implements Serializable {
    private long guid;
    private long firstAccessTime;
}
