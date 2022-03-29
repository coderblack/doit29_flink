package eagle.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeviceIdMapBean implements Serializable {
    private long guid;
    private long firstAccessTime;
}
