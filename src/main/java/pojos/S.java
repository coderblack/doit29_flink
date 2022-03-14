package pojos;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
public class S {
    public static void main(String[] args) {
        String x = "<LF><jobs><LF>";
        String res = x.replaceAll("<LF>", "");
        // res 是  <jobs>
        // x 依然是  <LF><jobs><LF>
        System.out.println();
    }
}
