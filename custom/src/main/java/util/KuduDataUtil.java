package util;

import java.util.HashSet;
import java.util.Set;

public class KuduDataUtil {
    private static Set<String> uselessPrimaryKeySet = new HashSet<>();

    static {
        //不增加前缀的主键
        uselessPrimaryKeySet.add("testtime");
        uselessPrimaryKeySet.add("create_date");
        uselessPrimaryKeySet.add("scan_time");
        uselessPrimaryKeySet.add("pro_in_date");
        uselessPrimaryKeySet.add("fin_time");
        uselessPrimaryKeySet.add("confirm_time");
        uselessPrimaryKeySet.add("fintime");
        uselessPrimaryKeySet.add("gmt_create");
        uselessPrimaryKeySet.add("pro_date");
    }

    public static Set<String> getUselessPrimaryKeySet() {
        return uselessPrimaryKeySet;
    }
}
