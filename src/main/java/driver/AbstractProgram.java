package driver;

/**
 * Created by carl.downs on 4/21/15.
 */
public class AbstractProgram {

    public static void checkArgs (String[] args, int required, String usage) {
        if (args.length < required) {
            throw new IllegalArgumentException(usage);
        }
    }
}
