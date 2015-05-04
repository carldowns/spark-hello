import driver.Program1;
import driver.Program2;
import driver.Program3;
import driver.Program4;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Arrays;

public class SparkApp implements Serializable {

    private static Logger _log = Logger.getLogger(SparkApp.class);

    public static void main(String[] args) {
        _log.info(Arrays.asList(args));
        checkArgs (args, 1, "usage: {program} {arg1} {arg2} ... ");
        String programName = args[0];

        switch (programName) {
            case Program1.NAME :
                checkArgs (args, 3, "usage: " + programName + " {src file} {output dir}");
                Program1 p1 = new Program1(args[1], args[2]);
                p1.process();
                break;

            case Program2.NAME :
                checkArgs (args, 2, "usage: " + programName + "{output path} {src path 1} ... {src path N} ");
                Program2 p2 = new Program2(args);
                p2.process();
                break;

//            case Program3.NAME :
//                checkArgs (args, 2, "usage: " + programName + "{output path} {path to region/stash/env/date bucket}");
//                Program3 p3 = new Program3(args);
//                p3.process();
//                break;
//
//            case Program4.NAME :
//                checkArgs (args, 2, "usage: " + programName + "{output path} {path to region/stash/env/date bucket} using native mode");
//                Program4 p4 = new Program4(args);
//                p4.process();
//                break;

//            case Program4.NAME :
//                checkArgs (args, 2, "usage: " + programName + "{output path} {path to region/stash/env bucket}");
//                Program4 p4 = new Program4(args);
//                p4.process();
//                break;
//
            default :
                throw new IllegalArgumentException ("HOLD UP :: unexpected argument " + args[0]);
        }
    }

    public static void checkArgs (String[] args, int required, String usage) {
        if (args.length < required) {
            throw new IllegalArgumentException(usage);
        }
    }
}
