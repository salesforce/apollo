
package io.quantumdb.core.utils;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;

public class RandomHasher {
    private static final String CONTENTS = "0123456789abcdef";
    private static final int    LENGTH   = 10;

    public static String generateHash() {
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        int position = 0;
        while (builder.length() < LENGTH) {
            position += random.nextInt(CONTENTS.length() * 4) % CONTENTS.length();
            builder.append(CONTENTS.charAt(position % CONTENTS.length()));
        }
        return builder.toString();
    }

    public static String generateRefId(RefLog refLog) {
        checkArgument(refLog != null, "You must specify a \'refLog\'.");
        Set<String> refIds = refLog.getTableRefs().stream().map(TableRef::getRefId).collect(Collectors.toSet());
        String hash = "table_" + generateHash();
        while (refIds.contains(hash)) {
            hash = "table_" + generateHash();
        }
        return hash;
    }

    
    private RandomHasher() {
    }
}
