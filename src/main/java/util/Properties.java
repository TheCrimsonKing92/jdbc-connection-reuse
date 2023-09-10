package util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class Properties {

    private Properties() {
        throw new UnsupportedOperationException("No instances");
    }
    public static java.util.Properties load(final String file) throws IOException {
        Objects.requireNonNull(file);

        final ClassLoader loader = Properties.class.getClassLoader();
        try (final InputStream inputStream = loader.getResourceAsStream(file + ".properties")) {
            if (inputStream == null) {
                System.err.println("No properties file named " + file + " found");
                return null;
            }

            final java.util.Properties props = new java.util.Properties();
            props.load(inputStream);

            return props;
        }
    }
}
