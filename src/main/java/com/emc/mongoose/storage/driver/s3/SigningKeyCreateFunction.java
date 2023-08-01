package com.emc.mongoose.storage.driver.s3;

import java.util.function.Function;

/**
 *  A function to create the signing key using the date as the function argument
 */
public interface SigningKeyCreateFunction
        extends Function<String, byte[]> {

    /**
     * @param datestamp the scope name
     * @return the created signing key
     */
    byte[] apply(final String datestamp);
}