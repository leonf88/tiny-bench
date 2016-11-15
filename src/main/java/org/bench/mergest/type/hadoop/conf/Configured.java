package org.bench.mergest.type.hadoop.conf;

/**
 * Base class for things that may be configured with a {@link Configuration}.
 */
public class Configured implements Configurable {

    private Configuration conf;

    /**
     * Construct a Configured.
     */
    public Configured() {
        this(null);
    }

    /**
     * Construct a Configured.
     */
    public Configured(Configuration conf) {
        setConf(conf);
    }

    // inherit javadoc
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    // inherit javadoc
    @Override
    public Configuration getConf() {
        return conf;
    }

}
