package imc.persistence;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

/**
 * Starts up an empty node with persistence enabled.
 */
public class NodePersistenceStartup {
    /**
     * Start up an empty node with persistence enabled.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws IgniteException {
        Ignition.start("config/ignite-persistence-configuration.xml");
    }
}
