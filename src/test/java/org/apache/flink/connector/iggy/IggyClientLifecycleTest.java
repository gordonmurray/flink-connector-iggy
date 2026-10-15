package org.apache.flink.connector.iggy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Against a live Iggy: restarting readers and enumerators must not leave connections open. */
@EnabledIfEnvironmentVariable(named = "IGGY_HOST", matches = ".+")
class IggyClientLifecycleTest {

    private static final String HOST = System.getenv().getOrDefault("IGGY_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("IGGY_PORT", "8090"));

    @Test
    void closingReadersReleasesTheirConnections() throws Exception {
        var admin = new IggyConnectionConfig(HOST, PORT, "iggy", "iggy", false, null).connect();
        int before = admin.system().getClients().size();
        for (int i = 0; i < 5; i++) {
            var reader = IggySourceReaderTest.createReader();
            reader.start();
            reader.addSplits(java.util.List.of(new IggySplit("crypto", "prices", 0, 0)));
            reader.pollNext(new IggySourceReaderTest.CollectingReaderOutput(new java.util.ArrayList<>()));
            assertEquals(before + 1, admin.system().getClients().size(), "the reader is connected");
            reader.close();
            Thread.sleep(200);
        }
        Thread.sleep(500);
        assertEquals(before, admin.system().getClients().size(),
                "every reader start/close must leave the server's client count unchanged");
        IggyClients.closeQuietly(admin);
    }

    @Test
    void failedLoginDoesNotLeaveAConnection() throws Exception {
        var admin = new IggyConnectionConfig(HOST, PORT, "iggy", "iggy", false, null).connect();
        int before = admin.system().getClients().size();
        for (int i = 0; i < 3; i++) {
            var bad = new IggyConnectionConfig(HOST, PORT, "iggy", "wrong-password", false, null);
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, bad::connect);
        }
        Thread.sleep(500);
        assertEquals(before, admin.system().getClients().size());
        IggyClients.closeQuietly(admin);
    }

    @Test
    void closeQuietlyDisposesTheSharedConnectionOnce() {
        var client = new IggyConnectionConfig(HOST, PORT, "iggy", "iggy", false, null).connect();
        assertEquals(1, IggyClients.closeQuietly(client));
        assertEquals(0, IggyClients.closeQuietly(client), "already disposed");
    }
}
