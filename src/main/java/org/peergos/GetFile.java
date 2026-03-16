package org.peergos;

import io.ipfs.cid.Cid;
import io.libp2p.core.PeerId;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;
import org.peergos.config.*;
import org.peergos.protocol.dht.DatabaseRecordStore;
import org.peergos.util.Logging;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.peergos.EmbeddedIpfs.buildBlockStore;
import static org.peergos.EmbeddedIpfs.buildBlockMetadata;

public class GetFile {

    private static final Logger LOG = Logging.LOG();

    public static void main(String[] args) {
        try {
            Args a = Args.parse(args);
            String cidStr = a.getArg("cid");
            String outputPath = a.getArg("output");

            List<String> peerStrings = a.getOptionalArg("peers")
                    .map(p -> Arrays.asList(p.split(",")))
                    .orElse(Collections.emptyList());

            Path ipfsPath = a.getIPFSDir();
            Logging.init(ipfsPath, a.getBoolean("log-to-console", true));
            Config config = Config.build(Files.readString(ipfsPath.resolve("config")));

            BlockRequestAuthoriser authoriser = (c, p2, auth) -> CompletableFuture.completedFuture(true);
            Path datastorePath = ipfsPath.resolve("datastore").resolve("h2-v2.datastore");
            DatabaseRecordStore records = new DatabaseRecordStore(datastorePath.toAbsolutePath().toString());
            BlockMetadataStore meta = buildBlockMetadata(a);
            EmbeddedIpfs ipfs = EmbeddedIpfs.build(records,
                    buildBlockStore(config, ipfsPath, meta, true),
                    true,
                    config.addresses.getSwarmAddresses(),
                    config.bootstrap.getBootstrapAddresses(),
                    config.identity,
                    authoriser,
                    Optional.empty()
            );
            ipfs.start(false);

            try {
                Cid cid = Cid.decode(cidStr);
                Set<PeerId> peers = ipfs.resolvePeerStrings(peerStrings);
                InputStream stream = ipfs.getFileStream(cid, peers);
                try (OutputStream out = Files.newOutputStream(Path.of(outputPath))) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = stream.read(buf)) != -1) {
                        out.write(buf, 0, n);
                    }
                }
                stream.close();
                LOG.info("File written to " + outputPath);
            } finally {
                ipfs.stop();
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            System.exit(1);
        }
    }
}
