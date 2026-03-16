package org.peergos.protocol.unixfs;

import com.google.protobuf.ByteString;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import org.junit.Assert;
import org.junit.Test;
import com.sun.net.httpserver.HttpServer;
import org.peergos.*;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.net.APIHandler;
import org.peergos.blockstore.metadatadb.BlockMetadata;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;
import org.peergos.config.IdentitySection;
import org.peergos.protocol.unixfs.pb.Merkledag;
import org.peergos.protocol.unixfs.pb.Unixfs;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class FileAssemblerTest {

    @Test
    public void singleRawBlock() throws Exception {
        EmbeddedIpfs node1 = buildNode();
        node1.start(false);
        EmbeddedIpfs node2 = buildNode(node1);
        node2.start(false);

        try {
            byte[] fileData = "Hello, IPFS World!".getBytes();
            Cid cid = node2.blockstore.put(fileData, Cid.Codec.Raw).join();

            PeerId peerId2 = node2.node.getPeerId();
            byte[] result = node1.getFile(cid, Set.of(peerId2));
            Assert.assertArrayEquals("Single raw block file should match", fileData, result);
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void singleDagPbWrappedFile() throws Exception {
        EmbeddedIpfs node1 = buildNode();
        node1.start(false);
        EmbeddedIpfs node2 = buildNode(node1);
        node2.start(false);

        try {
            byte[] fileData = "Small file in dag-pb".getBytes();

            // Create a UnixFS-wrapped dag-pb block (leaf with inline data)
            byte[] dagPbBlock = buildUnixFsLeafBlock(fileData);
            Cid cid = node2.blockstore.put(dagPbBlock, Cid.Codec.DagProtobuf).join();

            PeerId peerId2 = node2.node.getPeerId();
            byte[] result = node1.getFile(cid, Set.of(peerId2));
            Assert.assertArrayEquals("dag-pb wrapped file should match", fileData, result);
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void multiBlockFile() throws Exception {
        EmbeddedIpfs node1 = buildNode();
        node1.start(false);
        EmbeddedIpfs node2 = buildNode(node1);
        node2.start(false);

        try {
            byte[] chunk1 = "First chunk of data. ".getBytes();
            byte[] chunk2 = "Second chunk of data. ".getBytes();
            byte[] chunk3 = "Third chunk of data.".getBytes();

            // Store leaf blocks as raw codec
            Cid cid1 = node2.blockstore.put(chunk1, Cid.Codec.Raw).join();
            Cid cid2 = node2.blockstore.put(chunk2, Cid.Codec.Raw).join();
            Cid cid3 = node2.blockstore.put(chunk3, Cid.Codec.Raw).join();

            // Build interior dag-pb node linking to the 3 leaves
            long totalSize = chunk1.length + chunk2.length + chunk3.length;
            byte[] rootBlock = buildUnixFsInteriorBlock(
                    List.of(cid1, cid2, cid3),
                    List.of((long) chunk1.length, (long) chunk2.length, (long) chunk3.length),
                    totalSize
            );
            Cid rootCid = node2.blockstore.put(rootBlock, Cid.Codec.DagProtobuf).join();

            PeerId peerId2 = node2.node.getPeerId();
            byte[] result = node1.getFile(rootCid, Set.of(peerId2));

            // Expected: concatenation of all chunks
            ByteArrayOutputStream expected = new ByteArrayOutputStream();
            expected.write(chunk1);
            expected.write(chunk2);
            expected.write(chunk3);
            Assert.assertArrayEquals("Multi-block file should match concatenated chunks",
                    expected.toByteArray(), result);
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void streamingMatchesByteArray() throws Exception {
        EmbeddedIpfs node1 = buildNode();
        node1.start(false);
        EmbeddedIpfs node2 = buildNode(node1);
        node2.start(false);

        try {
            byte[] chunk1 = "Stream chunk one. ".getBytes();
            byte[] chunk2 = "Stream chunk two.".getBytes();

            Cid cid1 = node2.blockstore.put(chunk1, Cid.Codec.Raw).join();
            Cid cid2 = node2.blockstore.put(chunk2, Cid.Codec.Raw).join();

            long totalSize = chunk1.length + chunk2.length;
            byte[] rootBlock = buildUnixFsInteriorBlock(
                    List.of(cid1, cid2),
                    List.of((long) chunk1.length, (long) chunk2.length),
                    totalSize
            );
            Cid rootCid = node2.blockstore.put(rootBlock, Cid.Codec.DagProtobuf).join();

            PeerId peerId2 = node2.node.getPeerId();

            byte[] byteArrayResult = node1.getFile(rootCid, Set.of(peerId2));

            InputStream stream = node1.getFileStream(rootCid, Set.of(peerId2));
            ByteArrayOutputStream streamResult = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int n;
            while ((n = stream.read(buf)) != -1) {
                streamResult.write(buf, 0, n);
            }
            stream.close();

            Assert.assertArrayEquals("Streaming and byte-array modes should produce identical results",
                    byteArrayResult, streamResult.toByteArray());
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void emptyFile() throws Exception {
        EmbeddedIpfs node1 = buildNode();
        node1.start(false);
        EmbeddedIpfs node2 = buildNode(node1);
        node2.start(false);

        try {
            byte[] emptyData = new byte[0];

            // Create an empty UnixFS file block
            byte[] dagPbBlock = buildUnixFsLeafBlock(emptyData);
            Cid cid = node2.blockstore.put(dagPbBlock, Cid.Codec.DagProtobuf).join();

            PeerId peerId2 = node2.node.getPeerId();
            byte[] result = node1.getFile(cid, Set.of(peerId2));
            Assert.assertArrayEquals("Empty file should return empty byte array", emptyData, result);
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void extractMetadataDagProtobuf() throws Exception {
        byte[] chunk1 = "chunk1".getBytes();
        byte[] chunk2 = "chunk2".getBytes();

        Cid cid1 = new Cid(1, Cid.Codec.Raw, Multihash.Type.sha2_256, Hash.sha256(chunk1));
        Cid cid2 = new Cid(1, Cid.Codec.Raw, Multihash.Type.sha2_256, Hash.sha256(chunk2));

        byte[] rootBlock = buildUnixFsInteriorBlock(
                List.of(cid1, cid2),
                List.of((long) chunk1.length, (long) chunk2.length),
                (long) chunk1.length + chunk2.length
        );
        Cid rootCid = new Cid(1, Cid.Codec.DagProtobuf, Multihash.Type.sha2_256, Hash.sha256(rootBlock));

        BlockMetadata metadata = BlockMetadataStore.extractMetadata(rootCid, rootBlock);
        Assert.assertEquals("Metadata size should match block size", rootBlock.length, metadata.size);
        Assert.assertEquals("Should have 2 links", 2, metadata.links.size());
    }

    @Test
    public void catApiEndpoint() throws Exception {
        EmbeddedIpfs node1 = buildNode();
        node1.start(false);
        EmbeddedIpfs node2 = buildNode(node1);
        node2.start(false);

        int apiPort = TestPorts.getPort();
        HttpServer apiServer = HttpServer.create(new InetSocketAddress("127.0.0.1", apiPort), 500);
        apiServer.createContext(APIHandler.API_URL, new APIHandler(node1));
        apiServer.setExecutor(Executors.newFixedThreadPool(4));
        apiServer.start();

        try {
            byte[] chunk1 = "API chunk one. ".getBytes();
            byte[] chunk2 = "API chunk two.".getBytes();

            Cid cid1 = node2.blockstore.put(chunk1, Cid.Codec.Raw).join();
            Cid cid2 = node2.blockstore.put(chunk2, Cid.Codec.Raw).join();

            long totalSize = chunk1.length + chunk2.length;
            byte[] rootBlock = buildUnixFsInteriorBlock(
                    List.of(cid1, cid2),
                    List.of((long) chunk1.length, (long) chunk2.length),
                    totalSize
            );
            Cid rootCid = node2.blockstore.put(rootBlock, Cid.Codec.DagProtobuf).join();

            PeerId peerId2 = node2.node.getPeerId();
            String urlStr = "http://127.0.0.1:" + apiPort + "/api/v0/cat?arg=" + rootCid + "&peers=" + peerId2.toBase58();
            HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
            conn.setRequestMethod("POST");
            Assert.assertEquals("HTTP status should be 200", 200, conn.getResponseCode());

            ByteArrayOutputStream result = new ByteArrayOutputStream();
            InputStream in = conn.getInputStream();
            byte[] buf = new byte[1024];
            int n;
            while ((n = in.read(buf)) != -1) {
                result.write(buf, 0, n);
            }
            in.close();

            ByteArrayOutputStream expected = new ByteArrayOutputStream();
            expected.write(chunk1);
            expected.write(chunk2);
            Assert.assertArrayEquals("Cat API should return correct file content",
                    expected.toByteArray(), result.toByteArray());
        } finally {
            apiServer.stop(1);
            node1.stop();
            node2.stop();
        }
    }

    // --- Helper methods ---

    private static byte[] buildUnixFsLeafBlock(byte[] fileData) {
        Unixfs.Data.Builder unixfsBuilder = Unixfs.Data.newBuilder()
                .setType(Unixfs.Data.DataType.File)
                .setFilesize(fileData.length);
        if (fileData.length > 0) {
            unixfsBuilder.setData(ByteString.copyFrom(fileData));
        }
        byte[] unixfsBytes = unixfsBuilder.build().toByteArray();

        Merkledag.PBNode pbNode = Merkledag.PBNode.newBuilder()
                .setData(ByteString.copyFrom(unixfsBytes))
                .build();
        return pbNode.toByteArray();
    }

    private static byte[] buildUnixFsInteriorBlock(List<Cid> childCids, List<Long> blocksizes, long totalSize) {
        Unixfs.Data.Builder unixfsBuilder = Unixfs.Data.newBuilder()
                .setType(Unixfs.Data.DataType.File)
                .setFilesize(totalSize);
        for (long bs : blocksizes) {
            unixfsBuilder.addBlocksizes(bs);
        }
        byte[] unixfsBytes = unixfsBuilder.build().toByteArray();

        Merkledag.PBNode.Builder pbNodeBuilder = Merkledag.PBNode.newBuilder()
                .setData(ByteString.copyFrom(unixfsBytes));

        for (int i = 0; i < childCids.size(); i++) {
            Cid childCid = childCids.get(i);
            // For CIDv1, use full CID bytes; for CIDv0, use multihash bytes
            byte[] hashBytes = childCid.version == 0 ? childCid.toBytes() : childCid.toBytes();
            Merkledag.PBLink.Builder linkBuilder = Merkledag.PBLink.newBuilder()
                    .setHash(ByteString.copyFrom(hashBytes))
                    .setTsize(blocksizes.get(i));
            pbNodeBuilder.addLinks(linkBuilder);
        }

        return pbNodeBuilder.build().toByteArray();
    }

    private static EmbeddedIpfs buildNode() {
        return EmbeddedIpfsTest.build(Collections.emptyList(),
                List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + TestPorts.getPort())));
    }

    private static EmbeddedIpfs buildNode(EmbeddedIpfs bootstrap) {
        return EmbeddedIpfsTest.build(
                bootstrap.node.listenAddresses().stream()
                        .map(a -> new MultiAddress(a.toString()))
                        .collect(Collectors.toList()),
                List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + TestPorts.getPort())));
    }
}
