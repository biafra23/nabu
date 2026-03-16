package org.peergos.protocol.unixfs;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.protocol.unixfs.pb.Merkledag;
import org.peergos.protocol.unixfs.pb.Unixfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class UnixFsNode {

    public final byte[] data;
    public final List<Cid> childLinks;
    public final long filesize;
    public final List<Long> blocksizes;

    public UnixFsNode(byte[] data, List<Cid> childLinks, long filesize, List<Long> blocksizes) {
        this.data = data;
        this.childLinks = childLinks;
        this.filesize = filesize;
        this.blocksizes = blocksizes;
    }

    public boolean isLeaf() {
        return childLinks.isEmpty();
    }

    public static UnixFsNode parseBlock(Cid cid, byte[] blockData) {
        if (cid.codec == Cid.Codec.Raw) {
            return new UnixFsNode(blockData, Collections.emptyList(), blockData.length, Collections.emptyList());
        } else if (cid.codec == Cid.Codec.DagProtobuf) {
            return parseDagPb(blockData);
        } else {
            throw new IllegalStateException("Unsupported codec for UnixFS: " + cid.codec);
        }
    }

    private static UnixFsNode parseDagPb(byte[] blockData) {
        try {
            Merkledag.PBNode pbNode = Merkledag.PBNode.parseFrom(blockData);

            List<Cid> links = new ArrayList<>();
            for (Merkledag.PBLink link : pbNode.getLinksList()) {
                byte[] hashBytes = link.getHash().toByteArray();
                Cid linkCid = cidFromPbLinkHash(hashBytes);
                links.add(linkCid);
            }

            byte[] data = new byte[0];
            long filesize = 0;
            List<Long> blocksizes = Collections.emptyList();

            if (pbNode.hasData()) {
                byte[] pbData = pbNode.getData().toByteArray();
                Unixfs.Data unixfsData = Unixfs.Data.parseFrom(pbData);
                if (unixfsData.hasData()) {
                    data = unixfsData.getData().toByteArray();
                }
                if (unixfsData.hasFilesize()) {
                    filesize = unixfsData.getFilesize();
                }
                blocksizes = unixfsData.getBlocksizesList();
            }

            return new UnixFsNode(data, links, filesize, blocksizes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse dag-pb block", e);
        }
    }

    public static Cid cidFromPbLinkHash(byte[] hashBytes) {
        // PBLink.Hash contains a raw multihash for CIDv0 or a full CID for CIDv1
        // CIDv1 starts with a varint version (>= 1), CIDv0 starts with a multihash
        // (hash function code, e.g., 0x12 for sha2-256)
        // Try to detect CIDv1: first varint > 0 and not a valid multihash type indicator for v0
        if (hashBytes.length > 2 && hashBytes[0] != 0x12) {
            // Likely CIDv1
            return Cid.cast(hashBytes);
        }
        // CIDv0: bare multihash, assumed dag-pb codec
        Multihash mhash = Multihash.deserialize(hashBytes);
        return new Cid(0, Cid.Codec.DagProtobuf, mhash.getType(), mhash.getHash());
    }
}
