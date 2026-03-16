package org.peergos.protocol.unixfs;

import io.ipfs.cid.Cid;
import io.libp2p.core.PeerId;
import org.peergos.EmbeddedIpfs;
import org.peergos.HashedBlock;
import org.peergos.Want;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class FileAssembler {

    private final EmbeddedIpfs ipfs;

    public FileAssembler(EmbeddedIpfs ipfs) {
        this.ipfs = ipfs;
    }

    public byte[] getFile(Cid root, Set<PeerId> peers) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Deque<Cid> toProcess = new ArrayDeque<>();
        toProcess.addLast(root);

        while (!toProcess.isEmpty()) {
            // Collect all CIDs at the current frontier for batch fetching
            List<Cid> batch = new ArrayList<>();
            while (!toProcess.isEmpty()) {
                batch.add(toProcess.pollFirst());
            }

            List<Want> wants = batch.stream().map(Want::new).collect(Collectors.toList());
            List<HashedBlock> blocks = ipfs.getBlocks(wants, peers, false);

            // Build a map for ordered access
            Map<Cid, byte[]> blockMap = new HashMap<>();
            for (HashedBlock hb : blocks) {
                blockMap.put(hb.hash, hb.block);
            }

            for (Cid cid : batch) {
                byte[] blockData = blockMap.get(cid);
                if (blockData == null) {
                    throw new RuntimeException("Failed to retrieve block: " + cid);
                }
                UnixFsNode node = UnixFsNode.parseBlock(cid, blockData);
                if (node.isLeaf()) {
                    out.write(node.data, 0, node.data.length);
                } else {
                    // Add children to front of queue in order
                    List<Cid> children = node.childLinks;
                    for (int i = children.size() - 1; i >= 0; i--) {
                        toProcess.addFirst(children.get(i));
                    }
                }
            }
        }

        return out.toByteArray();
    }

    public InputStream getFileStream(Cid root, Set<PeerId> peers) {
        return new UnixFsFileInputStream(ipfs, root, peers);
    }
}
