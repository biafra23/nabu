package org.peergos.protocol.unixfs;

import io.ipfs.cid.Cid;
import io.libp2p.core.PeerId;
import org.peergos.EmbeddedIpfs;
import org.peergos.HashedBlock;
import org.peergos.Want;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class UnixFsFileInputStream extends InputStream {

    private final EmbeddedIpfs ipfs;
    private final Set<PeerId> peers;
    private final Deque<Cid> pending;
    private byte[] currentChunk;
    private int currentOffset;

    public UnixFsFileInputStream(EmbeddedIpfs ipfs, Cid root, Set<PeerId> peers) {
        this.ipfs = ipfs;
        this.peers = peers;
        this.pending = new ArrayDeque<>();
        this.pending.addLast(root);
        this.currentChunk = null;
        this.currentOffset = 0;
    }

    @Override
    public int read() throws IOException {
        while (true) {
            if (currentChunk != null && currentOffset < currentChunk.length) {
                return currentChunk[currentOffset++] & 0xFF;
            }
            if (!advance()) {
                return -1;
            }
        }
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        if (len == 0) return 0;

        int totalRead = 0;
        while (totalRead < len) {
            if (currentChunk != null && currentOffset < currentChunk.length) {
                int available = currentChunk.length - currentOffset;
                int toCopy = Math.min(available, len - totalRead);
                System.arraycopy(currentChunk, currentOffset, buf, off + totalRead, toCopy);
                currentOffset += toCopy;
                totalRead += toCopy;
            } else {
                if (!advance()) {
                    return totalRead > 0 ? totalRead : -1;
                }
            }
        }
        return totalRead;
    }

    private boolean advance() {
        while (!pending.isEmpty()) {
            Cid next = pending.pollFirst();
            List<HashedBlock> blocks = ipfs.getBlocks(List.of(new Want(next)), peers, false);
            if (blocks.isEmpty()) {
                throw new RuntimeException("Failed to retrieve block: " + next);
            }
            HashedBlock block = blocks.get(0);
            UnixFsNode node = UnixFsNode.parseBlock(block.hash, block.block);

            if (node.isLeaf()) {
                if (node.data.length > 0) {
                    currentChunk = node.data;
                    currentOffset = 0;
                    return true;
                }
                // Empty leaf, continue to next
            } else {
                // Interior node: push children to front in order
                List<Cid> children = node.childLinks;
                for (int i = children.size() - 1; i >= 0; i--) {
                    pending.addFirst(children.get(i));
                }
            }
        }
        return false;
    }
}
