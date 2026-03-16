package org.peergos.protocol.dnsaddr;

import org.xbill.DNS.*;
import org.xbill.DNS.Record;
import org.xbill.DNS.lookup.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class DnsAddr {

    public static List<String> resolve(String in) {
        if (! in.startsWith("/dnsaddr/"))
            return List.of(in);
        // Strip trailing slash if present
        if (in.endsWith("/"))
            in = in.substring(0, in.length() - 1);
        int endDomain = in.indexOf("/", 9);
        boolean hasSuffix = endDomain != -1;
        String domain = "_dnsaddr." + (hasSuffix ? in.substring(9, endDomain) : in.substring(9));
        String suffix = hasSuffix ? in.substring(endDomain) : null;
        String alternativeSuffix = suffix != null ? suffix.replace("/ipfs/", "/p2p/") : null;

        LookupSession s = LookupSession.defaultBuilder().build();
        Name txtLookup;
        try {
            txtLookup = Name.fromString(domain + ".");
        } catch (TextParseException e) {
            return Collections.emptyList();
        }
        CompletableFuture<List<String>> res = new CompletableFuture<>();

        s.lookupAsync(txtLookup, Type.TXT)
                .whenComplete(
                        (answers, ex) -> {
                            if (ex == null) {
                                if (answers.getRecords().isEmpty()) {
                                    System.out.println(txtLookup + " has no TXT records");
                                    res.complete(Collections.emptyList());
                                } else {
                                    List<String> records = new ArrayList<>();
                                    for (Record rec : answers.getRecords()) {
                                        TXTRecord txt = ((TXTRecord) rec);
                                        List<String> strings = txt.getStrings();
                                        records.addAll(strings);
                                    }
                                    res.complete(records.stream()
                                            .filter(line -> line.startsWith("dnsaddr="))
                                            .map(line -> line.substring(8))
                                            .map(DnsAddr::resolve)
                                            .flatMap(List::stream)
                                            .collect(Collectors.toList()));
                                }
                            } else {
                                res.complete(Collections.emptyList());
                            }
                        })
                .toCompletableFuture()
                .join();
        String suffixFilter = suffix;
        String altSuffixFilter = alternativeSuffix;
        return res.orTimeout(5, TimeUnit.SECONDS).join()
                .stream()
                .filter(a -> suffixFilter == null || a.endsWith(suffixFilter) || a.endsWith(altSuffixFilter))
                .collect(Collectors.toList());
    }
}
