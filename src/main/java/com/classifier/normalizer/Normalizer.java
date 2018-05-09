package com.classifier.normalizer;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.net.InternetDomainName;
import com.google.inject.Inject;

import javax.annotation.concurrent.Immutable;
import java.util.regex.Pattern;

@Immutable
public final class Normalizer
{
    private static final boolean VALID_CHARS[] = new boolean[128];
    private final boolean stripSubDomains;

    static {
        Pattern pattern = Pattern.compile("[A-Za-z0-9_.-]");
        for (char c = 0; c < 128; c++) {
            VALID_CHARS[c] = pattern.matcher(String.valueOf(c)).matches();
        }
    }

    @Inject
    public Normalizer()
    {
        this.stripSubDomains = false;
    }

    private static final String PROTOCOL_SEPARATOR = "://";

    /**
     * Tidy up a url string for exact match string to string comparisons for threat related processing.
     * <p>
     * - Strips protocol
     * - Sets lowercase in the host/domain.
     * - Strips trailing "/"
     * - cleans up host to remove user and port information
     */
    public static String tidyUrlString(String url)
    {
        if (url == null || url.length() == 0) {
            return "";
        }

        if (url.indexOf(PROTOCOL_SEPARATOR) >= 0) {
            url = url.substring(url.indexOf(PROTOCOL_SEPARATOR) + PROTOCOL_SEPARATOR.length());
        }

        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }

        int slash = url.indexOf('/');
        String domain = url;
        String path = "";

        if (slash != -1) {
            path = url.substring(slash);
            domain = url.substring(0, slash);
        }

        domain = Normalizer.cleanHost(domain);

        return domain + path;
    }

    public NormalizedUrl normalize(String url)
    {
        String original = url;
        String path = "/";

        url = Normalizer.stripProtocol(url);

        int slash = url.indexOf('/');
        if (slash != -1) {
            path = url.substring(slash);
            url = url.substring(0, slash);
        }

        String host = Normalizer.cleanHost(url);
        String domain = Normalizer.extractDomainFromHost(host);

        if (stripSubDomains) {
            host = Objects.firstNonNull(domain, host);
        }

        return new NormalizedUrl(host, domain, Normalizer.normalizePath(path), domain, false, original);
    }

    private static String normalizePath(String path)
    {
        if (Strings.isNullOrEmpty(path)) {
            return "/";
        }

        int query = path.indexOf('?');
        if (query != -1) {
            path = path.substring(0, query);
        }

        int fragment = path.indexOf('#');
        if (fragment != -1) {
            path = path.substring(0, fragment);
        }

        return path;
    }

    private static String stripProtocol(String url)
    {
        if ((url.length() >= 7) && (url.substring(0, 4).toLowerCase().startsWith("http"))) {
            String afterHttp = url.substring(4);
            if (afterHttp.startsWith("://")) {
                url = url.substring(7);
            }
            else if (afterHttp.startsWith("s://")) {
                url = url.substring(8);
            }
            else if (afterHttp.startsWith("S://")) {
                url = url.substring(8);
            }
        }
        return url;
    }

    private static String extractDomainFromHost(String host)
    {
        try {
            return InternetDomainName.from(host).topPrivateDomain().toString();
        }
        catch (IllegalArgumentException e) {
            return host;
        }
        catch (IllegalStateException e) {
            return host;
        }
    }

    private static String validHostCharacters(String url)
    {
        boolean needLower = false;
        int i = 0;
        while (i < url.length()) {
            char c = url.charAt(i);
            if ((c >= 128) || (!VALID_CHARS[c])) {
                break;
            }
            if ((c >= 'A') && (c <= 'Z')) {
                needLower = true;
            }
            i++;
        }
        url = url.substring(0, i);
        return needLower ? url.toLowerCase() : url;
    }

    private static String cleanHost(String host)
    {
        // strip user info (also works for mailto)
        int atSign = host.indexOf('@');
        if (atSign != -1) {
            host = host.substring(atSign + 1);
        }

        // strip port
        int port = host.indexOf(':');
        if (port != -1) {
            host = host.substring(0, port);
        }

        return Strings.nullToEmpty(validHostCharacters(host));
    }
}