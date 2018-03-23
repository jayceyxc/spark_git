package com.linus.utils;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Pattern;

public class Utils {
    private static final Logger logger = Logger.getLogger (Utils.class);

    public static final String DATA_FILE_PATH = "data";
    public static final String URL_TAGS_FILE_NAME = "url_tags.txt";
    public static final String URL_TAGS_FILE = System.getProperty ("user.dir") + System.getProperty ("file.separator")
            + DATA_FILE_PATH + System.getProperty ("file.separator") + URL_TAGS_FILE_NAME;
    public static final String DOMAIN_SUFFIX_FILE_NAME = "domain.txt";

    private static final String HTTP_PREFIX = "http://";
    private static final String HTTPS_PREFIX = "https://";
    private static final String WWW_PREFIX = "www.";

    private static Set<String> suffixSet = loadDomainSuffix (DOMAIN_SUFFIX_FILE_NAME);

    public static Set<String> loadDomainSuffix (String filename) {
        try {

            Set<String> suffixSet = new TreeSet<String> ();
            BufferedReader reader = new BufferedReader (new FileReader (filename));
            String line = null;
            while ((line = reader.readLine ()) != null) {
                line = line.trim ();
                suffixSet.add (line);

            }

            return suffixSet;
        } catch (FileNotFoundException fnfe) {
            logger.error ("File not found");
        } catch (IOException ioe) {
            logger.error ("Read file failed");
        }

        return null;
    }

    public static String urlFormat (String url) {
        if (url.startsWith (HTTP_PREFIX)) {
            url = url.substring (HTTP_PREFIX.length ());
        } else if (url.startsWith (HTTPS_PREFIX)) {
            url = url.substring (HTTPS_PREFIX.length ());
        }

        if (url.endsWith ("/")) {
            url = url.substring (0, url.length () - 1);
        }

        if (url.startsWith (WWW_PREFIX)) {
            url = url.substring (WWW_PREFIX.length ());
        }

        return url;
    }

    public static String urlRemoveParams (String urlWithParam) {
        return urlWithParam.split ("\\?")[0];
    }

    public static String urlToHost (String url) {
        if (url.startsWith (HTTP_PREFIX)) {
            url = url.substring (HTTP_PREFIX.length ());
        } else if (url.startsWith (HTTPS_PREFIX)) {
            url = url.substring (HTTPS_PREFIX.length ());
        }

        try {
            return url.split ("/")[0];
        } catch (ArrayIndexOutOfBoundsException aioobe) {
            System.err.println (url + ", exception message: " + aioobe.getMessage ());
            aioobe.printStackTrace ();
        }

        return url;
    }

    public static boolean isIpStr (String host) {
        String[] tokens = host.split ("\\.");
        if (tokens.length != 4) {
//            System.out.println ("length is not 4");
            return false;
        }
        boolean isIp = true;
        for (String token : tokens) {
            if (StringUtils.isNumeric (token)) {
                try {
                    int value = Integer.valueOf (token);
                    if (value < 0 || value > 255) {
                        //                    System.out.println ("value beyond 0 and 255");
                        isIp = false;
                        break;
                    }
                } catch (NumberFormatException nfe) {
                    System.out.println ("invalid number in host: " + host);
                }
            } else {
//                System.out.println ("token is not number");
                isIp = false;
                break;
            }
        }
        return isIp;
    }


    public static String hostToDomain (String hostName) {
        String host = "";
        int port;
        if (hostName.contains (":")) {
            String[] tokens = hostName.split (":");
            if (tokens.length == 2 && StringUtils.isNumeric (tokens[1])) {
                host = tokens[0];
                port = Integer.valueOf (tokens[1]);
            }
        } else {
            host = hostName;
        }

        if (isIpStr (host)) {
            return host;
        }

        String[] tokens = host.split ("\\.");
        List<String> domainTokens = new ArrayList<> ();
        for (int i = tokens.length - 1; i >= 0; i--) {
            domainTokens.add (tokens[i]);
            if (!suffixSet.contains (tokens[i])) {
                break;
            }
        }

        Collections.reverse (domainTokens);

        return String.join (".", domainTokens);
    }

    public static void parseUrl (String url, Map<String, String> urlMap) {
        urlMap.clear ();
        String query;
        int pos = url.indexOf ('?');
        if (pos < 0) {
            pos = url.indexOf ("��");
            if (pos < 0) {
                return;
            } else {
                query = url.substring (pos + 2);
            }
        } else {
            query = url.substring (pos + 1);
        }
        String[] segs = query.split ("&", 0);
        for (String s : segs) {
            String[] kvs = s.split ("=", 2);
            if (kvs.length < 2) {
                continue;
            }
            String key = kvs[0];
            String value = transStr (kvs[1]);
            if (value.length () < 4) {
                continue;
            }
            urlMap.put (key, value);
        }
    }

    public static void parseCookie (String cookie, Map<String, String> cookieMap) {
        cookieMap.clear ();
        String[] segs = cookie.split (";", 0);
        for (String s : segs) {
            String[] kvs = s.split ("=", 2);
            if (kvs.length < 2) {
                continue;
            }
            String key = kvs[0].trim ();
            String value = transStr (kvs[1].trim ());
            if (value.length () < 4) {
                continue;
            }
            cookieMap.put (key, value);
        }
    }

    public static AhoCorasickDoubleArrayTrie<List<String>> buildACMachine (String filename) {
        try {

            TreeMap<String, List<String>> map = new TreeMap<String, List<String>> ();
            BufferedReader reader = new BufferedReader (new FileReader (filename));
            String line = null;
            while ((line = reader.readLine ()) != null) {
                line = line.trim ();
//                System.out.println (line);
                int index = StringUtils.lastIndexOf (line, ":");
                if (index > 0) {
                    String urlPattern = urlFormat (line.substring (0, index).trim ());
                    line = line.substring (index + 1);
                    String[] tags = line.trim ().split (",");
//                    System.out.println ("put url: " + urlPattern);
                    map.put (urlPattern, Arrays.asList (tags));
                }
            }
            AhoCorasickDoubleArrayTrie<List<String>> acdat = new AhoCorasickDoubleArrayTrie<List<String>> ();
            acdat.build (map);

            return acdat;
        } catch (FileNotFoundException fnfe) {
            logger.error ("File not found");
        } catch (IOException ioe) {
            logger.error ("Read file failed");
        }

        return null;
    }

    private static class UrlCodeTrans {
        private static Pattern validStandard = Pattern.compile ("%([0-9A-Fa-f]{2})");
        // private static Pattern choppedStandard =
        // Pattern.compile("%[0-9A-Fa-f]{0,1}$");
        private static Pattern validNonStandard = Pattern.compile ("%u([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})");
        // private static Pattern choppedNonStandard =
        // Pattern.compile("%u[0-9A-Fa-f]{0,3}$");

        static String resilientUrlDecode (String input) {
            String cookedInput = input;

            String inputEnc = "UTF-8";

            // Handle non standard (rejected by W3C) encoding that is used anyway by some
            // See: https://stackoverflow.com/a/5408655/114196
            if (cookedInput.contains ("%u")) {
                inputEnc = "UTF-16";
                // Transform all existing UTF-8 standard into UTF-16 standard.
                cookedInput = validStandard.matcher (cookedInput).replaceAll ("%00%$1");

                // Discard chopped encoded char at the end of the line (there is no way to know
                // what it was)
                // cookedInput = choppedStandard.matcher(cookedInput).replaceAll("");
                // Transform all existing non standard into UTF-16 standard.
                cookedInput = validNonStandard.matcher (cookedInput).replaceAll ("%$1%$2");

                // Discard chopped encoded char at the end of the line
                // cookedInput = choppedNonStandard.matcher(cookedInput).replaceAll("");
            }

            try {
                cookedInput = URLDecoder.decode (cookedInput, inputEnc);
                if (cookedInput.contains ("\\u")) {
                    cookedInput = StringEscapeUtils.unescapeJava (cookedInput);
                }
            } catch (Exception e) {
                // Will never happen because the encoding is hardcoded
                cookedInput = "";
            }
            return cookedInput;
        }
    }

    public static String transStr (String raw) {
        return UrlCodeTrans.resilientUrlDecode (raw);
    }

    public static void main (String[] args) {
        PropertyConfigurator.configure ("conf/log4j.properties");
        // Collect test data set
        TreeMap<String, String> map = new TreeMap<String, String> ();
        String[] keyArray = new String[]
                {
                        "hers",
                        "his",
                        "she",
                        "he"
                };
        for (String key : keyArray) {
            map.put (key, key);
        }
        // Build an AhoCorasickDoubleArrayTrie
        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<String> ();
        acdat.build (map);
        // Test it
        final String text = "uhers";
        List<AhoCorasickDoubleArrayTrie<String>.Hit<String>> wordList = acdat.parseText (text);
        for (AhoCorasickDoubleArrayTrie<String>.Hit<String> hit : wordList) {
            System.out.println ("begin: " + hit.begin + ", end: " + hit.end + ", value: " + hit.value);
        }

        String url = "http://www.baidu.com";
        System.out.println (urlFormat (url));
        System.out.println (urlToHost (url));
        url = "https://mail.163.com";
        System.out.println (urlFormat (url));
        System.out.println (urlToHost (url));
        url = "https://";
        System.out.println (urlFormat (url));
        System.out.println (urlToHost (url));
        url = "//";
        System.out.println (urlFormat (url));
        System.out.println (urlToHost (url));
        url = "http://\\";
//        System.out.println (urlFormat (url));
        System.out.println (urlToHost (url));

        logger.info ("begin buildACMachine");
        AhoCorasickDoubleArrayTrie<List<String>> ac = buildACMachine (URL_TAGS_FILE);
        logger.info ("finish buildACMachine");
        url = "http://ad.afy11.net/aaa.html";
        List<AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>>> tagList = ac.parseText (url);
        if (tagList != null) {
            for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                for (String tag : hit.value) {
                    System.out.println ("matched tag: " + tag);
                }
            }
        }

        url = "https://m.weibo.cn/openapi/sina/hotweibov8?callback=jsonp_08643580556263675&timestamp=1520482430235&&nopic=0&page=0&vid=2675466097425.987.1517974409469";
        String cookie = "UOR=115.29.173.59:9988,www.sina.com.cn,; SGUID=1517802309476_82399729; SINAGLOBAL=115.29.165.122_1517802310.450373; SUB=_2AkMtJvXWf8NxqwJRmPAVxWjlb4RzywHEieKbegQNJRMyHRl-yD83qhxatRB6BqbbOXbEmeUJcos3Fh9gVX_xdf4wd0Gx; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WhNMfP5Jn0YR6vdYsvud2Lr; CNZZDATA5580073=cnzz_eid%3D1984850260-1517797911-null%26ntime%3D1517997642; __utmz=269849203.1519397786.7.7.utmcsr=tianya.cn|utmccn=(referral)|utmcmd=referral|utmcct=/m/; U_TRS1=00000028.55141b35.5a952b99.85ea4f16; lxlrtst=1519814943_o; lxlrttp=1519814943; Apache=115.29.165.122_1520482424.514481; ULV=1520482425779:10:2:2:115.29.165.122_1520482424.514481:1520482424101; CNZZDATA1271230489=292927634-1517800554-null%7C1520478855; __utma=269849203.1450120486.1517815485.1519822170.1520482428.11; __utmc=269849203; __utmb=269849203.1.10.1520482428";
        Map<String, String> urlMap = new HashMap<String, String> ();
        Utils.parseUrl (url, urlMap);
        Set<Map.Entry<String, String>> entries = urlMap.entrySet ();
        for (Map.Entry<String, String> entry : urlMap.entrySet ()) {
            System.out.println (entry.getKey () + ": " + entry.getValue ());
        }

        System.out.println ("Cookies: ");
        Map<String, String> cookieMap = new HashMap<String, String> ();
        Utils.parseCookie (cookie, cookieMap);
        for (Map.Entry<String, String> entry : cookieMap.entrySet ()) {
            System.out.println (entry.getKey () + ": " + entry.getValue ());
        }

        String host = "www.baidu.com.cn";
        System.out.println (isIpStr (host));
        System.out.println (hostToDomain (host));
        host = "127.9.0.1";
        System.out.println (isIpStr (host));
        System.out.println (hostToDomain (host));
        host = "1232.232.444.424";
        System.out.println (isIpStr (host));
        System.out.println (hostToDomain (host));
        host = "sports.sina.com.cn";
        System.out.println (isIpStr (host));
        System.out.println (hostToDomain (host));
    }
}
