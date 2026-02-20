package it.eng.idra.utils;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Small collection of utilities extracted from CswConnector to keep helpers together.
 */
public class GeoNetworkConnectorUtils {

    private static final Logger logger = LogManager.getLogger(GeoNetworkConnectorUtils.class);
    
    public static String firstNonEmpty(String... vals) {
        if (vals == null) return "";
        for (String v : vals) if (v != null && !v.trim().isEmpty()) return v.trim();
        return "";
    }

    public static String textNS(Element parent, String ns, String local) {
        NodeList nl = parent.getElementsByTagNameNS(ns, local);
        if (nl.getLength() == 0) return "";
        return nl.item(0).getTextContent().trim();
    }

    public static String truncate(String s, int max) {
        if (s == null) return "";
        if (s.length() <= max) return s;
        return s.substring(0, Math.max(0, max - 1)) + "…";
    }

    
    public static String safeIsoText(Element root, String[][] pathOptions) {
        final String GMD_NS   = "http://www.isotc211.org/2005/gmd";
        final String GCO_NS   = "http://www.isotc211.org/2005/gco";
        final String GML_NS   = "http://www.opengis.net/gml";      // GML 3.1
        final String GMX_NS   = "http://www.isotc211.org/2005/gmx";
        final String GML32_NS = "http://www.opengis.net/gml/3.2";   // GML 3.2 fallback
        final String XLINK_NS = "http://www.w3.org/1999/xlink";

        for (String[] path : pathOptions) {
            Element cur = root;
            boolean ok = true;

            for (String step : path) {
                String ns;
                String local;

                if (step.contains(":")) {
                    String[] px = step.split(":", 2);
                    String p = px[0], l = px[1];
                    local = l;
                    if ("gmd".equals(p))      ns = GMD_NS;
                    else if ("gco".equals(p)) ns = GCO_NS;
                    else if ("gml".equals(p)) ns = GML_NS;
                    else if ("gmx".equals(p)) ns = GMX_NS;
                    else                      ns = null;
                } else {
                    ns = null;
                    local = step;
                }

                NodeList nl = (ns == null)
                        ? cur.getElementsByTagName(local)
                        : cur.getElementsByTagNameNS(ns, local);

                // Fallback: if looking for GML 3.1 and nothing found, retry with GML 3.2
                if (nl.getLength() == 0 && GML_NS.equals(ns)) {
                    nl = cur.getElementsByTagNameNS(GML32_NS, local);
                }

                if (nl.getLength() == 0) { ok = false; break; }
                cur = (Element) nl.item(0);
            }

            if (ok) {
                // 1) Preferred: gco:CharacterString
                String txt = firstChildText(cur, "http://www.isotc211.org/2005/gco", "CharacterString");
                if (!txt.isEmpty()) return txt;

                // 2) gmx:Anchor text
                String anchorText = firstChildText(cur, "http://www.isotc211.org/2005/gmx", "Anchor");
                if (!anchorText.isEmpty()) return anchorText;

                // 3) gmx:Anchor @xlink:href
                String anchorHref = firstChildAttr(cur, "http://www.isotc211.org/2005/gmx", "Anchor", XLINK_NS, "href");
                if (!anchorHref.isEmpty()) return anchorHref;

                // 4) Fallback: element text content
                String all = cur.getTextContent();
                if (all != null) {
                    all = all.trim();
                    if (!all.isEmpty()) return all;
                }
            }
        }
        return "";
    }


    public static String sha1OfElement(org.w3c.dom.Element el) {
        try {
            javax.xml.transform.Transformer tf = javax.xml.transform.TransformerFactory.newInstance().newTransformer();
            tf.setOutputProperty(javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION, "yes");
            java.io.StringWriter sw = new java.io.StringWriter();
            tf.transform(new javax.xml.transform.dom.DOMSource(el), new javax.xml.transform.stream.StreamResult(sw));
            byte[] bytes = sw.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-1");
            byte[] dig = md.digest(bytes);
            StringBuilder sb = new StringBuilder();
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(el.hashCode());
        }
    }

    public static String normalizeUrl(String url) {
        if (url == null) return "";
        String u = url.trim();
        if (u.isEmpty()) return "";
        if (u.startsWith("<") && u.endsWith(">")) {
            u = u.substring(1, u.length() - 1).trim();
        }
        u = u.replace(" ", "%20");
        if (u.matches("^[a-zA-Z][a-zA-Z0-9+\\-.]*:.*")) {
            if (u.toLowerCase().startsWith("mailto:")) return "";
            return u;
        }
        if (u.startsWith("//")) return "https:" + u;
        if (u.startsWith("www.")) u = "https://" + u;
        else u = "https://" + u;
        try {
            new java.net.URI(u);
            return u;
        } catch (Exception e) {
            return "";
        }
    }

    public static boolean isDownloadLink(String funcCode, String protocol, String url) {
        String f = (funcCode == null ? "" : funcCode.toLowerCase());
        String p = (protocol == null ? "" : protocol.toLowerCase());
        String u = (url == null ? "" : url.toLowerCase());
        // Explicit function code wins
        if (f.contains("download")) return true;

        // Never consider GetCapabilities or plain WMS GetMap as download
        if (u.contains("request=getcapabilities")) return false;
        if (u.contains("service=wms")) return false;

        // WCS GetCoverage (or protocol hints) -> download
        if (u.contains("service=wcs") && u.contains("request=getcoverage")) return true;
        if (p.contains("get-coverage") || p.contains("download")) return true;

        // File-like endings
        if (u.matches(".*\\.(zip|7z|gz|tgz|tar|csv|tsv|xls|xlsx|json|geojson|kml|kmz|tif|tiff|geotiff|gpkg|shp)(\\?.*)?$") ) return true;

        return false;
    }

    public static String guessFormat(String url, String protocol, String defaultFmt) {
        String fmt = CommonUtil.extractFormatFromFileExtension(url);
        if (fmt != null && !fmt.isEmpty()) return fmt;
        String p = (protocol == null ? "" : protocol.toUpperCase());
        if (p.contains("KML")) return "KML";
        if (p.contains("WMS")) return "WMS";
        if (p.contains("WCS")) return "WCS";
        if (defaultFmt != null && !defaultFmt.trim().isEmpty()) return defaultFmt.trim();
        return "";
    }

    public static String guessMediaType(String url, String protocol) {
        String u = (url == null ? "" : url.toLowerCase());
        String p = (protocol == null ? "" : protocol.toLowerCase());

        // Files and explicit formats
        if (u.endsWith(".kml") || p.contains("kml")) return "application/vnd.google-earth.kml+xml";
        if (u.endsWith(".kmz")) return "application/vnd.google-earth.kmz";
        if (u.endsWith(".geojson") || p.contains("geojson")) return "application/geo+json";
        if (u.endsWith(".csv") || p.contains("csv")) return "text/csv";
        if (u.endsWith(".json") || p.contains("json")) return "application/json";
        if (u.endsWith(".tif") || u.endsWith(".tiff") || u.contains("geotiff") || p.contains("geotiff")) return "image/tiff";
        if (u.endsWith(".zip")) return "application/zip";

        // Services: avoid forcing application/xml; let it be empty
        if (u.contains("request=getcapabilities")) return "";
        if (u.contains("service=wms")) return "";
        if (u.contains("service=wcs") && !u.contains("request=getcoverage")) return "";

        return "";
    }

    public static String hostnameOf(String url) {
        try {
            java.net.URI uri = java.net.URI.create(url);
            String h = uri.getHost();
            return (h != null) ? h : url;
        } catch (Exception e) {
            return url;
        }
    }

    public static String extractAccessRightsShort(org.w3c.dom.Element recordElem) {
        final String GMD_NS = "http://www.isotc211.org/2005/gmd";
        final String GMX_NS = "http://www.isotc211.org/2005/gmx";
        final String GCO_NS = "http://www.isotc211.org/2005/gco";
        String restr = readRestrictionCode(recordElem);
        if (!restr.isEmpty()) {
            String norm = normalizeRestriction(restr);
            if (!norm.isEmpty()) return norm;
        }
        NodeList legalList = recordElem.getElementsByTagNameNS(GMD_NS, "MD_LegalConstraints");
        for (int i = 0; i < legalList.getLength(); i++) {
            org.w3c.dom.Element legal = (org.w3c.dom.Element) legalList.item(i);
            NodeList anchors = legal.getElementsByTagNameNS(GMX_NS, "Anchor");
            for (int j = 0; j < anchors.getLength(); j++) {
                String t = anchors.item(j).getTextContent();
                if (t != null) {
                    t = t.trim().toLowerCase();
                    if (t.contains("geen beperkingen") || t.contains("no limitations")) {
                        return "PUBLIC";
                    }
                    if (t.contains("modellicentie") || t.contains("gratis hergebruik")) {
                        return "OPEN";
                    }
                }
                if (anchors.item(j) instanceof org.w3c.dom.Element) {
                    org.w3c.dom.Element a = (org.w3c.dom.Element) anchors.item(j);
                    String href = a.getAttributeNS("http://www.w3.org/1999/xlink", "href");
                    if (href != null) {
                        String h = href.toLowerCase();
                        if (h.contains("limitationsonpublicaccess/nolimitations")) return "PUBLIC";
                        if (h.contains("modellicentie-gratis-hergebruik")) return "OPEN";
                    }
                }
            }
            NodeList strings = legal.getElementsByTagNameNS(GCO_NS, "CharacterString");
            for (int j = 0; j < strings.getLength(); j++) {
                String t = strings.item(j).getTextContent();
                if (t == null) continue;
                String low = t.trim().toLowerCase();
                if (low.contains("geen beperkingen") || low.contains("no limitations")) return "PUBLIC";
                if (low.contains("modellicentie") || low.contains("gratis hergebruik")) return "OPEN";
            }
        }
        return "OTHER";
    }

    public static String readRestrictionCode(org.w3c.dom.Element recordElem) {
        final String GMD_NS = "http://www.isotc211.org/2005/gmd";
        final String GCO_NS = "http://www.isotc211.org/2005/gco";
        NodeList accs = recordElem.getElementsByTagNameNS(GMD_NS, "accessConstraints");
        for (int i = 0; i < accs.getLength(); i++) {
            org.w3c.dom.Element acc = (org.w3c.dom.Element) accs.item(i);
            NodeList codes = acc.getElementsByTagNameNS(GMD_NS, "MD_RestrictionCode");
            if (codes.getLength() > 0) {
                org.w3c.dom.Element code = (org.w3c.dom.Element) codes.item(0);
                String val = code.getAttribute("codeListValue");
                if (val != null && !val.trim().isEmpty()) return val.trim();
                String txt = code.getTextContent();
                if (txt != null && !txt.trim().isEmpty()) return txt.trim();
            }
        }
        NodeList legalList = recordElem.getElementsByTagNameNS(GMD_NS, "MD_LegalConstraints");
        for (int i = 0; i < legalList.getLength(); i++) {
            org.w3c.dom.Element legal = (org.w3c.dom.Element) legalList.item(i);
            NodeList strings = legal.getElementsByTagNameNS(GCO_NS, "CharacterString");
            for (int j = 0; j < strings.getLength(); j++) {
                String t = strings.item(j).getTextContent();
                if (t != null && !t.trim().isEmpty()) {
                    String low = t.trim().toLowerCase();
                    if (low.equals("restricted") || low.equals("license") || low.equals("otherrestrictions")) {
                        return t.trim();
                    }
                }
            }
        }
        return "";
    }

    public static String normalizeRestriction(String val) {
        String x = val.trim().toLowerCase();
        if (x.contains("restricted")) return "RESTRICTED";
        if (x.contains("license")) return "LICENSED";
        if (x.contains("other")) return "OTHER";
        if (x.contains("no limitations") || x.contains("geen beperkingen")) return "PUBLIC";
        return "";
    }

    public static HttpURLConnection openFollowingRedirects(String initialUrl) throws Exception {

        if (initialUrl == null) throw new IllegalArgumentException("initialUrl is null");
        String u = initialUrl.trim();
        if (u.isEmpty()) throw new IllegalArgumentException("initialUrl is empty");

        URL url = new URL(u);
        int redirects = 0;
        int maxRedirects = 10;
        int max503Retries = 2;
        int retry503 = 0;

        while (true) {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            // We will follow redirects manually so we can handle protocol/relative Location correctly
            conn.setInstanceFollowRedirects(false);
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            conn.setRequestProperty("Accept", "application/xml");
            conn.setRequestProperty("User-Agent", "Idra-CSW-Connector/1.0");
            conn.setRequestMethod("GET");
            conn.connect();

            int code = conn.getResponseCode();
            String ct = conn.getContentType();

            logger.info("CSW code={} url='{}' contentType='{}'", code, url.toString(), ct);

            // Successful
            if (code >= 200 && code < 300) {
                if (ct != null && ct.toLowerCase().contains("text/html")) {
                    conn.disconnect();
                    throw new Exception("Unexpected HTML response (possible portal/login)");
                }
                return conn;
            }

            // Redirects
            if (code == HttpURLConnection.HTTP_MOVED_PERM /*301*/
                    || code == HttpURLConnection.HTTP_MOVED_TEMP /*302*/
                    || code == HttpURLConnection.HTTP_SEE_OTHER /*303*/
                    || code == 307 /*HTTP/1.1 307*/
                    || code == 308 /*HTTP/1.1 308*/) {
                String loc = conn.getHeaderField("Location");
                conn.disconnect();
                if (loc == null || loc.trim().isEmpty()) {
                    throw new Exception("Redirect (" + code + ") without Location header");
                }
                // Resolve relative redirects against the current URL
                URL next = new URL(url, loc);
                redirects++;
                if (redirects > maxRedirects) {
                    throw new Exception("Too many redirects");
                }
                url = next;
                // reset 503 retry counter when we change URL
                retry503 = 0;
                continue;
            }

            // 503 Service Unavailable: allow a small number of retries with short backoff
            if (code == HttpURLConnection.HTTP_UNAVAILABLE) {
                conn.disconnect();
                if (retry503 < max503Retries) {
                    retry503++;
                    try { Thread.sleep(1000L * retry503); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    continue;
                }
                throw new Exception("HTTP 503 Service Unavailable after retries");
            }

            // Other errors
            conn.disconnect();
            throw new Exception("HTTP " + code);
        }
    }

    public static String ensureDateTimeZ(String in) {
        if (in == null) return "";
        String s = in.trim();
        if (s.isEmpty()) return "";
        if (s.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$")) return s;
        if (s.matches("^\\d{4}-\\d{2}-\\d{2}$")) return s + "T00:00:00Z";
        if (s.matches("^\\d{4}-\\d{2}$")) return s + "-01T00:00:00Z";
        if (s.matches("^\\d{4}$")) return s + "-01-01T00:00:00Z";
        if (s.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$")) return s + "Z";
        return s;
    }

    public static String safeIsoTextOrAnchorHref(Element root, String[][] pathOptions) {
        final String GMD_NS = "http://www.isotc211.org/2005/gmd";
        final String GCO_NS = "http://www.isotc211.org/2005/gco";
        final String GMX_NS = "http://www.isotc211.org/2005/gmx";
        for (String[] path : pathOptions) {
            Element cur = root; boolean ok = true;
            for (String step : path) {
                String ns; String local;
                if (step.contains(":")) {
                    String[] px = step.split(":", 2);
                    String p = px[0], l = px[1];
                    local = l;
                    if ("gmd".equals(p)) ns = GMD_NS;
                    else if ("gco".equals(p)) ns = GCO_NS;
                    else if ("gmx".equals(p)) ns = GMX_NS;
                    else ns = null;
                } else { ns = null; local = step; }
                NodeList nl = (ns == null) ? cur.getElementsByTagName(local) : cur.getElementsByTagNameNS(ns, local);
                if (nl.getLength() == 0) { ok = false; break; }
                cur = (Element) nl.item(0);
            }
            if (ok) {
                if ("Anchor".equals(cur.getLocalName()) && "http://www.isotc211.org/2005/gmx".equals(cur.getNamespaceURI())) {
                    String href = cur.getAttributeNS("http://www.w3.org/1999/xlink", "href");
                    if (href != null && !href.trim().isEmpty()) return href.trim();
                }
                String txt = cur.getTextContent();
                if (txt != null && !txt.trim().isEmpty()) return txt.trim();
            }
        }
        return "";
    }

    public static String cap(String s, int max) {
        if (s == null) return "";
        String t = s.trim();
        if (t.length() <= max) return t;
        return t.substring(0, max);
    }



    /** Returns trimmed text of the first child with given ns/local name, or "" */
    private static String firstChildText(Element parent, String ns, String local) {
        NodeList nl = parent.getElementsByTagNameNS(ns, local);
        if (nl.getLength() == 0) return "";
        String t = nl.item(0).getTextContent();
        return (t == null) ? "" : t.trim();
    }

    /** Returns attribute (possibly namespaced) of first child with given ns/local, or "" */
    private static String firstChildAttr(Element parent, String ns, String local, String attrNs, String attrLocal) {
        NodeList nl = parent.getElementsByTagNameNS(ns, local);
        if (nl.getLength() == 0) return "";
        Element e = (Element) nl.item(0);
        String v = (attrNs == null) ? e.getAttribute(attrLocal) : e.getAttributeNS(attrNs, attrLocal);
        return (v == null) ? "" : v.trim();
    }

    /** Normalizes language codes/labels to ISO639-1 when possible (e.g., dut/nld/Nederlands -> nl). */
    public static String normalizeLanguage(String lang) {
        if (lang == null) return "";
        String s = lang.trim().toLowerCase();
        if (s.isEmpty()) return "";
        if ("dut".equals(s) || "nld".equals(s) || s.startsWith("nederlands")) return "nl";
        if ("ita".equals(s) || s.startsWith("italiano")) return "it";
        if ("eng".equals(s) || s.startsWith("english")) return "en";
        // pass-through common 2-letter codes
        if (s.length() == 2) return s;
        return s;
    }

}