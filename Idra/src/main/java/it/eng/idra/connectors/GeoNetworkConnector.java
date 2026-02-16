package it.eng.idra.connectors;

import it.eng.idra.beans.dcat.DcatDataset;
import it.eng.idra.beans.dcat.DcatDistribution;
import it.eng.idra.beans.dcat.DctLocation;
import it.eng.idra.beans.dcat.DctPeriodOfTime;
import it.eng.idra.beans.dcat.FoafAgent;
import it.eng.idra.beans.dcat.SkosConceptTheme;
import it.eng.idra.beans.dcat.SkosConceptSubject;
import it.eng.idra.beans.dcat.DctStandard;
import it.eng.idra.beans.dcat.VcardOrganization;
import it.eng.idra.beans.odms.OdmsCatalogue;
import it.eng.idra.beans.odms.OdmsCatalogueNotFoundException;
import it.eng.idra.beans.odms.OdmsCatalogueForbiddenException;
import it.eng.idra.beans.odms.OdmsCatalogueOfflineException;
import it.eng.idra.beans.odms.OdmsSynchronizationResult;
import it.eng.idra.utils.CommonUtil;
import it.eng.idra.utils.GeoNetworkConnectorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.DCAT;
import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.net.CookieManager;
import java.net.CookieHandler;
import java.net.CookiePolicy;

/**
 * Connector for CSW (Catalog Service for the Web) endpoints.
 * Fetches ISO19139 metadata via CSW GetRecords/GetRecordById and maps to DCAT datasets.
 */
public class GeoNetworkConnector implements IodmsConnector {
	
	
	static {
	    CookieManager cm = new CookieManager();
	    cm.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
	    CookieHandler.setDefault(cm);
	}

	
	
	
    /** The ODMS catalogue node configuration. */
    private OdmsCatalogue node;
    /** Logger for CswConnector. */
    private static Logger logger = LogManager.getLogger(GeoNetworkConnector.class);

    /**
     * Constructs a new CswConnector for the given ODMS catalogue node.
     * @param node the OdmsCatalogue containing CSW endpoint info
     */
    public GeoNetworkConnector(OdmsCatalogue node) {
        this.node = node;
    }

    /**
     * Counts the datasets present in the CSW node.
     *
     * @return int number of datasets in the catalogue
     * @throws OdmsCatalogueOfflineException   if the catalogue is unreachable
     * @throws OdmsCatalogueNotFoundException  if the catalogue endpoint is not found
     * @throws OdmsCatalogueForbiddenException if access to the catalogue is forbidden
     * @throws Exception for general errors
     */
    @Override
    public int countDatasets() throws Exception {
        // Leverage getAllDatasets to retrieve and count all records:contentReference[oaicite:1]{index=1}
        int size = getAllDatasets().size();
        logger.info("CswConnector - countDatasets - size: " + size);
        return size;
    }

    /**
     * Retrieves all datasets from the CSW node.
     * Uses CSW GetRecords with ISO19139 output to fetch and parse all metadata records, with pagination support.
     *
     * @return List<DcatDataset> list of all datasets from the CSW
     * @throws OdmsCatalogueOfflineException   if the node is unreachable
     * @throws OdmsCatalogueNotFoundException  if the endpoint URL is not found
     * @throws OdmsCatalogueForbiddenException if access is forbidden
     * @throws Exception for general errors during fetch or parse
     */
    @Override
    public List<DcatDataset> getAllDatasets() throws Exception {
        logger.info("CswConnector - getAllDatasets - nodeId: " + node.getId());
        logger.info("CswConnector - getAllDatasets - node name: " + node.getName());
        logger.info("CswConnector - getAllDatasets - node host: " + node.getHost());

        ArrayList<DcatDataset> dcatResults = new ArrayList<>();

        // CSW paginated fetch: iterate using startPosition and maxRecords until all records are retrieved
        int startPosition = 1;
        int maxRecords = 100;  // fetch 100 records per page (adjustable)
        boolean moreRecords = true;
        logger.info("-- CSW Connector Request sent -- Fetching all records from CSW endpoint");
        try {

        	while (moreRecords) {
                // Construct CSW GetRecords URL for current page
        		
                String cswUrl = node.getHost()
                        + (node.getHost().contains("?") ? "&" : "?")
                        + "service=CSW&request=GetRecords&version=2.0.2"
                        + "&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd"
                        + "&typeNames=gmd:MD_Metadata&elementSetName=full"
                        + "&startPosition=" + startPosition + "&maxRecords=" + maxRecords
                        + "&namespace=xmlns(gmd,http://www.isotc211.org/2005/gmd),xmlns(csw,http://www.opengis.net/cat/csw/2.0.2)";

                // Include namespace for gmd if needed

                logger.info("CswConnector - getAllDatasets - fetching records "
                        + startPosition + " to " + (startPosition + maxRecords - 1));
                // Open HTTP connection to CSW
                logger.info("CSW request (before redirects): {}", cswUrl);

                HttpURLConnection conn;
                try {
                    conn = openFollowingRedirects(cswUrl);
                } catch (Exception e) {
                    String msg = e.getMessage() == null ? "" : e.getMessage();
                    if (msg.contains("HTTP 404")) throw new OdmsCatalogueNotFoundException("The ODMS host does not exist (HTTP 404)");
                    if (msg.contains("HTTP 403")) throw new OdmsCatalogueForbiddenException("The ODMS node is forbidden (HTTP 403)");
                    // Redirect non risolto / HTML inatteso / ecc. → consideralo unreachable
                    throw new OdmsCatalogueOfflineException("The ODMS node is currently unreachable: " + msg);
                }

                logger.info("CSW final URL (after redirects): {}", conn.getURL());
                logger.info("CSW Content-Type: {}", conn.getContentType());

                // Parse XML come prima
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(conn.getInputStream());
                conn.disconnect();


                // Extract total count and nextRecord from SearchResults attributes
                final String CSW_NS = "http://www.opengis.net/cat/csw/2.0.2";

                Element resultsElem = (Element) doc.getElementsByTagNameNS(CSW_NS, "SearchResults").item(0);

                if (resultsElem == null) {
                    // If SearchResults element missing, throw exception
                    throw new Exception("Invalid CSW response: missing SearchResults");
                }
                String matchedStr = resultsElem.getAttribute("numberOfRecordsMatched");
                String returnedStr = resultsElem.getAttribute("numberOfRecordsReturned");
                String nextStr = resultsElem.getAttribute("nextRecord");
                int matched = (matchedStr != null && !matchedStr.isEmpty()) ? Integer.parseInt(matchedStr) : 0;
                int returned = (returnedStr != null && !returnedStr.isEmpty()) ? Integer.parseInt(returnedStr) : 0;
                int next = (nextStr != null && !nextStr.isEmpty()) ? Integer.parseInt(nextStr) : 0;
                logger.info("CswConnector - getAllDatasets - received " + returned + " records (total matched: " + matched + ")");

                // Parse each metadata record (ISO19139 MD_Metadata) in the response
                final String GMD_NS = "http://www.isotc211.org/2005/gmd";
                NodeList recordNodes = doc.getElementsByTagNameNS(GMD_NS, "MD_Metadata");
                for (int i = 0; i < recordNodes.getLength(); i++) {
                    Element recordElem = (Element) recordNodes.item(i);
                    try {
                        DcatDataset dcat = datasetToDcat(recordElem, node);
                        if (dcat != null) {
                            dcatResults.add(dcat);

                        } else {
                            logger.warn("CSW record mappato a null, saltato (nodeId: {}, start: {}, idx: {})",
                                        node.getId(), startPosition, i);
                        }
                    } catch (Exception ex) {
                        // try to log a useful identifier
                        String recId = safeIsoText(recordElem, new String[][]{
                            {"gmd:fileIdentifier","gco:CharacterString"},
                            {"gmd:identifier","gmd:MD_Identifier","gmd:code","gco:CharacterString"}
                        });
                        logger.error("Errore mappando record CSW (fileIdentifier: {}). Saltato.", recId, ex);
                    }
                }

                // Determine if more records remain to fetch
                if (next > 0 && next <= matched) {
                    // More records available: set startPosition for next page and continue loop
                    startPosition = next;
                    logger.info("CswConnector - getAllDatasets - fetched up to " + (next - 1)
                            + ", continuing to next page at startPosition " + startPosition);
                } else {
                    // No more records to fetch
                    moreRecords = false;
                    logger.info("CswConnector - getAllDatasets - all records fetched (" + dcatResults.size() + " datasets)");
                }
                
            }
        } catch (Exception e) {
            // In caso di errore, mappa l'eccezione su quelle specifiche ODMS o rilancia generica
            handleError(e);
        }
        

    // --- de-duplicate by identifier (keep the first encountered) ---
           LinkedHashMap<String, DcatDataset> byId = new LinkedHashMap<>();
           for (DcatDataset d : dcatResults) {
               if (d == null) continue;
               String id = (d.getIdentifier() != null) ? d.getIdentifier().toString().trim() : "";
               if (!id.isEmpty() && !byId.containsKey(id)) {
                   byId.put(id, d);
               }
           }
           dcatResults = new ArrayList<>(byId.values());
           logger.info("CSW connector: {} dataset dopo dedup.", dcatResults.size());
        
        return dcatResults;
        
    }

    /**
     * Maps a single CSW ISO19139 metadata record to a DCAT dataset.
     *
     * @param recordObj the metadata record object (XML Element gmd:MD_Metadata)
     * @param node      the OdmsCatalogue node information
     * @return DcatDataset mapped dataset
     * @throws Exception if parsing fails or required fields missing
     */
    @Override
    public DcatDataset datasetToDcat(Object recordObj, OdmsCatalogue node) throws Exception {
        final String GMD_NS = "http://www.isotc211.org/2005/gmd";
        final String GML_NS = "http://www.opengis.net/gml";

        Element recordElem = (Element) recordObj;
        final String nodeId = Integer.toString(node.getId()); // <-- int → String

        // --- Identifier (fileIdentifier -> MD_Identifier -> fallback UUID) ---
        String identifier = safeIsoText(recordElem, new String[][]{
            {"gmd:fileIdentifier","gco:CharacterString"},
            {"gmd:identifier","gmd:MD_Identifier","gmd:code","gco:CharacterString"}
        });
        identifier = (identifier != null) ? identifier.trim() : "";

        if (identifier.isEmpty()) {
            // Deterministic fallback: SHA-1 of the record XML
            identifier = "csw-" + sha1OfElement(recordElem);
            // If preferred, you can also choose to SKIP the record:
            // logger.warn("Record without identifier, skipped");
            // return null;
        }

        // --- Title (gmd:title gco:CharacterString -> PT_FreeText LocalisedCharacterString) ---
        String title = safeIsoText(recordElem, new String[][]{
            {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:citation","gmd:CI_Citation","gmd:title","gco:CharacterString"},
            {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:citation","gmd:CI_Citation","gmd:title","gmd:PT_FreeText","gmd:textGroup","gmd:LocalisedCharacterString"}
        });
        
        if (title == null || title.isEmpty()) {
            title = identifier; // fallback semplice e stabile
        }
        
        // --- Landing page
        String landing = firstNonEmpty(
            safeIsoText(recordElem, new String[][]{
                {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:citation","gmd:CI_Citation","gmd:identifier","gmd:MD_Identifier","gmd:code","gco:CharacterString"}
            }),
            safeIsoText(recordElem, new String[][]{
                {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:citation","gmd:CI_Citation","gmd:identifier","gmd:MD_Identifier","gmd:code","gmx:Anchor"}
            })
        );
        if (landing != null && !landing.isEmpty() && !landing.startsWith("http")) {
            landing = "https://" + landing; // normalize if scheme is missing
        }


        // --- Description / Abstract ---
        String description = safeIsoText(recordElem, new String[][]{
            {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:abstract","gco:CharacterString"},
            {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:abstract","gmd:PT_FreeText","gmd:textGroup","gmd:LocalisedCharacterString"}
        });
        

     // --- Keywords extraction from ISO19139 (gmd:keyword using gco:CharacterString or PT_FreeText) ---
        java.util.List<String> keywords = new java.util.ArrayList<>();
        org.w3c.dom.NodeList kwCS = recordElem.getElementsByTagNameNS(GMD_NS, "keyword");

        for (int i = 0; i < kwCS.getLength(); i++) {
            Element kw = (Element) kwCS.item(i);

            // Extract keyword text from either gco:CharacterString or gmd:PT_FreeText
            String k = safeIsoText(kw, new String[][] {
                {"gco:CharacterString"},
                {"gmd:PT_FreeText", "gmd:textGroup", "gmd:LocalisedCharacterString"}
            });
            if (k != null) {
                k = k.trim();
                if (!k.isEmpty()) {
                    // Split keyword into words (separated by spaces)
                    String[] words = k.split("\\s+");
                    // Limit keywords to a maximum of 3 words (truncate longer ones)
                    if (words.length > 3) {
                        k = String.join(" ", java.util.Arrays.copyOf(words, 3));
                    }
                    // Cap string length to avoid exceeding DB column limits (e.g., VARCHAR(255))
                    k = GeoNetworkConnectorUtils.cap(k, 250);
                }
            }
            // Add keyword if valid, non-empty, and not already included
            if (k != null && !k.isEmpty() && !keywords.contains(k)) {
                keywords.add(k);
            }
        }


        // --- Dates (release/publication + update from dateStamp) ---
        String releaseDate = "";
        String updateDate  = "";
        // CI_Date with dateType=publication
        org.w3c.dom.NodeList ciDates = recordElem.getElementsByTagNameNS(GMD_NS, "CI_Date");
        for (int i = 0; i < ciDates.getLength(); i++) {
            Element ci = (Element) ciDates.item(i);
            String dateType = safeIsoText(ci, new String[][]{
                {"gmd:dateType","gmd:CI_DateTypeCode"},
                {"gmd:dateType","gmd:CI_DateTypeCode","gco:CharacterString"} // in some profiles
            }).toLowerCase();
            if (dateType.contains("publication")) {
                String d = safeIsoText(ci, new String[][]{
                    {"gmd:date","gco:DateTime"},
                    {"gmd:date","gco:Date"}
                });
                if (d != null && !d.isEmpty()) {
                    releaseDate = d;
                    break;
                }
            }
        }
        // dateStamp as update (and fallback for release)
        
      // Normalize to a Solr-safe format first, then let CommonUtil refine it
        if (releaseDate != null && !releaseDate.isEmpty()) releaseDate = ensureDateTimeZ(releaseDate);
        if (updateDate  != null && !updateDate.isEmpty())  updateDate  = ensureDateTimeZ(updateDate);
        try { if (releaseDate != null && !releaseDate.isEmpty()) releaseDate = CommonUtil.fixBadUtcDate(releaseDate); } catch (Exception ignore) {}
        try { if (updateDate  != null && !updateDate.isEmpty())  updateDate  = CommonUtil.fixBadUtcDate(updateDate);  } catch (Exception ignore) {}

      // --- Normalize dates to full ISO 8601 format ---
        if (releaseDate != null && releaseDate.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
            releaseDate = releaseDate + "T00:00:00Z";
        }
        if (updateDate != null && updateDate.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
            updateDate = updateDate + "T00:00:00Z";
        }
        
        if (releaseDate == null || releaseDate.isEmpty()) releaseDate = "1970-01-01T00:00:00Z";
        if (updateDate == null || updateDate.isEmpty()) updateDate = "1970-01-01T00:00:00Z";

        // --- Spatial extent (EX_GeographicBoundingBox) ---
        DctLocation spatialCoverage = null;
        org.w3c.dom.NodeList bboxes = recordElem.getElementsByTagNameNS(GMD_NS, "EX_GeographicBoundingBox");
        if (bboxes.getLength() > 0) {
            Element bbox = (Element) bboxes.item(0);
            String west  = safeIsoText(bbox, new String[][]{{"gmd:westBoundLongitude","gco:Decimal"}});
            String east  = safeIsoText(bbox, new String[][]{{"gmd:eastBoundLongitude","gco:Decimal"}});
            String south = safeIsoText(bbox, new String[][]{{"gmd:southBoundLatitude","gco:Decimal"}});
            String north = safeIsoText(bbox, new String[][]{{"gmd:northBoundLatitude","gco:Decimal"}});
            if (west!=null && east!=null && south!=null && north!=null) {
                String bboxValue = west + "," + south + "," + east + "," + north;
                spatialCoverage = new DctLocation(DCTerms.spatial.getURI(), "", "", bboxValue,nodeId,bboxValue, "");
            }
        }

        // --- Temporal extent (gml:TimePeriod) ---
        DctPeriodOfTime temporalCoverage = null;
        org.w3c.dom.NodeList tps = recordElem.getElementsByTagNameNS(GML_NS, "TimePeriod");
        if (tps.getLength() > 0) {
            Element tp = (Element) tps.item(0);
            String begin = firstNonEmpty(
                textNS(tp, GML_NS, "beginPosition"),
                safeIsoText(tp, new String[][]{{"gml:begin","gml:TimeInstant","gml:timePosition"}})
            );
            String end = firstNonEmpty(
                textNS(tp, GML_NS, "endPosition"),
                safeIsoText(tp, new String[][]{{"gml:end","gml:TimeInstant","gml:timePosition"}})
            );
            if ((begin!=null && !begin.isEmpty()) || (end!=null && !end.isEmpty())) {
                temporalCoverage = new DctPeriodOfTime(DCTerms.temporal.getURI(), begin, end, nodeId,null,null);
            }
        }

      // --- Distributions (only the actual ones under distributionInfo) ---
        String defaultFormat = safeIsoText(recordElem, new String[][]{
            {"gmd:distributionInfo","gmd:MD_Distribution","gmd:distributionFormat","gmd:MD_Format","gmd:name","gco:CharacterString"}
        });
        if (defaultFormat == null || defaultFormat.isEmpty()) defaultFormat = "UNKNOWN";

        java.util.List<DcatDistribution> distributionList = new java.util.ArrayList<>();
        java.util.Set<String> seenLinks = new java.util.HashSet<>();

        NodeList mdDistributions = recordElem.getElementsByTagNameNS(GMD_NS, "MD_Distribution");
        for (int d = 0; d < mdDistributions.getLength(); d++) {
            Element mdDist = (Element) mdDistributions.item(d);

            NodeList transferOpts = mdDist.getElementsByTagNameNS(GMD_NS, "MD_DigitalTransferOptions");
            for (int t = 0; t < transferOpts.getLength(); t++) {
                Element dto = (Element) transferOpts.item(t);

                NodeList onlines = dto.getElementsByTagNameNS(GMD_NS, "CI_OnlineResource");
                for (int i = 0; i < onlines.getLength(); i++) {
                    Element onlineRes = (Element) onlines.item(i);

                    // Main URL
                    String link = safeIsoText(onlineRes, new String[][]{
                        {"gmd:linkage","gmd:URL"}
                    });
                    link = normalizeUrl(link);
                    if (link.isEmpty() || seenLinks.contains(link)) continue;

                    seenLinks.add(link);

                    String name = safeIsoText(onlineRes, new String[][]{
                        {"gmd:name","gco:CharacterString"}
                    });
                    String desc = safeIsoText(onlineRes, new String[][]{
                        {"gmd:description","gco:CharacterString"}
                    });
                    String protocol = safeIsoText(onlineRes, new String[][]{
                        {"gmd:protocol","gco:CharacterString"}
                    });
                    String funcCode = safeIsoText(onlineRes, new String[][]{
                        {"gmd:function","gmd:CI_OnLineFunctionCode"}
                    });

                    // Classify as download or access
                    boolean isDownload = isDownloadLink(funcCode, protocol, link);
                    String accessURL = isDownload ? "" : link;
                    String downloadURL = isDownload ? link : "";

                    // Estimate format and media type
                    String format = guessFormat(link, protocol, defaultFormat);
                    if (format == null || format.isEmpty()) format = "UNKNOWN";
                    String mediaType = guessMediaType(link, protocol);
                    if (mediaType == null) mediaType = "";

                    String titleDist = firstNonEmpty(
                        (name != null ? name.trim() : ""),
                        (desc != null ? desc.trim() : ""),
                        hostnameOf(link)
                    );
                    if (titleDist == null || titleDist.isEmpty()) titleDist = "resource";

                    // Crea distribution pulita e coerente
                    DcatDistribution dist = new DcatDistribution();
                    dist.setNodeId(nodeId);
                    dist.setAccessUrl(accessURL.isEmpty() ? null : accessURL);
                    dist.setDownloadUrl(downloadURL.isEmpty() ? null : downloadURL);
                    dist.setFormat(format);
                    dist.setMediaType(mediaType);
                    dist.setTitle(titleDist);
                    dist.setDescription(desc != null ? desc : "");
                    dist.setReleaseDate("1970-01-01T00:00:00Z");
                    dist.setUpdateDate("1970-01-01T00:00:00Z");


                    distributionList.add(dist);
                }
            }
        }




        // --- Publisher / contact ---
        FoafAgent publisher = new FoafAgent(
                DCTerms.publisher.getURI(),
                null,
                List.of(node.getPublisherName()),
                null, null, null,
                null,
                nodeId);

        java.util.List<VcardOrganization> contactPointList = new java.util.ArrayList<>();
        if (node.getPublisherEmail() != null && !node.getPublisherEmail().isEmpty()) {
            VcardOrganization contactOrg = new VcardOrganization(
                    DCAT.contactPoint.getURI(),
                    node.getHomepage() != null ? node.getHomepage() : "",
                    node.getPublisherName(),
                    node.getPublisherEmail(),
                    "", "", "",
                    nodeId);
            contactPointList.add(contactOrg);
        }

        // --- Themes (topicCategory) ---
        java.util.List<SkosConceptTheme> themeList = new java.util.ArrayList<>();
        java.util.List<SkosConceptSubject> subjectList = new java.util.ArrayList<>();
        org.w3c.dom.NodeList topicNodes = recordElem.getElementsByTagNameNS(GMD_NS, "topicCategory");
        for (int i = 0; i < topicNodes.getLength(); i++) {
            Element t = (Element) topicNodes.item(i);
            String topic = firstNonEmpty(
                textNS(t, GMD_NS, "MD_TopicCategoryCode"),
                t.getTextContent()
            );
            if (topic != null) {
                topic = topic.trim();
                if (!topic.isEmpty()) {
                    java.util.List<it.eng.idra.beans.dcat.SkosPrefLabel> labels = new java.util.ArrayList<>();
                    labels.add(new it.eng.idra.beans.dcat.SkosPrefLabel("", topic, nodeId));
                    themeList.add(new SkosConceptTheme(DCAT.theme.getURI(), topic, labels, nodeId));
                }
            }
        }

     // --- Access rights ---
     // --- Access rights (short, safe for DB) ---
     String accessRights = extractAccessRightsShort(recordElem);

    // 2) Free text (may be long)
     String accessFreeText = firstNonEmpty(
         safeIsoText(recordElem, new String[][]{
             {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:resourceConstraints",
              "gmd:MD_LegalConstraints","gmd:useLimitation","gco:CharacterString"}
         }),
         safeIsoText(recordElem, new String[][]{
             {"gmd:identificationInfo","gmd:MD_DataIdentification","gmd:resourceConstraints",
              "gmd:MD_LegalConstraints","gmd:otherConstraints","gco:CharacterString"}
         })
     );

     // 3) Decide the short value for accessRights (max ~250 char)
     String accessRightsFinal = "";
     if (accessRights != null && !accessRights.trim().isEmpty()) {
         // map some common values to compact strings (adjust as needed)
         String c = accessRights.trim().toLowerCase();
         if (c.contains("copyright"))            accessRightsFinal = "copyright";
         else if (c.contains("license"))         accessRightsFinal = "license";
         else if (c.contains("intellectual"))    accessRightsFinal = "intellectualPropertyRights";
         else if (c.contains("other"))           accessRightsFinal = "otherRestrictions";
         else if (c.contains("restricted"))      accessRightsFinal = "restricted";
         else if (c.contains("private"))         accessRightsFinal = "nonPublic";
         else                                     accessRightsFinal = c; // già corto
     } else if (accessFreeText != null && !accessFreeText.trim().isEmpty()) {
         accessRightsFinal = truncate(accessFreeText.trim(), 250); // evita overflow DB
     } else {
         accessRightsFinal = ""; // nessuna info
     }

     // 4) If the free text was long, save it in documentation (not in accessRights!)
     java.util.List<String> documentation = new java.util.ArrayList<>();
     if (accessFreeText != null && accessFreeText.trim().length() > 250) {
         documentation.add(accessFreeText.trim());
     }


        // --- Assemble dataset ---
        DcatDataset dataset = new DcatDataset(
                nodeId,
                identifier,
                (title == null || title.isEmpty()) ? identifier : title,
                description,
                distributionList,
                themeList,
                publisher,
                contactPointList,
                keywords,
                accessRightsFinal,
                new java.util.ArrayList<DctStandard>(),   // conformsTo
                documentation,                            // documentation
                null,                                     // frequency
                new java.util.ArrayList<String>(),        // hasVersion
                new java.util.ArrayList<String>(),        // isVersionOf
                landing,             			          // landing page
                new java.util.ArrayList<String>(),        // languages
                new java.util.ArrayList<String>(),        // provenance
                (releaseDate == null || releaseDate.isEmpty()) ? "1970-01-01T00:00:00Z" : releaseDate,
                (updateDate  == null || updateDate.isEmpty())  ? "1970-01-01T00:00:00Z" : updateDate,
                new java.util.ArrayList<String>(),        // otherIdentifier
                new java.util.ArrayList<String>(),        // sample
                new java.util.ArrayList<String>(),        // source
                (spatialCoverage == null ? new ArrayList<>() : List.of(spatialCoverage)),
                (temporalCoverage == null ? new ArrayList<>() : List.of(temporalCoverage)),
                "", "",                                   // type, version
                new java.util.ArrayList<String>(),        // versionNotes
                null, null,                               // rightsHolder, creator
                subjectList,
                new java.util.ArrayList<String>(),        // relatedResources,
                new java.util.ArrayList<String>(),  //ApplacableLegislation
                null,  //inSeries
                null, //qualifiedRelation
                "", //temporalResolution
                new java.util.ArrayList<String>(), //wasGeneratedBy
                new java.util.ArrayList<String>() //HVDCategory
        );
        return dataset;
    }

    /* =====================  HELPERS   ===================== */

    private String firstNonEmpty(String... vals) {
        return GeoNetworkConnectorUtils.firstNonEmpty(vals);
    }

    private String textNS(Element parent, String ns, String local) {
        return GeoNetworkConnectorUtils.textNS(parent, ns, local);
    }
    
    private String truncate(String s, int max) {
        return GeoNetworkConnectorUtils.truncate(s, max);
    }


    /**
     * Follows a path of elements (with prefixes gmd/gco/gml) and returns the text found.
     * Tries multiple paths as fallback. Uses the official ISO/GML namespaces.
     */
    private String safeIsoText(Element root, String[][] pathOptions) {
        return GeoNetworkConnectorUtils.safeIsoText(root, pathOptions);
    }
    
    private String sha1OfElement(org.w3c.dom.Element el) {
        return GeoNetworkConnectorUtils.sha1OfElement(el);
    }
    
    private String normalizeUrl(String url) {
        return GeoNetworkConnectorUtils.normalizeUrl(url);
    }
    
    /** Heuristics to determine if this is a real download link */
    private boolean isDownloadLink(String funcCode, String protocol, String url) {
        return GeoNetworkConnectorUtils.isDownloadLink(funcCode, protocol, url);
    }

    /** Guess the format from URL/protocol/defaultFormat */
    private String guessFormat(String url, String protocol, String defaultFmt) {
        return GeoNetworkConnectorUtils.guessFormat(url, protocol, defaultFmt);
    }

    /** Guess (when possible) a sensible media-type */
    private String guessMediaType(String url, String protocol) {
        return GeoNetworkConnectorUtils.guessMediaType(url, protocol);
    }

    /** Hostname of a URL for fallback title */
    private String hostnameOf(String url) {
        return GeoNetworkConnectorUtils.hostnameOf(url);
    }
    
 // Ritorna una stringa corta (<=255) per accessRights.
 // Regole:
 // - se troviamo "noLimitations" (INSPIRE) => "PUBLIC"
 // - se troviamo la "Modellicentie gratis hergebruik" => "OPEN"
 // - se MD_RestrictionCode=restricted => "RESTRICTED"
 // - se MD_RestrictionCode=license => "LICENSED"
 // - altrimenti "OTHER"
 // NEVER copy long paragraphs from otherConstraints.
 private String extractAccessRightsShort(org.w3c.dom.Element recordElem) {
     final String GMD_NS = "http://www.isotc211.org/2005/gmd";
     final String GMX_NS = "http://www.isotc211.org/2005/gmx";
     final String GCO_NS = "http://www.isotc211.org/2005/gco";

     // 1) accessConstraints -> MD_RestrictionCode (preferisci un valore corto e normalizzato)
     String restr = readRestrictionCode(recordElem);
     if (!restr.isEmpty()) {
         String norm = normalizeRestriction(restr);
         if (!norm.isEmpty()) return norm;
     }

    // 2) otherConstraints -> try to detect cases like "noLimitations" or "Modellicentie..." (still short)
     //   Cerco prima Anchor (spesso hanno URI INSPIRE o della licenza)
     org.w3c.dom.NodeList legalList = recordElem.getElementsByTagNameNS(GMD_NS, "MD_LegalConstraints");
     for (int i = 0; i < legalList.getLength(); i++) {
         org.w3c.dom.Element legal = (org.w3c.dom.Element) legalList.item(i);

         // Anchor
         org.w3c.dom.NodeList anchors = legal.getElementsByTagNameNS(GMX_NS, "Anchor");
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
             // also try the href of the Anchor (if present)
             if (anchors.item(j) instanceof org.w3c.dom.Element) {
                 org.w3c.dom.Element a = (org.w3c.dom.Element) anchors.item(j);
                 String href = a.getAttributeNS("http://www.w3.org/1999/xlink", "href");
                 if (href != null) {
                     String h = href.toLowerCase();
                     if (h.contains("limitationsonpublicaccess/noLimitations".toLowerCase())) return "PUBLIC";
                     if (h.contains("modellicentie-gratis-hergebruik")) return "OPEN";
                 }
             }
         }

         // CharacterString (non copiare il paragrafo! usa una label corta riconoscibile)
         org.w3c.dom.NodeList strings = legal.getElementsByTagNameNS(GCO_NS, "CharacterString");
         for (int j = 0; j < strings.getLength(); j++) {
             String t = strings.item(j).getTextContent();
             if (t == null) continue;
             String low = t.trim().toLowerCase();
             if (low.contains("geen beperkingen") || low.contains("no limitations")) {
                 return "PUBLIC";
             }
             if (low.contains("modellicentie") || low.contains("gratis hergebruik")) {
                 return "OPEN";
             }
         }
     }

      // 3) fallback
      return GeoNetworkConnectorUtils.extractAccessRightsShort(recordElem);
 }

 // Reads the first gmd:MD_RestrictionCode (accessConstraints) as text or codeListValue
 private String readRestrictionCode(org.w3c.dom.Element recordElem) {
     final String GMD_NS = "http://www.isotc211.org/2005/gmd";
     final String GCO_NS = "http://www.isotc211.org/2005/gco";

     org.w3c.dom.NodeList accs = recordElem.getElementsByTagNameNS(GMD_NS, "accessConstraints");
     for (int i = 0; i < accs.getLength(); i++) {
         org.w3c.dom.Element acc = (org.w3c.dom.Element) accs.item(i);
         org.w3c.dom.NodeList codes = acc.getElementsByTagNameNS(GMD_NS, "MD_RestrictionCode");
         if (codes.getLength() > 0) {
             org.w3c.dom.Element code = (org.w3c.dom.Element) codes.item(0);
             // try the attribute codeListValue
             String val = code.getAttribute("codeListValue");
             if (val != null && !val.trim().isEmpty()) return val.trim();
             // fallback: testo interno (raro)
             String txt = code.getTextContent();
             if (txt != null && !txt.trim().isEmpty()) return txt.trim();
         }
     }

    // some profiles put a CharacterString instead of the code
     org.w3c.dom.NodeList legalList = recordElem.getElementsByTagNameNS(GMD_NS, "MD_LegalConstraints");
     for (int i = 0; i < legalList.getLength(); i++) {
         org.w3c.dom.Element legal = (org.w3c.dom.Element) legalList.item(i);
         org.w3c.dom.NodeList strings = legal.getElementsByTagNameNS(GCO_NS, "CharacterString");
         for (int j = 0; j < strings.getLength(); j++) {
             String t = strings.item(j).getTextContent();
             if (t != null && !t.trim().isEmpty()) {
                 // se somiglia a un valore corto noto, restituiscilo
                 String low = t.trim().toLowerCase();
                 if (low.equals("restricted") || low.equals("license") || low.equals("otherrestrictions")) {
                     return t.trim();
                 }
             }
         }
     }
     return "";
 }

 // Normalize possible values of the restriction code into short labels
 private String normalizeRestriction(String val) {
     String x = val.trim().toLowerCase();
     if (x.contains("restricted")) return "RESTRICTED";
     if (x.contains("license"))    return "LICENSED";
     if (x.contains("other"))      return "OTHER";
     if (x.contains("no limitations") || x.contains("geen beperkingen")) return "PUBLIC";
     return "";
 }





    /**
     * Fetches a specific dataset by ID from the CSW node (using GetRecordById).
     *
     * @param datasetId the identifier of the dataset (as in ISO fileIdentifier)
     * @return DcatDataset corresponding to the ID, or null if not found
     * @throws OdmsCatalogueOfflineException   if the node is unreachable
     * @throws OdmsCatalogueNotFoundException  if the dataset or host is not found
     * @throws OdmsCatalogueForbiddenException if access is forbidden
     * @throws Exception for general errors
     */
    @Override
    public DcatDataset getDataset(String datasetId) throws Exception {
        logger.info("CswConnector - getDataset - nodeId " + node.getId() + ", datasetId: " + datasetId);
        // Build CSW GetRecordById request URL:contentReference[oaicite:4]{index=4}
        String cswUrl = node.getHost()
                + (node.getHost().contains("?") ? "&" : "?")
                + "service=CSW&request=GetRecordById&version=2.0.2"
                + "&outputSchema=http://www.isotc211.org/2005/gmd&typeNames=gmd:MD_Metadata&elementSetName=full"
                + "&Id=" + datasetId;
        cswUrl += "&namespace=xmlns(gmd,http://www.isotc211.org/2005/gmd),xmlns(csw,http://www.opengis.net/cat/csw/2.0.2)";
;
        logger.info("CswConnector - getDataset - URL: " + cswUrl);
        try {
            // Open connection and parse single record
        	logger.info("CSW request (before redirects): {}", cswUrl);

        	HttpURLConnection conn;
        	try {
        	    conn = openFollowingRedirects(cswUrl);
        	} catch (Exception e) {
        	    String msg = e.getMessage() == null ? "" : e.getMessage();
        	    if (msg.contains("HTTP 404")) throw new OdmsCatalogueNotFoundException("The ODMS host does not exist (HTTP 404)");
        	    if (msg.contains("HTTP 403")) throw new OdmsCatalogueForbiddenException("The ODMS node is forbidden (HTTP 403)");
        	    // Redirect non risolto / HTML inatteso / ecc. → consideralo unreachable
        	    throw new OdmsCatalogueOfflineException("The ODMS node is currently unreachable: " + msg);
        	}

        	logger.info("CSW final URL (after redirects): {}", conn.getURL());
        	logger.info("CSW Content-Type: {}", conn.getContentType());

        	// Parse XML come prima
        	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        	factory.setNamespaceAware(true);
        	DocumentBuilder builder = factory.newDocumentBuilder();
        	Document doc = builder.parse(conn.getInputStream());
        	conn.disconnect();

            // Get the metadata record element
            NodeList recordNodes = doc.getElementsByTagName("gmd:MD_Metadata");
            if (recordNodes.getLength() > 0) {
                Element recordElem = (Element) recordNodes.item(0);
                DcatDataset dataset = datasetToDcat(recordElem, node);
                logger.info("CswConnector - getDataset - Dataset title: " + dataset.getTitle().getValue());
                return dataset;
            } else {
                logger.info("CswConnector - getDataset - No record found for ID: " + datasetId);
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            handleError(e);
            return null;
        }
    }

    /**
     * Searches datasets in the CSW node based on given parameters.
     * Supports free-text search via "q" parameter (on AnyText), and optional pagination ("page" & "size").
     *
     * @param searchParameters map of search parameters (e.g., "q" for query string, "page", "size")
     * @return List<DcatDataset> list of datasets matching the query
     * @throws OdmsCatalogueOfflineException, OdmsCatalogueNotFoundException, OdmsCatalogueForbiddenException, Exception
     */
    @Override
    public List<DcatDataset> findDatasets(HashMap<String, Object> searchParameters) throws Exception {
        logger.info("CswConnector - findDatasets - parameters: " + searchParameters);
        // If no search parameters, return all datasets
        if (searchParameters == null || searchParameters.isEmpty()) {
            return getAllDatasets();
        }
        // Build CSW GetRecords query with constraints if "q" (full-text search) is provided
        String query = "";
        if (searchParameters.containsKey("q")) {
            String q = searchParameters.get("q").toString().trim();
            if (!q.isEmpty()) {
                // Use CQL text constraint on AnyText (full-text search):contentReference[oaicite:5]{index=5}
                query = "&constraintLanguage=CQL_TEXT&constraint_language_version=1.1.0"
                        + "&constraint=AnyText Like '%" + q.replace("'", "''") + "%'";
            }
        }
        // Handle pagination if provided
        int start = 1;
        int max = 100;
        if (searchParameters.containsKey("page") && searchParameters.containsKey("size")) {
            try {
                int page = (Integer) searchParameters.get("page");
                int size = (Integer) searchParameters.get("size");
                if (page < 1) page = 1;
                if (size > 0) {
                    start = ((page - 1) * size) + 1;
                    max = size;
                }
            } catch (Exception e) {
                // ignore if parsing page/size fails, use defaults
            }
        }
        ArrayList<DcatDataset> results = new ArrayList<>();
        // Construct URL for GetRecords with search constraint
        String cswUrl = node.getHost()
                + (node.getHost().contains("?") ? "&" : "?")
                + "service=CSW&request=GetRecords&version=2.0.2"
                + "&resultType=results&outputSchema=http://www.isotc211.org/2005/gmd"
                + "&typeNames=gmd:MD_Metadata&elementSetName=full"
                + "&startPosition=" + start + "&maxRecords=" + max
                + "&namespace=xmlns(gmd,http://www.isotc211.org/2005/gmd),xmlns(csw,http://www.opengis.net/cat/csw/2.0.2)"
                + query;
        logger.info("CswConnector - findDatasets - CSW query URL: " + cswUrl);
        // Fetch and parse the results similar to getAllDatasets (single page)
        try {
        	logger.info("CSW request (before redirects): {}", cswUrl);

        	HttpURLConnection conn;
        	try {
        	    conn = openFollowingRedirects(cswUrl);
        	} catch (Exception e) {
        	    String msg = e.getMessage() == null ? "" : e.getMessage();
        	    if (msg.contains("HTTP 404")) throw new OdmsCatalogueNotFoundException("The ODMS host does not exist (HTTP 404)");
        	    if (msg.contains("HTTP 403")) throw new OdmsCatalogueForbiddenException("The ODMS node is forbidden (HTTP 403)");
        	    // Redirect non risolto / HTML inatteso / ecc. → consideralo unreachable
        	    throw new OdmsCatalogueOfflineException("The ODMS node is currently unreachable: " + msg);
        	}

        	logger.info("CSW final URL (after redirects): {}", conn.getURL());
        	logger.info("CSW Content-Type: {}", conn.getContentType());

        	// Parse XML come prima
        	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        	factory.setNamespaceAware(true);
        	DocumentBuilder builder = factory.newDocumentBuilder();
        	Document doc = builder.parse(conn.getInputStream());
        	conn.disconnect();

            final String GMD_NS = "http://www.isotc211.org/2005/gmd";

            NodeList recordNodes = doc.getElementsByTagNameNS(GMD_NS, "MD_Metadata");
            
            for (int i = 0; i < recordNodes.getLength(); i++) {
                Element recordElem = (Element) recordNodes.item(i);
                results.add(datasetToDcat(recordElem, node));
            }
        } catch (Exception e) {
            handleError(e);
        }
        logger.info("CswConnector - findDatasets - found " + results.size() + " datasets");
        return results;
    }

    /**
     * Counts datasets matching search parameters, without retrieving all details.
     * This performs a CSW GetRecords with resultType=hits to get the number of matched records.
     *
     * @param searchParameters the search parameters (same as findDatasets)
     * @return int count of datasets matching the search
     * @throws OdmsCatalogueOfflineException, OdmsCatalogueNotFoundException, OdmsCatalogueForbiddenException, Exception
     */
    @Override
    public int countSearchDatasets(HashMap<String, Object> searchParameters) throws Exception {
        logger.info("CswConnector - countSearchDatasets - parameters: " + searchParameters);
        // Build base URL similar to findDatasets but with resultType=hits for counting
        String query = "";
        if (searchParameters != null && searchParameters.containsKey("q")) {
            String q = searchParameters.get("q").toString().trim();
            if (!q.isEmpty()) {
                query = "&constraintLanguage=CQL_TEXT&constraint_language_version=1.1.0"
                        + "&constraint=AnyText Like '%" + q.replace("'", "''") + "%'";
            }
        }
        String cswUrl = node.getHost()
                + (node.getHost().contains("?") ? "&" : "?")
                + "service=CSW&request=GetRecords&version=2.0.2"
                + "&resultType=hits&outputSchema=http://www.isotc211.org/2005/gmd"
                + "&typeNames=gmd:MD_Metadata&elementSetName=full"
                + "&namespace=xmlns(gmd,http://www.isotc211.org/2005/gmd)"
                + query;
        logger.info("CswConnector - countSearchDatasets - CSW query URL: " + cswUrl);
        int matched = 0;
        try {
            URL url = new URL(cswUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(30000);
            conn.setRequestMethod("GET");
            conn.connect();
            int code = conn.getResponseCode();
            if (code == 404) {
                throw new OdmsCatalogueNotFoundException("The ODMS host does not exist (HTTP 404)");
            } else if (code == 403) {
                throw new OdmsCatalogueForbiddenException("The ODMS node is forbidden (HTTP 403)");
            } else if (code != 200) {
                throw new OdmsCatalogueOfflineException("The ODMS node is currently unreachable (HTTP " + code + ")");
            }
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(conn.getInputStream());
            conn.disconnect();
            
            final String CSW_NS = "http://www.opengis.net/cat/csw/2.0.2";

            Element resultsElem = (Element) doc.getElementsByTagNameNS(CSW_NS, "SearchResults").item(0);
            
            if (resultsElem != null) {
                String matchedStr = resultsElem.getAttribute("numberOfRecordsMatched");
                if (matchedStr != null && !matchedStr.isEmpty()) {
                    matched = Integer.parseInt(matchedStr);
                }
            }
        } catch (Exception e) {
            handleError(e);
        }
        logger.info("CswConnector - countSearchDatasets - matched count: " + matched);
        return matched;
    }

    /**
     * Retrieves all recent changes on datasets of the node compared to a previous list.
     * Compares two sets of DcatDataset (old vs new) to identify added, removed, and changed datasets,
     * ignoring datasets unchanged. Uses updateDate to detect modifications after a given starting date.
     *
     * @param oldDatasets  the previously known datasets
     * @param startingDate the starting date (ISO string) to consider changes (if applicable)
     * @return OdmsSynchronizationResult containing lists of added, deleted, and changed datasets
     * @throws Exception in case of errors during retrieval or comparison
     */
    @Override
    public OdmsSynchronizationResult getChangedDatasets(List<DcatDataset> oldDatasets, String startingDate) throws Exception {
        // Fetch current list of datasets
        List<DcatDataset> newDatasets = getAllDatasets();
        OdmsSynchronizationResult synchroResult = new OdmsSynchronizationResult();
        // Use set operations to find added, removed, and potentially updated datasets:contentReference[oaicite:6]{index=6}
        ImmutableSet<DcatDataset> newSet = ImmutableSet.copyOf(newDatasets);
        ImmutableSet<DcatDataset> oldSet = ImmutableSet.copyOf(oldDatasets);

        // Identify added datasets
        SetView<DcatDataset> addedDiff = Sets.difference(newSet, oldSet);
        logger.info("New datasets: " + addedDiff.size());
        for (DcatDataset d : addedDiff) {
            synchroResult.addToAddedList(d);
        }

        // Identify removed datasets
        SetView<DcatDataset> removedDiff = Sets.difference(oldSet, newSet);
        logger.info("Removed datasets: " + removedDiff.size());
        for (DcatDataset d : removedDiff) {
            synchroResult.addToDeletedList(d);
        }

        // Identify potentially changed datasets (present in both sets)
        SetView<DcatDataset> intersection = Sets.intersection(newSet, oldSet);
        logger.info("Datasets present in both (to check for changes): " + intersection.size());
        // Compare update dates for intersection to detect changes:contentReference[oaicite:7]{index=7}
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        GregorianCalendar oldCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        GregorianCalendar newCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        int exceptions = 0;
        for (DcatDataset d : intersection) {
            try {
                int oldIndex = oldDatasets.indexOf(d);
                int newIndex = newDatasets.indexOf(d);
                String oldDateStr = oldDatasets.get(oldIndex).getUpdateDate().getValue();
                String newDateStr = newDatasets.get(newIndex).getUpdateDate().getValue();
                oldCal.setTime(isoFormat.parse(oldDateStr));
                newCal.setTime(isoFormat.parse(newDateStr));
                if (newCal.after(oldCal)) {
                    synchroResult.addToChangedList(d);
                }
            } catch (Exception ex) {
                exceptions++;
                if (exceptions % 100 == 0) {
                    ex.printStackTrace();
                }
            }
        }
        logger.info("Changed datasets: " + synchroResult.getChangedDatasets().size());
        logger.info("Added datasets: " + synchroResult.getAddedDatasets().size());
        logger.info("Deleted datasets: " + synchroResult.getDeletedDatasets().size());
        return synchroResult;
    }

    /**
     * Utility method to safely retrieve text content of a sub-element by tag name.
     */
   
    private HttpURLConnection openFollowingRedirects(String initialUrl) throws Exception {
        return GeoNetworkConnectorUtils.openFollowingRedirects(initialUrl);
    }
    
 // --- DATE: portiamo tutto a YYYY-MM-DDThh:mm:ssZ per Solr ---
    private String ensureDateTimeZ(String in) {
        return GeoNetworkConnectorUtils.ensureDateTimeZ(in);
    }




    /**
     * Handles exceptions by mapping them to specific ODMS exceptions or rethrowing as generic Exception.
     *
     * @param e the exception to handle
     * @throws OdmsCatalogueNotFoundException, OdmsCatalogueForbiddenException, OdmsCatalogueOfflineException, Exception
     */
    private void handleError(Exception e) throws OdmsCatalogueNotFoundException,
    OdmsCatalogueForbiddenException, OdmsCatalogueOfflineException, Exception {
		String msg = e.getMessage();
		if (msg == null) msg = "";
		if (msg.contains("HTTP 404")) {
		    throw new OdmsCatalogueNotFoundException(msg);
		} else if (msg.contains("HTTP 403")) {
		    throw new OdmsCatalogueForbiddenException(msg);
		} else if (msg.matches(".*HTTP 3\\d\\d.*")) {
		    // Se ancora emergono 3xx qui, è un redirect non risolto dall'helper
		    throw new Exception("CSW redirect not resolved: " + msg, e);
		} else if (msg.contains("timed out") || msg.contains("unreachable")) {
		    throw new OdmsCatalogueOfflineException(msg);
		} else {
		    throw new Exception(msg, e);
		}
		}
    
    

}