package it.eng.idra.connectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import it.eng.idra.beans.dcat.DcatDataset;
import it.eng.idra.beans.dcat.DcatDistribution;
import it.eng.idra.beans.dcat.DctLocation;
import it.eng.idra.beans.dcat.DctPeriodOfTime;
import it.eng.idra.beans.odms.OdmsCatalogue;
import it.eng.idra.beans.odms.OdmsSynchronizationResult;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CopernicusConnector implements IodmsConnector {

  private static final Logger logger = LogManager.getLogger(CopernicusConnector.class);
  private static final int COLLECTION_PAGE_LIMIT = 1000;
  private static final int PAGE_LIMIT = 200;
  private static final int MAX_DISTRIBUTIONS = 100;
  private static final int HTTP_MAX_ATTEMPTS = 4;
  private static final long HTTP_RETRY_BASE_DELAY_MILLIS = 10000L;
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(120);
  private static final String SAFE_BBOX = "NO_BBOX";
  private static final String SAFE_DATETIME = "UNKNOWN_DATETIME";
  private static final Type MAP_TYPE = new TypeToken<HashMap<String, Object>>() { }.getType();
  private static final Type MAP_LIST_TYPE = new TypeToken<List<HashMap<String, Object>>>() { }.getType();

  private final OdmsCatalogue node;
  private final String endpoint;
  private final HttpClient httpClient;
  private final Gson gson;

  private enum SourceType { STAC, OPENEO }

  private static final class SearchContext {
    private final String collection;
    private final String bbox;
    private final String startDate;
    private final String endDate;
    private final String datetime;
    private final SourceType sourceType;
    private SearchContext(String collection, String bbox, String startDate, String endDate,
        String datetime, SourceType sourceType) {
      this.collection = collection;
      this.bbox = bbox;
      this.startDate = startDate;
      this.endDate = endDate;
      this.datetime = datetime;
      this.sourceType = sourceType;
    }
  }

  private static final class TemporalFilter {
    private final String startDate;
    private final String endDate;
    private final String datetime;
    private TemporalFilter(String startDate, String endDate, String datetime) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.datetime = datetime;
    }
  }

  private static final class CollectionInfo {
    private final String id;
    private final String title;
    private final String description;
    private final String landingPage;
    private CollectionInfo(String id, String title, String description, String landingPage) {
      this.id = id;
      this.title = title;
      this.description = description;
      this.landingPage = landingPage;
    }
  }

  private static final class StacCollectionsPage {
    private final String nextPageUrl;
    private final List<CollectionInfo> collections;
    private StacCollectionsPage(String nextPageUrl, List<CollectionInfo> collections) {
      this.nextPageUrl = nextPageUrl;
      this.collections = collections;
    }
  }

  private static final class StacSearchPage {
    private final String nextPageUrl;
    private final List<HashMap<String, Object>> items;
    private StacSearchPage(String nextPageUrl, List<HashMap<String, Object>> items) {
      this.nextPageUrl = nextPageUrl;
      this.items = items;
    }
  }

  private static final class DatasetIdentityInput {
    private final String collection;
    private final String bbox;
    private final String start;
    private final String end;
    private final int chunkSequence;
    private DatasetIdentityInput(String collection, String bbox, String start, String end,
        int chunkSequence) {
      this.collection = collection;
      this.bbox = bbox;
      this.start = start;
      this.end = end;
      this.chunkSequence = chunkSequence;
    }
  }

  private static final class ParsedDatasetId {
    private final String originalId;
    private final String collection;
    private final String bbox;
    private ParsedDatasetId(String originalId, String collection, String bbox) {
      this.originalId = originalId;
      this.collection = collection;
      this.bbox = bbox;
    }
  }

  public CopernicusConnector(OdmsCatalogue node) {
    this.node = node;
    this.gson = new GsonBuilder().disableHtmlEscaping().create();
    String storedHost = node != null ? node.getHost() : null;
    String connectorParamsStacEndpoint = node != null
        ? extractStacEndpointFromConnectorParams(node.getConnectorParams()) : null;
    String fallbackEndpoint = resolveEffectiveEndpoint(node, storedHost);
    this.endpoint = !isBlank(connectorParamsStacEndpoint)
        ? normalizeBaseUrl(connectorParamsStacEndpoint) : fallbackEndpoint;
    this.httpClient = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    logger.info("Initialized CopernicusConnector from OdmsCatalogue id={} name={} storedHost={} "
        + "connectorParamsStacEndpoint={} effectiveStacEndpoint={}",
        node != null ? node.getId() : null, node != null ? node.getName() : null, storedHost,
        connectorParamsStacEndpoint, this.endpoint);
  }

  public CopernicusConnector(String endpoint) {
    this.node = null;
    this.endpoint = normalizeBaseUrl(endpoint);
    this.httpClient = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    this.gson = new GsonBuilder().disableHtmlEscaping().create();
    logger.info("Initialized CopernicusConnector from raw endpoint={}", this.endpoint);
  }

  @Override
  public List<DcatDataset> findDatasets(HashMap<String, Object> searchParameters) throws Exception {
    SearchContext ctx = buildSearchContext(searchParameters);
    validateSearchContext(ctx);
    logger.info("findDatasets start endpoint={} bbox={} datetime={}", endpoint, ctx.bbox,
        ctx.datetime);
    return harvestDatasets(ctx, null);
  }

  @Override
  public int countSearchDatasets(HashMap<String, Object> searchParameters) throws Exception {
    logger.info("countSearchDatasets invoked");
    int count = findDatasets(searchParameters).size();
    logger.info("countSearchDatasets result={}", count);
    return count;
  }

  @Override
  public int countDatasets() throws Exception {
    logger.info("countDatasets invoked");
    int count = getAllDatasets().size();
    logger.info("countDatasets result={}", count);
    return count;
  }

  @Override
  public DcatDataset datasetToDcat(Object dataset, OdmsCatalogue node) throws Exception {
    logger.debug("datasetToDcat invoked datasetType={} nodeId={}",
        dataset != null ? dataset.getClass().getName() : null, node != null ? node.getId() : null);
    return dataset instanceof DcatDataset ? (DcatDataset) dataset : new DcatDataset();
  }

  @Override
  public DcatDataset getDataset(String datasetId) throws Exception {
    logger.info("getDataset invoked datasetId={}", datasetId);
    ParsedDatasetId parsed = parseDatasetId(datasetId);
    if (parsed == null) {
      logger.warn("getDataset could not parse datasetId={}", datasetId);
      return null;
    }
    SearchContext ctx = new SearchContext(parsed.collection, parsed.bbox, null, null, null,
        SourceType.STAC);
    List<DcatDataset> datasets = harvestSingleCollection(ctx, fetchCollectionInfo(parsed.collection, ctx),
        parsed.originalId);
    logger.info("getDataset completed datasetId={} found={}", datasetId, !datasets.isEmpty());
    return datasets.isEmpty() ? null : datasets.get(0);
  }

  @Override
  public List<DcatDataset> getAllDatasets() throws Exception {
    logger.info("getAllDatasets invoked");
    SearchContext ctx = buildSearchContext(null);
    validateSearchContext(ctx);
    List<DcatDataset> datasets = harvestDatasets(ctx, null);
    logger.info("getAllDatasets completed datasetCount={}", datasets.size());
    return datasets;
  }

  @Override
  public OdmsSynchronizationResult getChangedDatasets(List<DcatDataset> oldDatasets,
      String startingDate) throws Exception {
    logger.info("getChangedDatasets invoked startingDate={} oldDatasetCount={}", startingDate,
        oldDatasets != null ? oldDatasets.size() : 0);
    List<DcatDataset> newDatasets = getAllDatasets();
    OdmsSynchronizationResult result = new OdmsSynchronizationResult();

    Set<DcatDataset> newSet = new HashSet<>(newDatasets);
    Set<DcatDataset> oldSet = new HashSet<>(oldDatasets != null
        ? oldDatasets : Collections.<DcatDataset>emptyList());

    for (DcatDataset dataset : newSet) {
      if (!oldSet.contains(dataset)) {
        result.addToAddedList(dataset);
      }
    }

    for (DcatDataset dataset : oldSet) {
      if (!newSet.contains(dataset)) {
        result.addToDeletedList(dataset);
      }
    }

    logger.info("getChangedDatasets completed added={} changed={} deleted={}",
        result.getAddedDatasets().size(), result.getChangedDatasets().size(),
        result.getDeletedDatasets().size());
    return result;
  }

  private SearchContext buildSearchContext(HashMap<String, Object> searchParameters) {
    logger.debug("buildSearchContext incoming searchParameters={}", searchParameters);
    Object rawBbox = extractBbox(searchParameters);
    String bboxSource = "searchParameters";

    if (rawBbox == null && node != null && !isBlank(node.getConnectorParams())) {
      logger.debug("buildSearchContext using connectorParams={}", node.getConnectorParams());
      rawBbox = extractBboxFromConnectorParams(node.getConnectorParams());
      bboxSource = "connectorParams";
    }

    logger.debug("buildSearchContext raw bbox source={} value={}", bboxSource, rawBbox);
    String normalizedBbox = normalizeBbox(rawBbox);
    logger.info("Resolved Copernicus bbox source={} value={}", bboxSource, normalizedBbox);

    logIgnoredCollectionFilters(searchParameters);
    TemporalFilter temporalFilter = resolveTemporalFilter(searchParameters);
    SearchContext ctx = new SearchContext("", normalizedBbox, temporalFilter.startDate,
        temporalFilter.endDate, temporalFilter.datetime, SourceType.STAC);
    logger.info("Built search context bbox={} datetime={} sourceType={}", ctx.bbox, ctx.datetime,
        ctx.sourceType);
    return ctx;
  }

  private void validateSearchContext(SearchContext ctx) {
    if (isBlank(endpoint)) {
      logger.error("Search context validation failed because endpoint is blank");
      throw new IllegalStateException("Copernicus endpoint is blank");
    }
    logger.debug("Search context validation succeeded for endpoint={} bbox={} datetime={}",
        endpoint, ctx.bbox, ctx.datetime);
  }

  private SourceType resolveSourceType(OdmsCatalogue node) {
    if (node != null && node.getConnectorParams() != null
        && node.getConnectorParams().toLowerCase(Locale.ROOT).contains("openeo")) {
      return SourceType.OPENEO;
    }
    return SourceType.STAC;
  }

  private String normalizeBaseUrl(String baseUrl) {
    if (baseUrl == null) {
      return "";
    }
    String normalized = baseUrl.trim();
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    if (normalized.startsWith("http://stac.dataspace.copernicus.eu")
        || normalized.startsWith("http://catalogue.dataspace.copernicus.eu")) {
      normalized = "https://" + normalized.substring("http://".length());
      logger.info("Normalized Copernicus endpoint to HTTPS: {}", normalized);
    }
    return normalized;
  }

  private String resolveEffectiveEndpoint(OdmsCatalogue node, String rawEndpoint) {
    String normalized = normalizeBaseUrl(rawEndpoint);
    if (isBlank(normalized)) {
      return normalized;
    }
    if (!isCopernicusNode(node, normalized)) {
      logger.debug("Keeping stored endpoint unchanged for non-COPERNICUS node endpoint={}",
          normalized);
      return normalized;
    }
    String effective = stripSyntheticCatalogueSuffix(normalized);
    if (normalized.equals(effective)) {
      logger.debug("Keeping COPERNICUS endpoint unchanged because no synthetic segment was found "
          + "endpoint={}", normalized);
      return normalized;
    }
    logger.info("Resolved COPERNICUS effective endpoint storedEndpoint={} effectiveEndpoint={}",
        normalized, effective);
    return effective;
  }

  private String extractStacEndpointFromConnectorParams(String connectorParams) {
    if (isBlank(connectorParams)) {
      return null;
    }
    try {
      HashMap<String, Object> payload = parseJsonObject(connectorParams);
      return stringValue(payload.get("stacEndpoint"));
    } catch (Exception ex) {
      logger.warn("Unable to parse stacEndpoint from connectorParams: {}", ex.getMessage());
      return null;
    }
  }

  private boolean isCopernicusNode(OdmsCatalogue node, String endpointValue) {
    if (node != null && node.getNodeType() != null
        && "COPERNICUS".equalsIgnoreCase(node.getNodeType().name())) {
      return true;
    }
    return endpointValue != null && endpointValue.contains(".dataspace.copernicus.eu/");
  }

  private String stripSyntheticCatalogueSuffix(String endpointValue) {
    try {
      URI uri = URI.create(endpointValue);
      String path = uri.getPath();
      if (isBlank(path)) {
        return endpointValue;
      }
      String[] rawSegments = path.split("/");
      List<String> segments = new ArrayList<>();
      for (String rawSegment : rawSegments) {
        if (!isBlank(rawSegment)) {
          segments.add(rawSegment);
        }
      }
      if (segments.size() < 2) {
        return endpointValue;
      }
      String lastSegment = segments.get(segments.size() - 1);
      if ("stac".equalsIgnoreCase(lastSegment) || "v1".equalsIgnoreCase(lastSegment)) {
        return endpointValue;
      }
      String prefix = endpointValue.substring(0, endpointValue.lastIndexOf('/'));
      return isBlank(prefix) ? endpointValue : prefix;
    } catch (IllegalArgumentException ex) {
      logger.warn("Unable to parse stored Copernicus endpoint for suffix stripping endpoint={} error={}",
          endpointValue, ex.getMessage());
      return endpointValue;
    }
  }

  private String buildCollectionsUrl(OdmsCatalogue node, SourceType sourceType) {
    logger.debug("Resolved collections URL base={} sourceType={}", endpoint, sourceType);
    return endpoint + "/collections";
  }

  private String buildCollectionByIdUrl(OdmsCatalogue node, SourceType sourceType,
      String collectionId) {
    String url = buildCollectionsUrl(node, sourceType) + "/" + encode(collectionId);
    logger.debug("Built collection detail URL for collectionId={} url={}", collectionId, url);
    return url;
  }

  private HttpRequest buildInitialStacSearchRequest(SearchContext ctx) {
    String url = buildInitialStacSearchUrl(ctx);
    logger.debug("Building initial STAC search request url={}", url);
    return HttpRequest.newBuilder().uri(URI.create(url))
        .timeout(HTTP_TIMEOUT).header("Accept", "application/json").GET().build();
  }

  private String buildInitialStacSearchUrl(SearchContext ctx) {
    List<String> query = new ArrayList<>();
    if (!SAFE_BBOX.equals(ctx.bbox)) {
      query.add("bbox=" + encode(ctx.bbox));
    }
    if (!isBlank(ctx.datetime)) {
      query.add("datetime=" + encode(ctx.datetime));
    }
    query.add("limit=" + PAGE_LIMIT);
    query.add("sortby=" + encode("properties.created"));
    String url = buildCollectionByIdUrl(null, ctx.sourceType, ctx.collection) + "/items?"
        + String.join("&", query);
    logger.info("Built STAC search URL collection={} bbox={} datetime={} "
        + "sortby=properties.created url={}", ctx.collection, ctx.bbox, ctx.datetime, url);
    return url;
  }

  private CollectionInfo fetchCollectionInfo(String collectionId, SearchContext ctx) {
    String landingPage = buildCollectionByIdUrl(null, ctx.sourceType, collectionId);
    logger.debug("Fetching collection info collectionId={} landingPage={}", collectionId, landingPage);
    try {
      HashMap<String, Object> payload = parseJsonObject(httpGet(landingPage));
      String title = firstNonBlank(stringValue(payload.get("title")), collectionId);
      String description = firstNonBlank(stringValue(payload.get("description")), "");
      String self = extractLinkHref(asListOfMaps(payload.get("links")), "self");
      CollectionInfo info =
          new CollectionInfo(collectionId, title, description, isBlank(self) ? landingPage : self);
      logger.debug("Fetched collection info collectionId={} title={} landingPage={}", collectionId,
          info.title, info.landingPage);
      return info;
    } catch (Exception ex) {
      logger.warn("Collection metadata fetch failed for {}: {}", collectionId, ex.getMessage());
      return new CollectionInfo(collectionId, collectionId, "", landingPage);
    }
  }

  private CollectionInfo fetchCollectionInfo(SearchContext ctx) {
    return fetchCollectionInfo(ctx.collection, ctx);
  }

  private String buildCollectionsListingUrl(SearchContext ctx) {
    String url = buildCollectionsUrl(null, ctx.sourceType) + "?limit=" + COLLECTION_PAGE_LIMIT;
    logger.debug("Built collections listing URL={}", url);
    return url;
  }

  private StacCollectionsPage fetchCollectionsPage(String requestUrl) throws Exception {
    logger.debug("Fetching collections page url={}", requestUrl);
    HashMap<String, Object> payload = parseJsonObject(httpGet(requestUrl));
    List<CollectionInfo> collections = collectCollectionsFromPage(payload);
    String nextPageUrl = extractNextPageUrl(payload);
    logger.debug("Fetched collections page url={} collectionCount={} next={}", requestUrl,
        collections.size(), nextPageUrl);
    return new StacCollectionsPage(nextPageUrl, collections);
  }

  private List<CollectionInfo> collectCollectionsFromPage(HashMap<String, Object> payload) {
    List<HashMap<String, Object>> rawCollections = asListOfMaps(payload.get("collections"));
    List<CollectionInfo> collections = new ArrayList<>();
    for (HashMap<String, Object> rawCollection : rawCollections) {
      String collectionId = stringValue(rawCollection.get("id"));
      if (isBlank(collectionId)) {
        continue;
      }
      String title = firstNonBlank(stringValue(rawCollection.get("title")), collectionId);
      String description = firstNonBlank(stringValue(rawCollection.get("description")), "");
      String landingPage = firstNonBlank(
          extractLinkHref(asListOfMaps(rawCollection.get("links")), "self"),
          buildCollectionByIdUrl(null, SourceType.STAC, collectionId));
      collections.add(new CollectionInfo(collectionId, title, description, landingPage));
      logger.debug("Discovered collection id={} title={} landingPage={}", collectionId, title,
          landingPage);
    }
    return collections;
  }

  private List<CollectionInfo> discoverCollections(SearchContext ctx) throws Exception {
    List<CollectionInfo> collections = new ArrayList<>();
    logger.info("Discovering all Copernicus collections from STAC /collections endpoint");
    StacCollectionsPage page = fetchCollectionsPage(buildCollectionsListingUrl(ctx));
    int pageIndex = 0;
    while (page != null) {
      logger.debug("Processing collections page index={} collectionCount={}", pageIndex,
          page.collections.size());
      collections.addAll(page.collections);
      if (isBlank(page.nextPageUrl)) {
        break;
      }
      page = fetchCollectionsPage(page.nextPageUrl);
      pageIndex++;
    }
    logger.info("Collection discovery completed discoveredCount={} mode=all-collections",
        collections.size());
    return collections;
  }

  private StacSearchPage fetchStacSearchPage(String requestUrl) throws Exception {
    logger.debug("Fetching STAC items page url={}", requestUrl);
    HashMap<String, Object> payload = parseJsonObject(httpGet(requestUrl));
    List<HashMap<String, Object>> items = collectItemsFromPage(payload);
    String nextPageUrl = extractNextPageUrl(payload);
    logger.debug("Fetched STAC items page url={} itemCount={} next={}", requestUrl, items.size(),
        nextPageUrl);
    return new StacSearchPage(nextPageUrl, items);
  }

  private String extractNextPageUrl(StacSearchPage page) {
    return page != null ? page.nextPageUrl : null;
  }

  private String extractNextPageUrl(HashMap<String, Object> payload) {
    String next = extractLinkHref(asListOfMaps(payload.get("links")), "next");
    if (isBlank(next)) {
      logger.debug("No next link found in payload");
      return null;
    }
    try {
      return URI.create(next).toString();
    } catch (IllegalArgumentException ex) {
      logger.warn("Ignoring malformed next link {}", next);
      return null;
    }
  }

  private List<HashMap<String, Object>> collectItemsFromPage(HashMap<String, Object> payload) {
    List<HashMap<String, Object>> items = asListOfMaps(payload.get("features"));
    logger.debug("Collected {} items from payload", items.size());
    return items;
  }

  private List<DcatDataset> harvestDatasets(SearchContext ctx, String targetDatasetId) throws Exception {
    List<DcatDataset> datasets = new ArrayList<>();
    List<CollectionInfo> collections = discoverCollections(ctx);
    logger.info("Harvesting datasets across {} collection(s) targetDatasetId={}", collections.size(),
        targetDatasetId);
    for (CollectionInfo collectionInfo : collections) {
      SearchContext collectionContext = new SearchContext(collectionInfo.id, ctx.bbox, ctx.startDate,
          ctx.endDate, ctx.datetime, ctx.sourceType);
      logger.info("Harvesting collection id={} title={}", collectionInfo.id, collectionInfo.title);
      List<DcatDataset> harvested = harvestSingleCollection(collectionContext, collectionInfo,
          targetDatasetId);
      if (targetDatasetId != null && !harvested.isEmpty()) {
        logger.info("Target dataset {} found while harvesting collection={}", targetDatasetId,
            collectionInfo.id);
        return harvested;
      }
      datasets.addAll(harvested);
    }
    logger.info("Harvest datasets completed totalDatasetCount={}", datasets.size());
    return targetDatasetId != null ? Collections.<DcatDataset>emptyList() : datasets;
  }

  private List<DcatDataset> harvestSingleCollection(SearchContext ctx, CollectionInfo info,
      String targetDatasetId) throws Exception {
    List<DcatDataset> datasets = new ArrayList<>();
    List<HashMap<String, Object>> buffer = new ArrayList<>();
    Map<String, Integer> seen = new HashMap<>();
    int chunkSequence = 0;
    StacSearchPage page = fetchStacSearchPage(buildInitialStacSearchRequest(ctx).uri().toString());
    int pageIndex = 0;

    while (page != null) {
      logger.debug("Harvesting item page index={} collection={} itemCount={}", pageIndex,
          ctx.collection, page.items.size());
      for (HashMap<String, Object> item : page.items) {
        buffer.add(item);
        if (buffer.size() >= MAX_DISTRIBUTIONS) {
          logger.debug("Buffer reached MAX_DISTRIBUTIONS={} for collection={} chunkSequence={}",
              MAX_DISTRIBUTIONS, ctx.collection, chunkSequence);
          DcatDataset dataset = flushBufferedChunk(buffer, ctx, info, chunkSequence, seen);
          if (dataset != null) {
            if (targetDatasetId != null && Objects.equals(targetDatasetId, dataset.getId())) {
              logger.info("Returning target dataset {} from chunkSequence={}", targetDatasetId,
                  chunkSequence);
              return Collections.singletonList(dataset);
            }
            datasets.add(dataset);
          }
          buffer.clear();
          chunkSequence++;
        }
      }
      String next = extractNextPageUrl(page);
      if (isBlank(next)) {
        break;
      }
      page = fetchStacSearchPage(next);
      pageIndex++;
    }

    DcatDataset trailing = flushBufferedChunk(buffer, ctx, info, chunkSequence, seen);
    if (trailing != null) {
      if (targetDatasetId != null && Objects.equals(targetDatasetId, trailing.getId())) {
        logger.info("Returning trailing target dataset {} from collection={}", targetDatasetId,
            ctx.collection);
        return Collections.singletonList(trailing);
      }
      datasets.add(trailing);
    }
    logger.info("Completed collection harvest collection={} datasetCount={}", ctx.collection,
        datasets.size());
    return targetDatasetId != null ? Collections.<DcatDataset>emptyList() : datasets;
  }

  private String normalizeBbox(Object bboxValue) {
    List<String> values = new ArrayList<>();
    if (bboxValue instanceof List<?>) {
      List<?> raw = (List<?>) bboxValue;
      for (int i = 0; i < raw.size() && i < 4; i++) {
        values.add(normalizeNumericToken(raw.get(i)));
      }
    } else if (bboxValue instanceof String) {
      String[] raw = ((String) bboxValue).trim().split(",");
      for (int i = 0; i < raw.length && i < 4; i++) {
        values.add(normalizeNumericToken(raw[i]));
      }
    }
    while (values.size() < 4) {
      values.add("");
    }
    String normalized = allBlank(values) ? SAFE_BBOX : String.join(",", values.subList(0, 4));
    logger.debug("Normalized bbox input={} output={}", bboxValue, normalized);
    return normalized;
  }

  private String resolveEffectiveDatetime(HashMap<String, Object> stacItem) {
    Map<String, Object> properties = getProperties(stacItem);
    String raw = firstNonBlank(stringValue(properties.get("datetime")),
        stringValue(properties.get("start_datetime")),
        stringValue(properties.get("end_datetime")));
    if (isBlank(raw)) {
      return null;
    }
    try {
      String normalized = Instant.parse(raw).toString();
      logger.debug("Resolved effective datetime itemId={} raw={} normalized={}",
          stringValue(stacItem.get("id")), raw, normalized);
      return normalized;
    } catch (DateTimeParseException ex) {
      logger.debug("Using non-ISO effective datetime itemId={} raw={}", stringValue(stacItem.get("id")),
          raw);
      return raw.trim();
    }
  }

  private DcatDataset flushBufferedChunk(List<HashMap<String, Object>> bufferedItems,
      SearchContext ctx, CollectionInfo info, int chunkSequence, Map<String, Integer> seen)
      throws Exception {
    if (bufferedItems == null || bufferedItems.isEmpty()) {
      logger.debug("Skipping chunk flush because buffer is empty");
      return null;
    }

    DatasetIdentityInput identity = buildDatasetIdentityInputs(ctx, bufferedItems, chunkSequence);
    String datasetId = buildDatasetIdentifier(identity, seen);
    String nodeId = buildNodeId();
    String datasetLandingPage = resolveDatasetLandingPage(info);
    logger.info("Flushing buffered chunk collection={} chunkSequence={} itemCount={} datasetId={}",
        ctx.collection, chunkSequence, bufferedItems.size(), datasetId);
    List<DcatDistribution> distributions = new ArrayList<>();
    for (int i = 0; i < bufferedItems.size(); i++) {
      distributions.add(mapStacItemToDistribution(bufferedItems.get(i), ctx, i, nodeId));
    }

    DcatDataset dataset = new DcatDataset(datasetId, nodeId, datasetId,
        buildDatasetTitle(identity, info),
        buildDatasetDescription(identity, info, bufferedItems.size()), distributions, null, null,
        null, null, null, null, null, null, null, null, datasetLandingPage, null, null,
        safeDatetimeValue(identity.start), safeDatetimeValue(identity.end), null, null, null,
        Collections.singletonList(buildSpatialCoverage(identity, nodeId)),
        Collections.singletonList(buildTemporalCoverage(identity, nodeId)), null, null, null, null,
        null, null, null, false, null, null, null, null, null, null);
    dataset.setNodeName(info.title);
    logger.debug("Created dataset id={} title={} distributionCount={}", dataset.getId(),
        buildDatasetTitle(identity, info), distributions.size());
    return dataset;
  }

  private DatasetIdentityInput buildDatasetIdentityInputs(SearchContext ctx,
      List<HashMap<String, Object>> bufferedItems, int chunkSequence) {
    String[] temporalBounds = resolveTemporalBounds(bufferedItems, ctx);
    DatasetIdentityInput identity = new DatasetIdentityInput(ctx.collection, ctx.bbox,
        temporalBounds[0], temporalBounds[1], chunkSequence);
    logger.debug("Built dataset identity inputs collection={} bbox={} start={} end={} chunkSequence={}",
        identity.collection, identity.bbox, identity.start, identity.end, identity.chunkSequence);
    return identity;
  }

  private String[] resolveTemporalBounds(List<HashMap<String, Object>> bufferedItems, SearchContext ctx) {
    String fallbackStart = firstNonBlank(resolveEffectiveDatetime(bufferedItems.get(0)),
        ctx != null ? ctx.startDate : null);
    String fallbackEnd = firstNonBlank(
        resolveEffectiveDatetime(bufferedItems.get(bufferedItems.size() - 1)),
        ctx != null ? ctx.endDate : null);
    Instant min = null;
    Instant max = null;

    for (HashMap<String, Object> item : bufferedItems) {
      String value = resolveEffectiveDatetime(item);
      if (isBlank(value)) {
        continue;
      }
      try {
        Instant parsed = Instant.parse(value);
        min = min == null || parsed.isBefore(min) ? parsed : min;
        max = max == null || parsed.isAfter(max) ? parsed : max;
      } catch (DateTimeParseException ex) {
        logger.debug("Skipping non-ISO datetime while resolving temporal bounds value={}", value);
      }
    }

    return new String[] {
        min != null ? min.toString() : fallbackStart,
        max != null ? max.toString() : fallbackEnd
    };
  }

  private String buildDatasetIdentifier(DatasetIdentityInput identity, Map<String, Integer> seen) {
    String base = "copernicus|" + identity.collection + "|" + identity.bbox + "|"
        + safeDatetimeIdentity(identity.start) + "|" + safeDatetimeIdentity(identity.end);
    int duplicateIndex = seen.containsKey(base) ? seen.get(base) + 1 : 0;
    seen.put(base, duplicateIndex);
    String identifier = duplicateIndex > 0 || identity.start == null || identity.end == null
        ? base + "|chunk=" + identity.chunkSequence : base;
    logger.debug("Built dataset identifier base={} duplicateIndex={} final={}", base, duplicateIndex,
        identifier);
    return identifier;
  }

  private String buildDatasetTitle(DatasetIdentityInput identity, CollectionInfo info) {
    String collectionLabel = firstNonBlank(info != null ? info.title : null, identity.collection);
    String catalogueLabel = resolveCatalogueTitle();
    return collectionLabel + " - " + catalogueLabel + " - " + safeDatetimeLabel(identity.start)
        + " / " + safeDatetimeLabel(identity.end);
  }

  private String resolveCatalogueTitle() {
    return firstNonBlank(node != null ? stringValue(node.getName()) : null, "Copernicus");
  }

  private String buildDatasetDescription(DatasetIdentityInput identity, CollectionInfo info,
      int itemCount) {
    String prefix = isBlank(info.description) ? "" : info.description.trim() + " ";
    return prefix + "Synthetic chunked dataset derived from STAC search results for collection "
        + info.id + ", bbox " + identity.bbox + ", items " + itemCount + ", temporal range "
        + safeDatetimeLabel(identity.start) + " / " + safeDatetimeLabel(identity.end) + ".";
  }

  private String resolveDatasetLandingPage(CollectionInfo info) {
    String homepage = node != null ? stringValue(node.getHomepage()) : null;
    String landingPage = firstNonBlank(homepage, info != null ? info.landingPage : null);
    logger.debug("Resolved dataset landing page source={} value={}",
        !isBlank(homepage) ? "catalogue-homepage" : "collection-landing-page", landingPage);
    return landingPage;
  }

  private DcatDistribution mapStacItemToDistribution(HashMap<String, Object> stacItem,
      SearchContext ctx, int itemIndexWithinChunk, String nodeId) {
    String itemId = firstNonBlank(stringValue(stacItem.get("id")), "item-" + itemIndexWithinChunk);
    String effectiveDatetime = resolveEffectiveDatetime(stacItem);
    String selfLink = extractLinkHref(asListOfMaps(stacItem.get("links")), "self");
    String accessUrl = firstNonBlank(selfLink, endpoint);

    DcatDistribution distribution = new DcatDistribution();
    distribution.setNodeId(nodeId);
    distribution.setIdentifier(itemId);
    distribution.setTitle(safeDatetimeLabel(effectiveDatetime) + " - " + itemId);
    distribution.setAccessUrl(accessUrl);
    distribution.setDescription("STAC item distribution for Copernicus collection "
        + ctx.collection + ".");
    distribution.setStoredRdf(false);
    distribution.setFormat("json");
    distribution.setMediaType("application/json");
    if (effectiveDatetime != null) {
      distribution.setReleaseDate(effectiveDatetime);
      distribution.setUpdateDate(effectiveDatetime);
    }
    logger.debug("Mapped STAC item to distribution itemId={} collection={} accessUrl={} "
        + "mediaType=application/json format=json additionalConfigGenerated=false",
        itemId, ctx.collection, accessUrl);
    return distribution;
  }

  private DctLocation buildSpatialCoverage(DatasetIdentityInput identity, String nodeId) {
    return new DctLocation(DCTerms.spatial.getURI(), identity.bbox, "Search bbox " + identity.bbox,
        "", nodeId, identity.bbox, "");
  }

  private DctPeriodOfTime buildTemporalCoverage(DatasetIdentityInput identity, String nodeId) {
    String start = safeDatetimeValue(identity.start);
    String end = safeDatetimeValue(identity.end);
    return new DctPeriodOfTime(DCTerms.temporal.getURI(), start, end, nodeId, start, end);
  }

  private String httpGet(String url) throws Exception {
    HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(HTTP_TIMEOUT)
        .header("Accept", "application/json").GET().build();
    Exception lastException = null;

    for (int attempt = 1; attempt <= HTTP_MAX_ATTEMPTS; attempt++) {
      long startTime = System.currentTimeMillis();
      logger.debug("HTTP GET start url={} attempt={}/{}", url, attempt, HTTP_MAX_ATTEMPTS);
      try {
        HttpResponse<String> response = httpClient.send(request,
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        long elapsed = System.currentTimeMillis() - startTime;
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          logger.debug("HTTP GET success url={} status={} elapsedMs={} attempt={}", url,
              response.statusCode(), elapsed, attempt);
          return response.body();
        }

        String message = "Request failed with status " + response.statusCode() + " for " + url
            + " body=" + previewBody(response.body());
        lastException = new IllegalStateException(message);
        if (!isRetryableStatus(response.statusCode()) || attempt == HTTP_MAX_ATTEMPTS) {
          logger.error("HTTP GET failed url={} status={} elapsedMs={} attempt={}/{} bodyPreview={}",
              url, response.statusCode(), elapsed, attempt, HTTP_MAX_ATTEMPTS,
              previewBody(response.body()));
          throw lastException;
        }

        logger.warn("HTTP GET retryable failure url={} status={} elapsedMs={} attempt={}/{} "
            + "bodyPreview={}", url, response.statusCode(), elapsed, attempt, HTTP_MAX_ATTEMPTS,
            previewBody(response.body()));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw ex;
      } catch (java.io.IOException ex) {
        long elapsed = System.currentTimeMillis() - startTime;
        lastException = ex;
        if (attempt == HTTP_MAX_ATTEMPTS) {
          logger.error("HTTP GET failed url={} elapsedMs={} attempt={}/{} error={}", url, elapsed,
              attempt, HTTP_MAX_ATTEMPTS, ex.toString());
          throw ex;
        }
        logger.warn("HTTP GET retryable exception url={} elapsedMs={} attempt={}/{} error={}", url,
            elapsed, attempt, HTTP_MAX_ATTEMPTS, ex.toString());
      }

      sleepBeforeRetry(attempt);
    }

    throw lastException;
  }

  private boolean isRetryableStatus(int statusCode) {
    return statusCode == 429 || statusCode == 500 || statusCode == 502 || statusCode == 503
        || statusCode == 504;
  }

  private void sleepBeforeRetry(int attempt) throws InterruptedException {
    long delay = HTTP_RETRY_BASE_DELAY_MILLIS * attempt;
    logger.info("Waiting {} ms before retrying Copernicus request", delay);
    Thread.sleep(delay);
  }

  private String previewBody(String body) {
    if (body == null) {
      return "";
    }
    return body.length() <= 200 ? body : body.substring(0, 200) + "...";
  }

  private Object extractBbox(HashMap<String, Object> searchParameters) {
    return searchParameters != null ? searchParameters.get("bbox") : null;
  }

  private Object extractBboxFromConnectorParams(String connectorParams) {
    if (isBlank(connectorParams)) {
      return null;
    }
    try {
      HashMap<String, Object> payload = parseJsonObject(connectorParams);
      return payload.get("bbox");
    } catch (Exception ex) {
      logger.warn("Unable to parse bbox from connectorParams: {}", ex.getMessage());
      return null;
    }
  }

  private TemporalFilter resolveTemporalFilter(HashMap<String, Object> searchParameters) {
    HashMap<String, Object> connectorParams = parseConnectorParams();
    String searchStart = stringValue(searchParameters != null ? searchParameters.get("startDate") : null);
    String searchEnd = stringValue(searchParameters != null ? searchParameters.get("endDate") : null);
    String searchDatetime =
        stringValue(searchParameters != null ? searchParameters.get("datetime") : null);
    String connectorStart = stringValue(connectorParams.get("startDate"));
    String connectorEnd = stringValue(connectorParams.get("endDate"));
    String connectorDatetime = stringValue(connectorParams.get("datetime"));

    String startDate = firstNonBlank(searchStart, connectorStart);
    String endDate = firstNonBlank(searchEnd, connectorEnd);
    String datetime = firstNonBlank(buildDatetimeFilter(startDate, endDate), searchDatetime,
        connectorDatetime);
    TemporalFilter temporalFilter = parseTemporalFilter(datetime, startDate, endDate);
    logger.info("Resolved Copernicus temporal filter startDate={} endDate={} datetime={} "
        + "searchOverridesConnector={}", temporalFilter.startDate, temporalFilter.endDate,
        temporalFilter.datetime, !isBlank(searchStart) || !isBlank(searchEnd) || !isBlank(searchDatetime));
    return temporalFilter;
  }

  private TemporalFilter parseTemporalFilter(String datetime, String fallbackStart, String fallbackEnd) {
    String normalizedDatetime = stringValue(datetime);
    if (!isBlank(normalizedDatetime) && normalizedDatetime.contains("/")) {
      String[] parts = normalizedDatetime.split("/", 2);
      String start = normalizeDatetimeBoundary(parts.length > 0 ? parts[0] : null);
      String end = normalizeDatetimeBoundary(parts.length > 1 ? parts[1] : null);
      return new TemporalFilter(start, end, normalizedDatetime);
    }
    return new TemporalFilter(stringValue(fallbackStart), stringValue(fallbackEnd),
        buildDatetimeFilter(fallbackStart, fallbackEnd));
  }

  private String normalizeDatetimeBoundary(String value) {
    String normalized = stringValue(value);
    return "..".equals(normalized) ? null : normalized;
  }

  private String buildDatetimeFilter(String startDate, String endDate) {
    String start = stringValue(startDate);
    String end = stringValue(endDate);
    if (isBlank(start) && isBlank(end)) {
      return null;
    }
    if (!isBlank(start) && !isBlank(end)) {
      return start + "/" + end;
    }
    if (!isBlank(start)) {
      return start + "/..";
    }
    return "../" + end;
  }

  private void logIgnoredCollectionFilters(HashMap<String, Object> searchParameters) {
    Object searchCollection = firstNonNull(searchParameters != null ? searchParameters.get("collection") : null,
        searchParameters != null ? searchParameters.get("collections") : null);
    HashMap<String, Object> connectorParams = parseConnectorParams();
    Object connectorCollection = firstNonNull(connectorParams.get("collection"),
        connectorParams.get("collections"));
    if (searchCollection != null || connectorCollection != null) {
      logger.info("Ignoring Copernicus collection filter inputs searchParameters={} connectorParams={}",
          searchCollection, connectorCollection);
    }
  }

  private HashMap<String, Object> parseConnectorParams() {
    if (node == null || isBlank(node.getConnectorParams())) {
      return new HashMap<String, Object>();
    }
    try {
      return parseJsonObject(node.getConnectorParams());
    } catch (Exception ex) {
      logger.warn("Unable to parse connectorParams: {}", ex.getMessage());
      return new HashMap<String, Object>();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getProperties(HashMap<String, Object> stacItem) {
    Object properties = stacItem != null ? stacItem.get("properties") : null;
    return properties instanceof Map<?, ?> ? (Map<String, Object>) properties : Collections.<String, Object>emptyMap();
  }

  private HashMap<String, Object> parseJsonObject(String body) {
    HashMap<String, Object> payload = gson.fromJson(body, MAP_TYPE);
    logger.debug("Parsed JSON object bodyLength={} keys={}", body != null ? body.length() : 0,
        payload != null ? payload.keySet() : Collections.emptySet());
    return payload != null ? payload : new HashMap<String, Object>();
  }

  private List<HashMap<String, Object>> asListOfMaps(Object value) {
    if (value == null) {
      return Collections.emptyList();
    }
    List<HashMap<String, Object>> out = gson.fromJson(gson.toJson(value), MAP_LIST_TYPE);
    return out != null ? out : Collections.<HashMap<String, Object>>emptyList();
  }

  private HashMap<String, Object> asMap(Object value) {
    if (value == null) {
      return new HashMap<String, Object>();
    }
    HashMap<String, Object> out = gson.fromJson(gson.toJson(value), MAP_TYPE);
    return out != null ? out : new HashMap<String, Object>();
  }

  private String extractLinkHref(List<HashMap<String, Object>> links, String rel) {
    for (HashMap<String, Object> link : links) {
      if (rel.equalsIgnoreCase(stringValue(link.get("rel")))) {
        return stringValue(link.get("href"));
      }
    }
    return null;
  }

  private ParsedDatasetId parseDatasetId(String datasetId) {
    if (isBlank(datasetId) || !datasetId.startsWith("copernicus|")) {
      logger.debug("Dataset id is not parseable by CopernicusConnector datasetId={}", datasetId);
      return null;
    }
    String[] parts = datasetId.split("\\|");
    if (parts.length < 5) {
      return null;
    }
    ParsedDatasetId parsed = new ParsedDatasetId(datasetId, decode(parts[1]), decode(parts[2]));
    logger.debug("Parsed dataset id={} collection={} bbox={}", datasetId, parsed.collection,
        parsed.bbox);
    return parsed;
  }

  private String stringValue(Object value) {
    if (value == null) {
      return null;
    }
    String out = String.valueOf(value).trim();
    return out.isEmpty() ? null : out;
  }

  private String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (!isBlank(value)) {
        return value;
      }
    }
    return null;
  }

  private Object firstNonNull(Object... values) {
    if (values == null) {
      return null;
    }
    for (Object value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static boolean isStaticBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private boolean allBlank(List<String> values) {
    for (String value : values) {
      if (!isBlank(value)) {
        return false;
      }
    }
    return true;
  }

  private String normalizeNumericToken(Object rawValue) {
    if (rawValue == null) {
      return "";
    }
    String raw = String.valueOf(rawValue).trim();
    if (raw.isEmpty()) {
      return "";
    }
    try {
      return new BigDecimal(raw).stripTrailingZeros().toPlainString();
    } catch (NumberFormatException ex) {
      return raw;
    }
  }

  private String encode(String value) {
    return URLEncoder.encode(value != null ? value : "", StandardCharsets.UTF_8);
  }

  private String decode(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private String safeDatetimeLabel(String value) {
    return value != null ? value : SAFE_DATETIME;
  }

  private String safeDatetimeIdentity(String value) {
    return value != null ? value : SAFE_DATETIME;
  }

  private String safeDatetimeValue(String value) {
    return value != null ? value : "";
  }

  private String buildNodeId() {
    String nodeId = node != null ? Integer.toString(node.getId())
        : "copernicus-" + Integer.toUnsignedString(endpoint.hashCode());
    logger.debug("Resolved connector nodeId={}", nodeId);
    return nodeId;
  }
}
