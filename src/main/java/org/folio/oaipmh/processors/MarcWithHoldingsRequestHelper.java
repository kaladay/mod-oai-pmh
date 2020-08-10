package org.folio.oaipmh.processors;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.helpers.streaming.StreamingHelper;
import org.folio.rest.persist.PostgresClient;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.OAIPMHerrorType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


public class MarcWithHoldingsRequestHelper extends StreamingHelper {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public static final MarcWithHoldingsRequestHelper INSTANCE = new MarcWithHoldingsRequestHelper();

  private static final String INSTANCE_UPDATED_DATE_FIELD_NAME = "updateddate";

  /**
   * The dates returned by inventory storage service are in format "2018-09-19T02:52:08.873+0000".
   * Using {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME} and just in case 2 offsets "+HHmm" and "+HH:MM"
   */
  private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .optionalStart().appendOffset("+HH:MM", "Z").optionalEnd()
    .optionalStart().appendOffset("+HHmm", "Z").optionalEnd()
    .toFormatter();

  public static MarcWithHoldingsRequestHelper getInstance() {
    return INSTANCE;
  }

  /**
   * Handle MarcWithHoldings request
   */
  @Override
  public Future<Response> handle(Request request, Context vertxContext) {
    Promise<Response> promise = Promise.promise();
    try {
      PostgresClient postgresClient = PostgresClient.getInstance(vertxContext.owner(), request.getTenant());
      String resumptionToken = request.getResumptionToken();
      final boolean deletedRecordSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      List<OAIPMHerrorType> errors = validateListRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }

      String requestId =
        (resumptionToken == null || request.getRequestId() == null) ? UUID
          .randomUUID().toString() : request.getRequestId();

      handleStreamingResponse(request, vertxContext, promise, postgresClient, resumptionToken, deletedRecordSupport, requestId);

    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  @Override
  protected HeaderType createHeader(JsonObject instance) {
    String updateddate = instance.getString(INSTANCE_UPDATED_DATE_FIELD_NAME);
    Instant datetime = formatter.parse(updateddate, Instant::from)
      .truncatedTo(ChronoUnit.SECONDS);

    return new HeaderType()
      .withDatestamp(datetime)
      .withSetSpecs("all");
  }
}
