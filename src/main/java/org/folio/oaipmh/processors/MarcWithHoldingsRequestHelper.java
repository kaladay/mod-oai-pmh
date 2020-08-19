package org.folio.oaipmh.processors;

import static org.folio.oaipmh.Constants.formatter;

import java.time.Instant;
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

  private static final String INSTANCE_UPDATED_DATE_FIELD_NAME = "updateddate";

  public static MarcWithHoldingsRequestHelper getInstance() {
    return new MarcWithHoldingsRequestHelper();
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
