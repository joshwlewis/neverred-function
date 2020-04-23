import * as http from 'http';
import * as url from 'url';
import { BinaryHTTPReceiver, StructuredHTTPReceiver } from 'cloudevents-sdk/v1';
import { LoggerLevel } from '@salesforce/core';
import {
    ConnectionConfig,
    Constants,
    Context,
    DataApi,
    ErrorResult,
    InvocationEvent,
    Logger,
    Org,
    Secrets,
    SObject,
    SuccessResult,
    UnitOfWork,
    User,
} from '@salesforce/salesforce-sdk';

const { DEBUG } = process.env;

  /**
   * Start an HTTP server that invokes the target function with an event.
   *
   * @param target -- The function to handle an event.
   */
export function invoke(targetFn: Function): void {
  const server = http.createServer((req, res) => {
    const reqId = [].concat(req.headers['x-ce-id'], req.headers['x-request-id']).join(',');
    const logger = createLogger(reqId);
    const secrets = createSecrets(logger);
    let body = [];
    const path = url.parse(req.url).path;
    if (path !== '/' || req.method !== "POST" ) {
      res.writeHead(404);
      res.end();
      return
    }
    req.on('data', chunk => { body.push(chunk) });
    req.on('end', () => {
      try {
        const event = parseCloudevent(body.join(), req.headers);
        let data, accessToken, invocationId;
        if (event) {
          data = event.getData();
          if (data.sfContext) {
            accessToken = data.sfContext.accessToken;
            invocationId = data.sfContext.functionInvocationId;
          }
        }
        const context = createContext(reqId, logger, secrets, accessToken, invocationId)

        const result = targetFn(data, context, logger);
        if (result) { res.write(result); }
        res.end();
      } catch(error) {
        logger.error({error});
        res.writeHead(500);
        res.end();
      }
    })
  }).listen(8080);
};

function parseCloudevent(body, headers) {
  return parseBinaryCloudevent(body, headers) || parseStructuredCloudevent(body, headers);
}

function parseBinaryCloudevent(body, headers) {
  const binaryParser = new BinaryHTTPReceiver();
  try {
    binaryParser.check(body, headers)
  } catch {
    return
  }
  return binaryParser.parse(body, headers);
}

function parseStructuredCloudevent(body, headers) {
  const structuredParser = new StructuredHTTPReceiver();
  try {
    structuredParser.check(body, headers)
  } catch {
    return
  }
  return structuredParser.parse(body, headers);
}

function createLogger(requestID) {
  const level = DEBUG ? LoggerLevel.DEBUG : LoggerLevel.INFO;
  const logger = new Logger('Neverred Logger');
  logger.addStream({stream: process.stderr});
  logger.setLevel(level);

  if (requestID) {
    logger.addField('request_id', requestID);
  }

  return logger;
}

// TODO: Remove when FunctionInvocationRequest is deprecated.
class FunctionInvocationRequest {
  public response: any;
  public status: string;

  constructor(public readonly id: string,
              private readonly logger: Logger,
              private readonly dataApi?: DataApi) {
  }

  /**
   * Saves FunctionInvocationRequest
   *
   * @throws err if response not provided or on failed save
   */
  public async save(): Promise<any> {
      if (!this.response) {
          throw new Error('Response not provided');
      }

      if (this.dataApi) {
          const responseBase64 = Buffer.from(JSON.stringify(this.response)).toString('base64');

          try {
              // Prime pump (W-6841389)
              const soql = `SELECT Id, FunctionName, Status, CreatedById, CreatedDate FROM FunctionInvocationRequest WHERE Id ='${this.id}'`;
              await this.dataApi.query(soql);
          } catch (err) {
              this.logger.warn(err.message);
          }

          const fxInvocation = new SObject('FunctionInvocationRequest').withId(this.id);
          fxInvocation.setValue('ResponseBody', responseBase64);
          const result: SuccessResult | ErrorResult = await this.dataApi.update(fxInvocation);
          if (!result.success && 'errors' in result) {
              // Tells tsc that 'errors' exist and join below is okay
              const msg = `Failed to send response [${this.id}]: ${result.errors.join(',')}`;
              this.logger.error(msg);
              throw new Error(msg);
          } else {
              return result;
          }
      } else {
          throw new Error('Authorization not provided');
      }
  }
}

/**
 * Construct Event from invocation request.
 *
 * @param data    -- function payload
 * @param headers -- request headers
 * @param payload -- request payload
 * @return event
 */
function createEvent(data: any, headers: any, payload: any): InvocationEvent {
    return new InvocationEvent(
        data,
        payload.contentType,
        payload.schemaURL,
        payload.id,
        payload.source,
        payload.time,
        payload.type,
        headers
    );
}

/**
 * Construct User object from the request context.
 *
 * @param userContext -- userContext object representing invoking org and user
 * @return user
 */
function createUser(userContext: any): User {
    return new User(
        userContext.userId,
        userContext.username,
        userContext.onBehalfOfUserId
    );
}

/**
 * Construct Secrets object with logger.
 *
 *
 * @param logger -- logger to use in case of secret load errors
 * @return secrets loader/cache
 */
function createSecrets(logger: Logger): Secrets {
    return new Secrets(logger);
}

/**
 * Construct Org object from the request context.
 *
 * @param reqContext
 * @return org
 */
function createOrg(logger: Logger, reqContext: any, accessToken?: string): Org {
    const userContext = reqContext.userContext;
    if (!userContext) {
        const message = `UserContext not provided: ${JSON.stringify(reqContext)}`;
        throw new Error(message);
    }

    const apiVersion = reqContext.apiVersion || process.env.FX_API_VERSION || Constants.CURRENT_API_VERSION;
    const user = createUser(userContext);

    // If accessToken was provided, setup APIs.
    let dataApi: DataApi | undefined;
    let unitOfWork: UnitOfWork | undefined;
    if (accessToken) {
        const config: ConnectionConfig = new ConnectionConfig(
            accessToken,
            apiVersion,
            userContext.salesforceBaseUrl
        );
        unitOfWork = new UnitOfWork(config, logger);
        dataApi = new DataApi(config, logger);
    }

    return new Org(
        apiVersion,
        userContext.salesforceBaseUrl,
        userContext.orgDomainUrl,
        userContext.orgId,
        user,
        dataApi,
        unitOfWork
    );
}

/**
 * Construct Context from function payload.
 *
 * @param id                   -- request payload id
 * @param logger               -- logger
 * @param secrets              -- secrets convenience class
 * @param reqContext           -- reqContext from the request, contains salesforce stuff (user reqContext, etc)
 * @param accessToken          -- accessToken for function org access, if provided
 * @param functionInvocationId -- FunctionInvocationRequest ID, if applicable
 * @return context
 */
function createContext(id: string, logger: Logger, secrets: Secrets, reqContext?: any,
                       accessToken?: string, functionInvocationId?: string): Context {
    if (typeof reqContext === 'string') {
        reqContext = JSON.parse(reqContext);
    }

    const org = reqContext ? createOrg(logger, reqContext!, accessToken) : undefined;
    const context = new Context(id, logger, org, secrets);

    // If functionInvocationId is provided, create and set FunctionInvocationRequest object
    let fxInvocation: FunctionInvocationRequest;
    if (accessToken && functionInvocationId) {
        fxInvocation = new FunctionInvocationRequest(functionInvocationId, logger, org.data);
        context['fxInvocation'] = fxInvocation;
    }
    return context;
}
