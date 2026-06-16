/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

'use strict';
const { fromEnv } = require('@aws-sdk/credential-providers');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');
const { DynamoDB } = require('@aws-sdk/client-dynamodb');

const moment = require('moment');
const event_schema = require('../config/event_schema.json');
const Ajv2020 = require('ajv/dist/2020');

const ajv = new Ajv2020();
var validate = ajv.compile(event_schema);

const creds = fromEnv('AWS'); // Lambda provided credentials

const dynamoConfig = {
  credentials: creds,
  region: process.env.AWS_REGION
};

const convertTimestamp = process.env.CONVERT_TIMESTAMP && process.env.CONVERT_TIMESTAMP === "true"

const dynamoClient = new DynamoDB(dynamoConfig);
const docClient = DynamoDBDocument.from(dynamoClient);

// Early versions of the MO Analytics had a bug whereby they could start spamming 
// millions of events.  We want to block these so they don't pollute the database.
// We want to early out as soon as possible for these misbehaving versions to save 
// on processing, so we hard code these here rather than wasting time looking anything 
// up in the database. 
// In later builds of MO each new build/sku gets it's own authorization that we can turn
// off in the database, so fingers crossed these are the only ones we'll ever need to
// block using this filthy hack.

const badApplications = new Map()

// Each application id has a set of versions that are bad and should be blocked.
// We have both the dev and prod environments to think of

// "Mini Motorways - Dev"
badApplications.set('edc7e5f3-1b89-46dd-bce3-9a6baf3fa847',  new Set([
  '1.0.0.1',
  '1.0.0.2'
]));

// "Mini Motorways - Prod"
badApplications.set('3bddb638-6e29-40b5-b488-f0b5cfb044c1',  new Set([
  '1.0.0.1',
  '1.0.0.2'
]));


class Event {

  constructor() {
    this.dynamoConfig = dynamoConfig;
  }

  /**
  * Process an event record sent to the events stream
  * Format processing output in format required by Kinesis Firehose
  * @param {JSON} input - game event input payload
  * @param {string} recordId - recordId from Kinesis
  * @param {JSON} context - AWS Lambda invocation context (https://docs.aws.amazon.com/lambda/latest/dg/nodejs-context.html)
  */
  async processEvent(input, recordId, context) {
    const _self = this;
    try {
      // Extract event object and applicationId string from payload. application_id and event are required or record fails processing
      if (!input.hasOwnProperty('application_id')) {
        return Promise.reject({
          recordId: recordId,
          result: 'ProcessingFailed',
          data: new Buffer.from(JSON.stringify(input) + '\n').toString('base64')
        });
      }
      if (!input.hasOwnProperty('event')) {
        return Promise.reject({
          recordId: recordId,
          result: 'ProcessingFailed',
          data: new Buffer.from(JSON.stringify(input) + '\n').toString('base64')
        });
      }
      const applicationId = input.application_id;
      const event = input.event;

      let badVersions = badApplications.get(String(applicationId));
      if(badVersions!=undefined)
      { 
        if (event.hasOwnProperty('app_version')) {
          if(badVersions.has(String(event.app_version))) {
            // Note: Even though we reject the event here, the game will still receive an OK HTTP 
            // response and the contents of that response doesn't indicate that that the event 
            // processing failed. This is what we want as the game only cares about the HTTP 
            // response type, and we want the game to _think_ the events have been sent successfully
            // and so it should not try to resend them.
            return Promise.reject({
              recordId: recordId,
              result: 'ProcessingFailed',
              data: new Buffer.from(JSON.stringify(input) + '\n').toString('base64')
            });
          }
        }
      }


      // Add a processing timestamp and the Lambda Request Id to the event metadata
      let metadata = {
        ingestion_id: context.awsRequestId,
        processing_timestamp: moment().unix()
      };

      // If event came from Solution API, it should have extra metadata
      if (input.aws_ga_api_validated_flag) {
        metadata.api = {};
        if (input.aws_ga_api_requestId) {
          metadata.api.request_id = input.aws_ga_api_requestId;
          delete input.aws_ga_api_requestId;
        }
        if (input.aws_ga_api_requestTimeEpoch) {
          metadata.api.request_time_epoch = input.aws_ga_api_requestTimeEpoch;
          delete input.aws_ga_api_requestTimeEpoch;
        }
        delete input.aws_ga_api_validated_flag;
      }

      // Retrieve application config from Applications table
      const application = await _self.getApplication(applicationId);
      if (application !== null) {
        // Validate the input record against solution event schema
        const schemaValid = await _self.validateSchema(input);
        let transformed_event = {};
        if (schemaValid.validation_result == 'schema_mismatch') {
          metadata.processing_result = {
            status: 'schema_mismatch',
            validation_errors: schemaValid.validation_errors
          };
          transformed_event.metadata = metadata;
          //console.log(`Errors processing event: ${JSON.stringify(errors)}`);
        } else {
          metadata.processing_result = {
            status: 'ok'
          };
          transformed_event.metadata = metadata;
        }

        if (event.hasOwnProperty('event_id')) {
          transformed_event.event_id = String(event.event_id);
        }
        if (event.hasOwnProperty('event_type')) {
          transformed_event.event_type = String(event.event_type);
        }
        if (event.hasOwnProperty('event_name')) {
          transformed_event.event_name = String(event.event_name);
        }
        if (event.hasOwnProperty('event_version')) {
          transformed_event.event_version = String(event.event_version);
        }
          // If event_timestamp_ms is provided, use it as the timestamp. 
          // Otherwise, fall back to event_timestamp for backwards compatibility
        if (event.hasOwnProperty('event_timestamp_ms')) {
          if (convertTimestamp) { 
            let newDate = new Date(0);
            // Note event_timestamp_ms since epoch is stored as an integer in a JS Number field 
            // It will start to lose precision after about 285,000 years (if my math is right). 
            // If you are still reading this after that becomes a problem then... sorry.

            newDate.setUTCMilliseconds(Number(event.event_timestamp_ms));
            transformed_event.event_timestamp = newDate;
          } else {
            transformed_event.event_timestamp = Number(event.event_timestamp);
          }
        } else if (event.hasOwnProperty('event_timestamp')) {
          if (convertTimestamp) { 
            let newDate = new Date(0);
            newDate.setUTCSeconds(Number(event.event_timestamp));
            transformed_event.event_timestamp = newDate;
          } else {
            transformed_event.event_timestamp = Number(event.event_timestamp);
          }
        }
        if (event.hasOwnProperty('app_version')) {
          transformed_event.app_version = String(event.app_version);
        }
        if (event.hasOwnProperty('event_data')) {
          transformed_event.event_data = event.event_data;
        }

        transformed_event.application_name = String(application.application_name);
        transformed_event.application_id = String(applicationId);

        return Promise.resolve({
          recordId: recordId,
          result: 'Ok',
          data: new Buffer.from(JSON.stringify(transformed_event) + '\n').toString('base64')
        });
      } else {
        /**
         * Handle events from unregistered ("NOT_FOUND") applications
         * Sets processing result as "unregistered"
         * We don't attempt to validate schema of unregistered events, we just coerce the necessary fields into expected format 
         */
        metadata.processing_result = {
          status: 'unregistered'
        };
        let unregistered_format = {};
        unregistered_format.metadata = metadata;

        if (event.hasOwnProperty('event_id')) {
          unregistered_format.event_id = String(event.event_id);
        }
        if (event.hasOwnProperty('event_type')) {
          unregistered_format.event_type = String(event.event_type);
        }
        if (event.hasOwnProperty('event_name')) {
          unregistered_format.event_name = String(event.event_name);
        }
        if (event.hasOwnProperty('event_version')) {
          unregistered_format.event_version = String(event.event_version);
        }
        if (event.hasOwnProperty('event_timestamp_ms')) {
          unregistered_format.event_timestamp = Number(event.event_timestamp_ms);
        } else if (event.hasOwnProperty('event_timestamp')) {
          unregistered_format.event_timestamp = Number(event.event_timestamp)*1000 ;
        }
        if (event.hasOwnProperty('app_version')) {
          unregistered_format.app_version = String(event.app_version);
        }
        if (event.hasOwnProperty('event_data')) {
          unregistered_format.event_data = event.event_data;
        }

        // Even though the application_id is not registered, let's add it to the event
        unregistered_format.application_id = String(applicationId);

        return Promise.resolve({
          recordId: recordId,
          result: 'Ok',
          data: new Buffer.from(JSON.stringify(unregistered_format) + '\n').toString('base64')
        });
      }
    } catch (err) {
      console.error(`Error processing record: ${JSON.stringify(err)}`);
      return Promise.reject({
        recordId: recordId,
        result: 'ProcessingFailed',
        data: new Buffer.from(JSON.stringify(input) + '\n').toString('base64')
      });
    }
  }

  /**
   * Retrieve application from DynamoDB
   * Fetches from and updates the local registered applications cache with results
   */
  async getApplication(applicationId) {
    const params = {
      TableName: process.env.APPLICATIONS_TABLE,
      Key: {
        application_id: applicationId
      }
    };

    // first try to fetch from cache
    let applicationsCacheResult = global.applicationsCache.get(applicationId);
    if (applicationsCacheResult == 'NOT_FOUND') {
      // if already marked not found, skip processing. Applications will remain "NOT_FOUND" until the cache refresh
      return Promise.resolve(null);
    } else if (applicationsCacheResult == undefined) {
      // get from DynamoDB and set in Applications cache

      try {
        let data = await docClient.get(params);
        if (data?.Item != undefined) {
          // if found in ddb, set in cache and return it
          global.applicationsCache.set(applicationId, data.Item);
          return Promise.resolve(data.Item);
        } else {
          // if application isn't registered in dynamodb, set not found in cache
          console.log(`Application ${applicationId} not found in DynamoDB`);
          global.applicationsCache.set(applicationId, 'NOT_FOUND');
          return Promise.resolve(null);
        }
      } catch (err) {
        console.error("Error encountered in getApplication");
        console.error(JSON.stringify(err));
        return Promise.reject(err);
      }
    } else {
      // if in cache, return it
      return Promise.resolve(applicationsCacheResult);
    }
  }

  /**
   * Validate input data against JSON schema
   */
  async validateSchema(data) {
    try {
      let valid = validate(data);
      if (!valid) {
        let errors = validate.errors;
        return Promise.resolve({
          validation_result: 'schema_mismatch',
          validation_errors: errors
        });
      } else {
        return Promise.resolve({
          validation_result: 'ok'
        });
      }
    } catch (err) {
      console.error(`There was an error validating the schema ${JSON.stringify(err)}`);
      return Promise.reject(err);
    }
  }
}


module.exports = Event;