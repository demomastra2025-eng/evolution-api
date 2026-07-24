import { RouterBroker } from '@api/abstract/abstract.router';
import { NumberDto } from '@api/dto/chat.dto';
import { businessController } from '@api/server.module';
import { Logger } from '@config/logger.config';
import { createMetaErrorResponse } from '@utils/errorResponse';
import { catalogSchema, collectionsSchema } from '@validate/validate.schema';
import { RequestHandler, Router } from 'express';

import { HttpStatus } from './index.router';

const logger = new Logger('BusinessRouter');

export class BusinessRouter extends RouterBroker {
  constructor(...guards: RequestHandler[]) {
    super();
    this.router
      .post(this.routerPath('getCatalog'), ...guards, async (req, res) => {
        try {
          const response = await this.dataValidate<NumberDto>({
            request: req,
            schema: catalogSchema,
            ClassRef: NumberDto,
            execute: (instance, data) => businessController.fetchCatalog(instance, data),
          });

          return res.status(HttpStatus.OK).json(response);
        } catch (error) {
          logger.error({ local: 'getCatalog', error });
          const errorResponse = createMetaErrorResponse(error, 'business_catalog');
          return res.status(errorResponse.status).json(errorResponse);
        }
      })

      .post(this.routerPath('getCollections'), ...guards, async (req, res) => {
        try {
          const response = await this.dataValidate<NumberDto>({
            request: req,
            schema: collectionsSchema,
            ClassRef: NumberDto,
            execute: (instance, data) => businessController.fetchCollections(instance, data),
          });

          return res.status(HttpStatus.OK).json(response);
        } catch (error) {
          logger.error({ local: 'getCollections', error });
          const errorResponse = createMetaErrorResponse(error, 'business_collections');
          return res.status(errorResponse.status).json(errorResponse);
        }
      });
  }

  public readonly router: Router = Router();
}
