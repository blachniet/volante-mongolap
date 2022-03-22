module.exports = {
  name: 'VolanteMongoMetrics',
  props: {
    defaultTimestampField: 'ts',           // the field to use for timestamps (only necessary if time-bounding)
    allowedNamespaces: [],                 // set to limit the allowed namespaces
    routePrefix: '/api/volante-analytics', // the route prefix to use
    authModule: null,                      // name of volante module providing auth
    authMethod: null,                      // name of auth module's auth method (should be express middleware method)
    countMeasure: 'count',                 // name of the virtual-measure for the count of documents
    datesAsStrings: false,                 // whether to treat the startTime and endTime params as strings
  },
  init() {
    this.router = require('express').Router();

    /**
     * @openapi
     *  /api/volante-analytics/query/{namespace}:
     *    post:
     *      tags:
     *        - analytics
     *      summary: query the analytics collection
     *      description: performs an aggregated query against the collected history
     *      parameters:
     *        - name: namespace
     *          in: path
     *          description: the mongo namespace (or volante-mongo alias to query)
     *          required: true
     *          type: string
     *      requestBody:
     *        content:
     *          application/json:
     *            schema:
     *              type: object
     *              properties:
     *                timestampField:
     *                  description: the field holding the timestamp, if different from default
     *                  type: string
     *                startTime:
     *                  description: the start time for the query
     *                  type: string
     *                  example: '2021-02-17T00:28:58.131Z'
     *                endTime:
     *                  description: the end time for the query
     *                  type: string
     *                  example: '2021-06-08T18:56:43.851Z'
     *                dimensions:
     *                  description: the field names to include as dimensions
     *                  type: array
     *                  items:
     *                    type: string
     *                measures:
     *                  description: the field names to aggregate as measures
     *                  type: array
     *                  items:
     *                    type: object
     *                    properties:
     *                      field:
     *                        description: field name to use for measure
     *                        type: string
     *                        example: value
     *                      operator:
     *                        description: mongo aggregation operator to apply
     *                        type: string
     *                        example: $sum
     *                      sort:
     *                        description: if defined, apply sort to this measure
     *                        type: string
     *                        example: none
     *                filters:
     *                  description: object of mongo-compatible filters (https://docs.mongodb.com/manual/tutorial/query-documents/)
     *                  type: object
     *                limit:
     *                  description: limit the number of returned documents
     *                  type: number
     *                  example: 100
     *      responses:
     *        200:
     *          description: OK
     *        401:
     *          description: Unauthorized
     *        500:
     *          description: Database error
     *      security:
     *        - JWT: []
     */
    this.router.route(`${this.routePrefix}/query/:namespace`)
    .all(this.authenticationProxy)
    .post((req, res) => {
      // if allowedNamespaces was set, check if specified namespace is allowed
      if (this.allowedNamespaces.length > 0 && this.allowedNamespaces.indexOf(req.params.namespace) < 0) {
        return res.status(400).send('namespace not in allowedNamespaces');
      }
      let dimensions = req.body.dimensions || [];
      let measures = req.body.measures || [];
      let timestampField = req.body.timestampField || this.defaultTimestampField;
      let sort = {};

      // GRANULARITY
      let granularity = req.body.granularity || 'all';

      // MATCH STAGE
      let match = {};
      if (req.body.startTime && req.body.endTime) {
        if (this.datesAsStrings) {
          match[timestampField] = {
            $gte: req.body.startTime,
            $lte: req.body.endTime
          };
        } else {
          match[timestampField] = {
            $gte: new Date(req.body.startTime),
            $lte: new Date(req.body.endTime)
          };
        }
      }

      // PROJECT STAGE
      let project = {
        _id: false,
      };

      // GROUP
      let group = {
        _id: {}, // _id is the grouping var, holds all dimensions including time
      };
      if (granularity !== 'all') {
        // project the timestamp key if granularity is not 'all'
        project[timestampField] = true;
        // this is the default granularity (day)
        group._id.year = {
          $year: `$${timestampField}`
        };
        group._id.month = {
          $month: `$${timestampField}`
        };
        group._id.day = {
          $dayOfMonth: `$${timestampField}`
        };
        // handle the finer granularities by adding h:m:s
        if (granularity === 'hour') {
          group._id.hour = {
            $hour: `$${timestampField}`
          };
        } else if (granularity === 'minute') {
          group._id.hour = {
            $hour: `$${timestampField}`
          };
          group._id.minute = {
            $minute: `$${timestampField}`
          };
        } else if (granularity === 'second') {
          group._id.hour = {
            $hour: `$${timestampField}`
          };
          group._id.minute = {
            $minute: `$${timestampField}`
          };
          group._id.second = {
            $second: `$${timestampField}`
          };
        }
        sort = {
          _id: 1,
        };
      }
      // process dimensions
      for (let d of dimensions) {
        // add it to be projected
        project[d] = true;
        group._id[d] = `$${d}`;
      }
      // process measures
      for (let d of measures) {
        // treat countMeasure special since it's not actually in the mongo documents,
        // it's just a meta-count of the documents
        if (d.field === this.countMeasure) {
          group[this.countMeasure] = { $sum: 1 };
        } else {
          // add it to be projected
          project[d.field] = true;
          group[d.field] = { $sum: `$${d.field}` };
        }
        // SORT only for non-timeseries (granularity == 'all')
        if (granularity === 'all') {
          if (d.sort === 'ascending') {
            sort[d.field] = 1;
          } else if (d.sort === 'descending') {
            sort[d.field] = -1;
          }
        }
      }

      let pipeline = [
        { $match: match },
        { $project: project },
        { $group: group },
      ];
      // only add SORT pipeline item if specified
      if (Object.keys(sort).length > 0) {
        pipeline.push({ $sort: sort });
      }

      // LIMIT
      if (req.body.limit) {
        pipeline.push({ $limit: parseInt(req.body.limit, 10) });
      }

      // send the query to mongo
      this.$emit('mongo.aggregate', req.params.namespace, pipeline, (err, docs) => {
        if (err) return res.status(500).send(err);
        // post-processing
        for (let d of docs) {
          // convert date fields back to timestamps
          switch (granularity) {
            case 'day':
              d.ts = new Date(d._id.year, d._id.month, d._id.day);
              break;
            case 'hour':
              d.ts = new Date(d._id.year, d._id.month, d._id.day, d._id.hour);
              break;
            case 'minute':
              d.ts = new Date(d._id.year, d._id.month, d._id.day, d._id.hour, d._id.minute);
              break;
            case 'second':
              d.ts = new Date(d._id.year, d._id.month, d._id.day, d._id.hour, d._id.minute, d._id.second);
              break;
          }
          // promote dimension fields
          for (let dim of dimensions) {
            d[dim] = d._id[dim];
          }
          // remove _id completely
          delete d._id;
        }
        res.send(docs);
      });
    });

    /**
     * @openapi
     *  /api/volante-analytics/concentrator/{namespace}:
     *    post:
     *      tags:
     *        - analytics
     *      summary: insert an analytics document
     *      description: inserts an analytics document into the given namespace
     *      parameters:
     *        - name: namespace
     *          in: path
     *          description: the mongo namespace (or volante-mongo alias to query)
     *          required: true
     *          type: string
     *      requestBody:
     *        content:
     *          application/json:
     *            schema:
     *              type: object
     *      responses:
     *        200:
     *          description: OK
     *        401:
     *          description: Unauthorized
     *        500:
     *          description: Database error
     *      security:
     *        - JWT: []
     */
    this.router.route(`${this.routePrefix}/concentrator/:namespace`)
    .all(this.authenticationProxy)
    .post((req, res) => {
      // check if specified namespace is allowed
      if (this.allowedNamespaces.indexOf(req.params.namespace) < 0) {
        return res.status(400).send('invalid namespace');
      }
      // ensure theres is a "defaultTimestampField", if not set one with the current Date
      if (!req.body[this.defaultTimestampField]) {
        req.body[this.defaultTimestampField] = new Date();
      }
      // send the body to mongo
      this.$emit('mongo.insertOne', req.params.namespace, req.body, (err, rslt) => {
        if (err) return res.status(500).send(err);
        res.send(rslt);
      });
    });
  },
  data() {
    return {
      router: null, // the express.js router for this module
    };
  },
  events: {
    'VolanteExpress.pre-start'() {
      this.$log('adding router to VolanteExpress');
      this.$emit('VolanteExpress.use', this.router);
    },
  },
  methods: {
    authenticationProxy(req, res, next) {
      // default to passthrough authentication, if user sets 'authModule' and 'authMethod'
      // look that up through volante
      if (this.authModule && this.authMethod) {
        return this.$hub.get(this.authModule)[this.authMethod](req, res, next);
      } else {
        next();
      }
    },
  },
};

if (require.main === module) {
  console.log('running in standalone mode');
  const volante = require('volante');
  let hub = new volante.Hub().loadConfig('config.json');
  hub.attachFromObject(module.exports);
  hub.emit('VolanteExpress.start');
}
