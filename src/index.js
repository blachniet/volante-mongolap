module.exports = {
  name: 'VolanteMongolap',
  props: {
    allowedNamespaces: [],       // IMPORTANT: limit the allowed mongo namespaces, make sure you set this,
                                 //            leaving it empty could be a security risk as it may allow
                                 //            access from client-side
    countMeasure: 'count',       // name of the virtual-measure for the count of documents
    datesAsStrings: false,       // whether to treat the startTime and endTime params as strings
  },
  events: {
    // event bridge to insert with no response
    'VolanteMongolap.insert'(args) {
      this.insert(args);
    },
  },
  methods: {
    insert({ namespace,          // required, the namespace to insert into
             doc,                // document to insert
             timestampField='ts' // default timestamp field, will be created if not in doc
           }) {
      // check if specified namespace is allowed
      if (this.allowedNamespaces.length > 0 &&
          this.allowedNamespaces.indexOf(namespace) < 0) {
        return Promise.reject('invalid namespace');
      }
      // ensure theres is a ts field, if not set one with the current Date
      if (!doc[timestampField]) {
        doc[timestampField] = new Date();
      } else if (typeof doc[timestampField] === 'string') {
        // attempt to convert to date if string
        doc[timestampField] = new Date(doc[timestampField]);
      }
      // send the doc to mongo
      return this.$.VolanteMongo.insertOne(namespace, doc);
    },
    // run a query using the parameters described below
    query({ namespace,           // required, the namespace to query
            startTime,           // optional
            endTime,             // optional
            dimensions=[],       // array of string field names
            measures=[],         // { 'field':, 'sort': ascending/descending }
            timestampField='ts', // optional, only for non-standard ts fields
            granularity='all',   // all/hour/minute/second
            limit,               // limit results
            debug                // print out pipeline sent to mongo for debug purposes
          }) {
      // if allowedNamespaces was set, check if specified namespace is allowed
      if (this.allowedNamespaces.length > 0 && this.allowedNamespaces.indexOf(namespace) < 0) {
        return Promise.reject('namespace not in allowedNamespaces');
      }
      // SORT STAGE
      let sort = {};

      // MATCH STAGE
      let match = {};

      if (startTime && endTime) {
        if (this.datesAsStrings) {
          match[timestampField] = {
            $gte: startTime,
            $lte: endTime,
            $type: 'date',
          };
        } else {
          match[timestampField] = {
            $gte: new Date(startTime),
            $lte: new Date(endTime),
            $type: 'date',
          };
        }
      } else {
        // ensure that the timestamp field exists
        match[timestampField] = { $exists: true, $type: 'date' };
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
        sort._id = 1;
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
      // build pipeline
      let pipeline = [
        { $match: match },
        { $project: project },
        { $group: group },
      ];
      // SORT - only add if specified
      if (Object.keys(sort).length > 0) {
        pipeline.push({ $sort: sort });
      }
      // LIMIT
      if (limit) {
        pipeline.push({ $limit: parseInt(limit, 10) });
      }
      // DEBUG print out pipeline for debug: true
      if (debug) {
        this.$debug(namespace, pipeline);
      }
      // send the query to mongo
      return this.$.VolanteMongo.aggregate(namespace, pipeline).then((docs) => {
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
        return docs;
      }).catch((err) => {
        if (debug) {
          this.$warn(err);
        }
        throw err;
      });
    }
  },
};
