module.exports = {
  name: 'VolanteMongolap',
  props: {
    allowedNamespaces: [],       // IMPORTANT: limit the allowed mongo namespaces, make sure you set this,
                                 //            leaving it empty could be a security risk as it may allow
                                 //            access to all collections from client-side
    timestampField: 'ts',        // default name for the datetime field
    countMeasure: 'count',       // name of the virtual-measure for the count of documents
  },
  events: {
    // event bridge to insert with no response
    'VolanteMongolap.insert'(args) {
      this.insert(args);
    },
  },
  data() {
    return {
      rangePresets: {
        '1 Minute'() {
          return new Date(Date.now() - 6e4);
        },
        '1 Hour'() {
          return new Date(Date.now() - 36e5);
        },
        '12 Hours'() {
          return new Date(Date.now() - 432e5);
        },
        '24 Hours'() {
          return new Date(Date.now() - 864e5);
        },
        '3 Days'() {
          return new Date(Date.now() - 2592e5);
        },
        '7 Days'() {
          return new Date(Date.now() - 6048e5);
        },
        '14 Days'() {
          return new Date(Date.now() - 12096e5);
        },
        '30 Days'() {
          return new Date(Date.now() - 2592e6);
        },
        '90 Days'() {
          return new Date(Date.now() - 7776e6);
        },
        '180 Days'() {
          return new Date(Date.now() - 15552e6);
        },
        '1 Year'() {
          return new Date(Date.now() - 31536e6);
        },
        'All Time'() {
          return new Date(0);
        },
      },
    };
  },
  methods: {
    //
    // Return formatted range preset names for use in a UI
    //
    getRangePresets() {
      return Object.keys(this.rangePresets);
    },
    //
    // insert a record into the specified namespace, ensures
    // that the namespace is allowed and that a valid timestamp field exists
    //
    insert({
      namespace,          // required, the namespace to insert into
      doc = {},             // document to insert
    }) {
      // check if specified namespace is allowed
      if (this.allowedNamespaces.length > 0 &&
          this.allowedNamespaces.indexOf(namespace) < 0) {
        return Promise.reject('invalid namespace');
      }
      // ensure theres is a ts field, if not set one with the current Date
      if (!doc[this.timestampField]) {
        doc[this.timestampField] = new Date();
      } else if (typeof doc[this.timestampField] === 'string') {
        // attempt to convert to date if string
        doc[this.timestampField] = new Date(doc[this.timestampField]);
      }
      // send the doc to mongo
      return this.$.VolanteMongo.insertOne(namespace, doc).catch((err) => {
        this.$warn('error on insert', err);
        return Promise.reject(err);
      });
    },
    //
    // run a query against the specified namespace using the parameters described below
    //
    query({
      namespace,             // required, the namespace to query
      range,                 // optional time range, either a key from rangePresets or array or string dates or Date objects: ['st', 'et']
      dimensions = [],       // { field: '', op: '$in/$nin/$regex/etc.', value: Object/String } (only field is required, op and value set up filtering)
      measures = [],         // { field: '', sort: 'ascending/descending', op: '$sum/$min/$max/$avg/etc.' } (only field is required)
      granularity = 'all',   // all/hour/minute/second
      limit,                 // limit results
      debug                  // print out pipeline sent to mongo for debug purposes
    }) {
      // if allowedNamespaces was set, check if specified namespace is allowed
      if (this.allowedNamespaces.length > 0 && this.allowedNamespaces.indexOf(namespace) < 0) {
        return Promise.reject('namespace not in allowedNamespaces');
      }
      // SORT STAGE
      let sort = {};

      // MATCH STAGE
      let match = {};
      // add time filtering first
      if (range) {
        let startTime, endTime;
        if (typeof(range) === 'string' && this.rangePresets[range]) {
          startTime = this.rangePresets[range]();
          endTime = new Date();
        } else {
          // let Date try to parse the times
          // to make sure they're in the right format
          startTime = new Date(range[0]);
          endTime = new Date(range[1]);
        }
        match[this.timestampField] = {
          $gte: startTime,
          $lte: endTime,
          $type: 'date',
        };
      } else {
        // at least ensure that the timestamp field exists in results
        match[this.timestampField] = { $exists: true, $type: 'date' };
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
        project[this.timestampField] = true;
        // this is the default granularity (day)
        group._id.year = {
          $year: `$${this.timestampField}`
        };
        group._id.month = {
          $month: `$${this.timestampField}`
        };
        group._id.day = {
          $dayOfMonth: `$${this.timestampField}`
        };
        // handle the finer granularities by adding h:m:s
        if (granularity === 'hour') {
          group._id.hour = {
            $hour: `$${this.timestampField}`
          };
        } else if (granularity === 'minute') {
          group._id.hour = {
            $hour: `$${this.timestampField}`
          };
          group._id.minute = {
            $minute: `$${this.timestampField}`
          };
        } else if (granularity === 'second') {
          group._id.hour = {
            $hour: `$${this.timestampField}`
          };
          group._id.minute = {
            $minute: `$${this.timestampField}`
          };
          group._id.second = {
            $second: `$${this.timestampField}`
          };
        }
        sort._id = 1;
      }
      // process dimensions
      for (let dim of dimensions) {
        // add any specified filtering op
        if (dim.op) {
          match[dim.field] = { [dim.op]: dim.value };
        }
        // add the field to projections to be projected
        project[dim.field] = true;
        // add it to be grouped
        group._id[dim.field] = `$${dim.field}`;
      }
      // process measures
      for (let meas of measures) {
        // treat countMeasure special since it's not actually in the mongo documents,
        // it's just a meta-count of the documents
        if (meas.field === this.countMeasure) {
          group[this.countMeasure] = { $sum: 1 };
        } else { // user measure
          // add it to be projected
          project[meas.field] = true;
          let op = meas.op || '$sum';
          // add to group with user-specified operation or default to $sum
          group[meas.field] = { [op]: `$${meas.field}` };
        }
        // SORT only for non-timeseries (granularity == 'all')
        if (granularity === 'all') {
          if (meas.sort === 'ascending') {
            sort[meas.field] = 1;
          } else if (meas.sort === 'descending') {
            sort[meas.field] = -1;
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
          // convert binned date fields back to timestamps
          switch (granularity) {
            case 'day':
              d[this.timestampField] = new Date(d._id.year, d._id.month-1, d._id.day);
              break;
            case 'hour':
              d[this.timestampField] = new Date(d._id.year, d._id.month-1, d._id.day, d._id.hour);
              break;
            case 'minute':
              d[this.timestampField] = new Date(d._id.year, d._id.month-1, d._id.day, d._id.hour, d._id.minute);
              break;
            case 'second':
              d[this.timestampField] = new Date(d._id.year, d._id.month-1, d._id.day, d._id.hour, d._id.minute, d._id.second);
              break;
          }
          // promote dimension fields
          for (let dim of dimensions) {
            d[dim.field] = d._id[dim.field];
          }
          // remove _id completely
          delete d._id;
        }
        return docs;
      }).catch((err) => {
        if (debug) {
          this.$warn(err);
        }
        return Promise.reject(err);
      });
    },
    //
    // Run a table scan, returns complete documents within the time range and limit
    //
    scan({
      namespace,             // required, the namespace to query
      range,                 // optional time range, either a key from rangePresets or array or string dates or Date objects: ['st', 'et']
      limit = 100,           // limit results, set null to return all
      order,                 // timestampField sort order descending/ascending(default)
    }) {
      // if allowedNamespaces was set, check if specified namespace is allowed
      if (this.allowedNamespaces.length > 0 && this.allowedNamespaces.indexOf(namespace) < 0) {
        return Promise.reject('namespace not in allowedNamespaces');
      }
      let filter = {};
      if (range) {
        let startTime, endTime;
        if (typeof(range) === 'string' && this.rangePresets[range]) {
          startTime = this.rangePresets[range]();
          endTime = new Date();
        } else {
          // let Date try to parse the times
          // to make sure they're in the right format
          startTime = new Date(range[0]);
          endTime = new Date(range[1]);
        }
        filter[this.timestampField] = {
          $gte: startTime,
          $lte: endTime,
          $type: 'date',
        };
      }
      let sort = 1;
      if (order === 'descending') {
        sort = -1;
      }
      // send the query to mongo
      return this.$.VolanteMongo.find(namespace, filter, {
        sort: {
          [this.timestampField]: sort,
        },
        limit,
      });
    },
  },
};
