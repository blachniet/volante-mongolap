module.exports = {
  name: 'Example',
  events: {
    'VolanteMongo.ready'() {
      this.startTimers();
      this.$debug('Range Presets', this.$.VolanteMongolap.getRangePresets());
    },
  },
  methods: {
    startTimers() {
      setInterval(this.sendMetric, 1000);
      setInterval(this.queryMetrics, 2000);
    },
    sendMetric() {
      this.$log('sending metric');
      // send metric, let volante-mongo-metrics add the timestamp
      this.$.VolanteMongolap.insert({
        namespace: 'testMetrics',
        doc: {
          example: 'hello world 1',
          value: this.$.VolanteUtils.randomInteger(),
        },
      });
    },
    async queryMetrics() {
      this.$log('querying metrics');
      let results = await this.$.VolanteMongolap.query({
        namespace: 'testMetrics',
        range: '1 Minute',
        dimensions: [{
          field: 'example',
        }],
        measures: [{
          field: 'value',
          sort: 'descending',
        }, { field: 'count' }],
        granularity: 'all',
        debug: true,
      });
      this.$log(results);
    },
  },
};