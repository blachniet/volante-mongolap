module.exports = {
  name: 'Example',
  events: {
    'VolanteMongo.ready'() {
      this.startTimers();
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
      this.$.VolanteMongoMetrics.insert({
        namespace: 'testMetrics',
        metric: {
          example: 'hello world 2',
          value: this.$.VolanteUtils.randomInteger(),
        },
      });
    },
    async queryMetrics() {
      this.$log('querying metrics');
      let results = await this.$.VolanteMongoMetrics.query({
        namespace: 'testMetrics',
        dimensions: ['example'],
        measures: [{
          field: 'value',
          sort: 'descending'
        }],
        granularity: 'minute',
        debug: true,
      });
      this.$log(results);
    },
  },
};