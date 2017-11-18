var _ = require('lodash');
var util = require('../util');
var config = util.getConfig();
var dirs = util.dirs();
var log = require(dirs.core + 'log');
var moment = require('moment');
var juration = require('juration');

var adapter = config[config.adapter];
var Reader = require(dirs.gekko + adapter.path + '/reader');
var daterange = config.backtest.daterange;

if(_.isString(daterange)) {
  var delta = juration.parse(daterange);
  var from = moment().utc().subtract(delta, 'seconds');
  var to = moment().utc();
}
else {
  var from = moment.utc(daterange.from);
  var to = moment.utc(daterange.to);
}

if(to <= from)
  util.die('This daterange does not make sense.')

if(!from.isValid())
  util.die('invalid `from`');

if(!to.isValid())
  util.die('invalid `to`');

// Internally we require that to and from align perfectly to minutes
to = to.startOf('minute');
from = from.startOf('minute');

var Market = function() {

  _.bindAll(this);
  this.result = false;
  this.ended = false;
  this.closed = false;

  Readable.call(this, {objectMode: true});

  log.write('');
  log.info('\tWARNING: BACKTESTING FEATURE NEEDS PROPER TESTING');
  log.info('\tWARNING: ACT ON THESE NUMBERS AT YOUR OWN RISK!');
  log.write('');

  this.reader = new Reader();
  this.batchSize = config.backtest.batchSize;
  this.iterator = {
    from: from.clone(),
    to: to.clone()
  }
}

var Readable = require('stream').Readable;
Market.prototype = Object.create(Readable.prototype, {
  constructor: { value: Market }
});

Market.prototype._read = _.once(function() {
  this.get();
});

Market.prototype.get = function() {
  if(this.iterator.to >= to) {
    this.iterator.to = to;
  }

  this.reader.get(
    this.iterator.from.unix(),
    this.iterator.to.unix(),
    this.batchSize,
    'full',
    this.processCandles
  )
}

Market.prototype.processCandles = function(err, candles) {
  var amount = _.size(candles);

  if (amount === 0) {
    if(this.result) {
      if (!this.ended) {
        var fromStr = this.iterator.from.format('YYYY-DD-MM HH:mm:ss');
        var toStr = this.iterator.to.format('YYYY-MM-DD HH:mm:ss');
        log.warn(`Market data missing from the end of the simulation, candles missing between ${fromStr} and ${toStr}.`);
      } 
      this.closed = true;
      this.reader.close();
      this.emit('end');
    } else {
      util.die('Query returned no candles (do you have local data for the specified range?)');
    }

    return; // Cannot keep proeccesing in this case
  }

  this.result = true;
  var d = function(ts) {
    return moment.unix(ts).utc();
  }

  var fromMoment = this.iterator.from;
  var toMoment = d(_.last(candles).start);
  var duration = moment.duration(toMoment.diff(fromMoment)).asMinutes();

  // Results are [from, to], not [from, to) typical with a range of time. So we +1 to account for the extra expected candle
  if(this.batchSize < (duration + 1)) {
    var fromStr = fromMoment.format('YYYY-DD-MM HH:mm:ss');
    var toStr = toMoment.format('YYYY-MM-DD HH:mm:ss');
    log.warn(`Simulation based on incomplete market data (${this.batchSize - (duration + 1)} candles missing between ${fromStr} and ${toStr}.`);
  }
  
  this.iterator.from = toMoment.clone().add(1, 'm');
  this.ended = this.iterator.from >= to;

  _.each(candles, function(c, i) {
    c.start = moment.unix(c.start);
    this.push(c);
  }, this);

  if(!this.closed)
    this.get();
}

module.exports = Market;
