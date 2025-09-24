'use strict'

const async = require('async')
const Bull = require('bull')
const Base = require('bfx-facs-base')
const { pick, isNumber, isPlainObject } = require('@bitfinex/lib-js-util-base')

class BullFacility extends Base {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'bull'
    this._hasConf = true

    this.init()
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        this._startQueue()

        if (this.opts.control) {
          this.queue.on('cleaned', (jobs, type) => {
            this._logMsg('cleaned %s %s jobs', jobs.length, type)
          })

          this._itvCount = setInterval(() => {
            this.queue.count().then(cnt => {
              if (cnt) console.log('bull count', cnt)
            })
          }, 30000)

          this._itv = setInterval(() => {
            this.queue.clean(10000, 'completed', 50000)
            this.queue.clean(10000, 'failed', 50000)
          }, 60000)
        }

        next()
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        clearInterval(this._itv)
        clearInterval(this._itvCount)
        this.queue.close().then(() => {})
        next()
      }
    ], cb)
  }

  _startQueue () {
    const redis = pick(this.conf, ['host', 'port', 'password', 'sentinels', 'name'])
    this.queue = Bull(this.opts.queue, {
      redis,
      ...this._parseLimiter()
    })
    this._addReconnectHandler(this.queue)
  }

  _parseLimiter () {
    if (!this.opts.queueOpts || !isPlainObject(this.opts.queueOpts)) return {}
    if (!this.opts.queueOpts.limiter || !isPlainObject(this.opts.queueOpts.limiter)) return {}
    if (!isNumber(this.opts.queueOpts.limiter.max) || !this.opts.queueOpts.limiter.max) return {}
    if (!isNumber(this.opts.queueOpts.limiter.duration) || !this.opts.queueOpts.limiter.duration) return {}
    return {
      limiter: {
        max: this.opts.queueOpts.limiter.max,
        duration: this.opts.queueOpts.limiter.duration
      }
    }
  }

  _addReconnectHandler (queue) {
    let reconnectingEvent = false
    queue.client.on('reconnecting', error => {
      this._logMsg(error, 'redis client reconnecting')
      reconnectingEvent = true
    })
    queue.client.on('ready', () => {
      this._logMsg('client ready')
      if (reconnectingEvent) {
        this._logMsg('rerun queue')
        reconnectingEvent = false
        queue.run(queue.concurrency)
      }
    })
  }

  _logMsg (...args) {
    if (this.opts.debug) {
      console.log(...args)
    }
  }
}

module.exports = BullFacility
