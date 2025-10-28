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
        // Skip queue init on lazy start
        if (this.opts.lazyStart) {
          return next()
        }

        this._startQueue()

        if (this.opts.control) {
          this.queue.on('cleaned', (jobs, type) => {
            if (this.opts.debug) {
              console.log('cleaned %s %s jobs', jobs.length, type)
            }
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
        if (this.queue) {
          this.queue.close().then(() => { })
        }

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

  /**
   * Push a job to the queue
   * By default will close the queue after adding the job
   *
   * @param {Object} data - job data
   * @param {Object} opts - job options
   * @param {boolean} closeQueue - close the queue after adding the job
   * @returns {Promise<Object>}
   */
  async pushJob (data, opts = {}, closeQueue = true) {
    if (!this.queue) {
      this._startQueue()
    }

    try {
      const job = await this.queue?.add(data, opts)
      return job
    } finally {
      if (this.queue && closeQueue) {
        await this.queue.close()
        this.queue = null
      }
    }
  }
}

module.exports = BullFacility
