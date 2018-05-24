'use strict'

const _ = require('lodash')
const async = require('async')
const Bull = require('bull')
const Base = require('bfx-facs-base')

function client (conf, label) {
  return Bull(conf.queue, {
    redis: {
      port: conf.port,
      host: conf.host
    }
  })
}

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
        this.queue = client(_.extend(_.pick(
          this.conf,
          ['host', 'port', 'auth']
        ), {
          queue: this.opts.queue
        }))

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
        this.queue.close().then(() => {})
        next()
      }
    ], cb)
  }
}

module.exports = BullFacility
