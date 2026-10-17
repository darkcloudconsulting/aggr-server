/*
 * Lightweight internet stability monitor.
 *
 * Every NETMON_INTERVAL ms, opens a TCP connection to a handful of well known
 * hosts (and resolves a hostname through DNS), then writes the results to
 * InfluxDB so outages can be overlaid on trade data gaps (Chronograf, Grafana).
 *
 * No dependencies, no privileges (TCP connect instead of ICMP).
 *
 * Measurements (database NETMON_DATABASE, default "netmon"):
 *   net_probe    tags: target                fields: up (0|1), rtt_ms (float, only when up)
 *   net_internet                             fields: online (0|1), up_targets (int), total_targets (int)
 *
 * "online" is 1 when at least NETMON_MIN_UP targets answered, so a single
 * provider being down is not reported as an outage.
 */

const net = require('net')
const dns = require('dns')
const http = require('http')

const env = process.env

const INTERVAL = +env.NETMON_INTERVAL || 5000
const TIMEOUT = +env.NETMON_TIMEOUT || 2000
const MIN_UP = +env.NETMON_MIN_UP || 2
const MAX_BUFFER = +env.NETMON_MAX_BUFFER || 20000 // lines kept while influx is unreachable

const INFLUX_HOST = env.INFLUX_HOST || 'localhost'
const INFLUX_PORT = +env.INFLUX_PORT || 8086
const DATABASE = env.NETMON_DATABASE || 'netmon'
const RETENTION = env.NETMON_RETENTION || '30d'

// "host:port" list, comma separated
const TARGETS = (env.NETMON_TARGETS || '1.1.1.1:443,8.8.8.8:443,9.9.9.9:443,api.binance.com:443')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean)
  .map(s => {
    const i = s.lastIndexOf(':')
    return { name: s, host: s.slice(0, i), port: +s.slice(i + 1) || 443 }
  })

const DNS_HOST = env.NETMON_DNS_HOST || 'www.google.com'

let buffer = []
let online = null
let offlineSince = null

function probeTcp({ host, port }) {
  return new Promise(resolve => {
    const start = process.hrtime.bigint()
    const socket = net.connect({ host, port })
    let done = false

    const finish = up => {
      if (done) return
      done = true
      socket.destroy()
      resolve(up ? Number(process.hrtime.bigint() - start) / 1e6 : null)
    }

    socket.setTimeout(TIMEOUT)
    socket.once('connect', () => finish(true))
    socket.once('timeout', () => finish(false))
    socket.once('error', () => finish(false))
  })
}

function probeDns() {
  return new Promise(resolve => {
    const start = process.hrtime.bigint()
    const timer = setTimeout(() => resolve(null), TIMEOUT)
    dns.lookup(DNS_HOST, err => {
      clearTimeout(timer)
      resolve(err ? null : Number(process.hrtime.bigint() - start) / 1e6)
    })
  })
}

function influxRequest(path, body) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: INFLUX_HOST, port: INFLUX_PORT, path, method: 'POST', timeout: 5000 },
      res => {
        res.resume()
        res.on('end', () =>
          res.statusCode < 300 ? resolve() : reject(new Error('influx responded ' + res.statusCode))
        )
      }
    )
    req.on('timeout', () => req.destroy(new Error('influx timeout')))
    req.on('error', reject)
    req.end(body)
  })
}

async function flush() {
  if (!buffer.length) return

  const lines = buffer
  buffer = []

  try {
    await influxRequest(`/write?db=${DATABASE}&precision=ms`, lines.join('\n'))
  } catch (error) {
    // keep for next attempt (oldest dropped first)
    buffer = lines.concat(buffer).slice(-MAX_BUFFER)
    console.error(`[netmon] write failed (${buffer.length} points buffered): ${error.message}`)
  }
}

async function setupDatabase() {
  for (;;) {
    try {
      const q = encodeURIComponent(
        `CREATE DATABASE "${DATABASE}" WITH DURATION ${RETENTION} NAME "netmon_${RETENTION}"`
      )
      await influxRequest(`/query?q=${q}`)
      console.log(`[netmon] database "${DATABASE}" ready (retention ${RETENTION})`)
      return
    } catch (error) {
      console.error(`[netmon] waiting for influx: ${error.message}`)
      await new Promise(r => setTimeout(r, 5000))
    }
  }
}

function formatTransition(isOnline, ts) {
  if (isOnline) {
    const seconds = Math.round((ts - offlineSince) / 1000)
    console.log(`[netmon] ${new Date(ts).toISOString()} INTERNET UP (was down ${seconds}s)`)
    offlineSince = null
  } else {
    offlineSince = ts
    console.log(`[netmon] ${new Date(ts).toISOString()} INTERNET DOWN`)
  }
}

async function tick() {
  const ts = Date.now()

  const results = await Promise.all([
    ...TARGETS.map(async t => ({ name: t.name, rtt: await probeTcp(t) })),
    probeDns().then(rtt => ({ name: `dns:${DNS_HOST}`, rtt }))
  ])

  let upCount = 0

  for (const { name, rtt } of results) {
    const tag = name.replace(/[ ,=]/g, '\\$&')
    if (rtt === null) {
      buffer.push(`net_probe,target=${tag} up=0i ${ts}`)
    } else {
      upCount++
      buffer.push(`net_probe,target=${tag} up=1i,rtt_ms=${rtt.toFixed(2)} ${ts}`)
    }
  }

  const isOnline = upCount >= Math.min(MIN_UP, results.length)
  buffer.push(
    `net_internet online=${isOnline ? 1 : 0}i,up_targets=${upCount}i,total_targets=${results.length}i ${ts}`
  )

  if (isOnline !== online) {
    // first tick only reports when starting offline
    if (online !== null || !isOnline) formatTransition(isOnline, ts)
    online = isOnline
  }

  await flush()
}

async function main() {
  console.log(
    `[netmon] probing ${TARGETS.map(t => t.name).join(', ')} + dns every ${INTERVAL}ms -> ${INFLUX_HOST}:${INFLUX_PORT}/${DATABASE}`
  )

  await setupDatabase()

  // sequential scheduling: a slow tick never overlaps the next one
  for (;;) {
    const start = Date.now()
    try {
      await tick()
    } catch (error) {
      console.error('[netmon] tick failed', error)
    }
    await new Promise(r => setTimeout(r, Math.max(0, INTERVAL - (Date.now() - start))))
  }
}

main()
