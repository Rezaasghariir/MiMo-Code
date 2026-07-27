import fs from "fs"
import path from "path"

type ProviderLimits = {
  maxRequestsPerMinute?: number
  maxConcurrent?: number
  retryOn429?: boolean
  defaultRetryAfterSeconds?: number
}

type ProviderConfig = {
  name?: string
  npm?: string
  options?: Record<string, any>
  limits?: ProviderLimits
  [k: string]: any
}

type Config = {
  provider?: Record<string, ProviderConfig>
  [k: string]: any
}

function loadConfig(): Config {
  const cfgPath = process.env.MIMOCODE_CONFIG_PATH ?? path.join(process.cwd(), "mimocode.config.json")
  try {
    if (!fs.existsSync(cfgPath)) return {}
    const raw = fs.readFileSync(cfgPath, "utf8")
    return JSON.parse(raw) as Config
  } catch (e) {
    // If parsing fails, return empty config
    console.error("Failed to load mimocode config:", e)
    return {}
  }
}

const config = loadConfig()

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms))
}

class RateLimitedQueue {
  private queue: Array<{
    task: () => Promise<any>
    resolve: (v: any) => void
    reject: (e: any) => void
    attempts: number
  }> = []
  private running = false
  private windowStart = Date.now()
  private countInWindow = 0

  constructor(
    private maxPerWindow: number = 60,
    private windowMs: number = 60_000,
    private maxAttempts = 5,
    private maxConcurrent = 1,
  ) {
    // optionally, we could track concurrent tasks; for simplicity we use maxConcurrent as a placeholder
  }

  enqueue<T>(task: () => Promise<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      this.queue.push({ task, resolve, reject, attempts: 0 })
      if (!this.running) this.processQueue()
    })
  }

  private async processQueue() {
    this.running = true
    while (this.queue.length > 0) {
      const now = Date.now()
      if (now - this.windowStart >= this.windowMs) {
        this.windowStart = now
        this.countInWindow = 0
      }

      if (this.countInWindow >= this.maxPerWindow) {
        const waitMs = this.windowMs - (now - this.windowStart)
        await sleep(waitMs)
        continue
      }

      const item = this.queue.shift()!
      try {
        this.countInWindow++
        const result = await item.task()
        item.resolve(result)
      } catch (err: any) {
        const retryAfterMs = this.extractRetryAfterMs(err)
        if (retryAfterMs != null && item.attempts < this.maxAttempts) {
          item.attempts++
          await sleep(retryAfterMs)
          this.countInWindow = Math.max(0, this.countInWindow - 1)
          this.queue.unshift(item)
          continue
        }

        if (item.attempts < this.maxAttempts) {
          item.attempts++
          const backoff = 2 ** item.attempts * 100
          await sleep(backoff)
          this.countInWindow = Math.max(0, this.countInWindow - 1)
          this.queue.unshift(item)
          continue
        }

        item.reject(err)
      }
    }
    this.running = false
  }

  private extractRetryAfterMs(err: any): number | null {
    try {
      const status = err?.status ?? err?.response?.status
      if (status !== 429 && status !== "RESOURCE_EXHAUSTED") return null
      const raw = err?.headers?.get?.("retry-after") ?? err?.response?.headers?.get?.("retry-after") ?? err?.retryAfter
      if (!raw) return null
      const seconds = Number(raw)
      if (!Number.isNaN(seconds)) return Math.ceil(seconds * 1000)
      const date = Date.parse(raw)
      if (!Number.isNaN(date)) return Math.max(0, date - Date.now())
    } catch {
      // ignore
    }
    return null
  }
}

const queues: Record<string, RateLimitedQueue> = {}

export function getQueueForProvider(providerName: string): RateLimitedQueue {
  if (queues[providerName]) return queues[providerName]

  const p: ProviderConfig | undefined = config.provider?.[providerName]
  const limits = p?.limits ?? {}
  const maxPerMinute = limits.maxRequestsPerMinute ?? 60
  const maxConcurrent = limits.maxConcurrent ?? 1
  const defaultRetryAfterSeconds = limits.defaultRetryAfterSeconds ?? 60

  const q = new RateLimitedQueue(maxPerMinute, 60_000, 5, maxConcurrent)
  queues[providerName] = q
  return q
}

export async function scheduleWithProvider<T>(
  providerName: string,
  task: () => Promise<T>,
  maxAttempts = 5,
): Promise<T> {
  const q = getQueueForProvider(providerName)
  let attempt = 0

  const run = async (): Promise<T> => {
    attempt++
    try {
      return await q.enqueue(task)
    } catch (err: any) {
      const status = err?.status ?? err?.response?.status
      const rawRetry = err?.headers?.get?.("retry-after") ?? err?.response?.headers?.get?.("retry-after") ?? err?.retryAfter

      if ((status === 429 || status === "RESOURCE_EXHAUSTED") && rawRetry) {
        let waitMs = 0
        const s = Number(rawRetry)
        if (!Number.isNaN(s)) waitMs = s * 1000
        else {
          const date = Date.parse(rawRetry)
          if (!Number.isNaN(date)) waitMs = Math.max(0, date - Date.now())
        }
        if (waitMs <= 0) {
          const defaultSec = config.provider?.[providerName]?.limits?.defaultRetryAfterSeconds ?? 60
          waitMs = defaultSec * 1000
        }
        await sleep(waitMs)
        if (attempt < maxAttempts) return run()
      }

      if (attempt < maxAttempts) {
        const backoffMs = Math.pow(2, attempt) * 100
        await sleep(backoffMs)
        return run()
      }
      throw err
    }
  }

  return run()
}

// Usage note: Import scheduleWithProvider and wrap calls to provider APIs.
// Example:
// import { scheduleWithProvider } from "./lib/providerLimiter"
// await scheduleWithProvider("nvidia", () => sendToNvidia(payload))
