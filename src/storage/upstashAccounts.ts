import axios from 'axios';
import type { RawMusicAccount } from '../accounts';
import type { AccountStore } from './accounts';

/**
 * 基于 Upstash Redis REST API 的账号存储。
 *
 * 设计目标与 EdgeOne Blob 实现一致：写操作同步更新内存，由 app 在响应结束前
 * 调用 flush() 异步持久化。这样既不需要把 AccountStore 接口改成异步，
 * 又能在 Render / Railway 这类无本地磁盘的免费实例上保住账号数据。
 *
 * 存储结构：
 *   <prefix>accounts             Set，成员是每个账号的 Redis key
 *   <prefix>account:<apiKey>     String，内容为 { version, account } 的 JSON
 *
 * 每个账号单独一个 key，多实例同时写不同账号时不会互相覆盖。
 */

/** 默认前缀。刻意与旧版自定义 fork 的键名区分，避免覆盖原有数据。 */
export const DEFAULT_UPSTASH_PREFIX = 'wow-origin-next:';
/** 早期单文档键，仅用于兼容读取。 */
const LEGACY_DOCUMENT_SUFFIX = 'accounts.json';
const DOCUMENT_VERSION = 1;

export interface UpstashAccountStoreOptions {
  /** 例如 https://xxx.upstash.io */
  url: string;
  /** Upstash REST Token */
  token: string;
  /** 键名前缀，默认 DEFAULT_UPSTASH_PREFIX */
  prefix?: string;
  /** 便于日志显示的标识 */
  location?: string;
}

function indexKey(prefix: string): string {
  return `${prefix}accounts`;
}

function legacyDocumentKey(prefix: string): string {
  return `${prefix}${LEGACY_DOCUMENT_SUFFIX}`;
}

function accountKey(prefix: string, apiAccessKey: string): string {
  return `${prefix}account:${encodeURIComponent(apiAccessKey)}`;
}

function cloneAccount(account: RawMusicAccount): RawMusicAccount {
  const copy = JSON.parse(JSON.stringify(account)) as RawMusicAccount & { kugouState?: unknown };
  // 酷狗缓存是内存态数据，不应写进持久化存储（与 EdgeOne 实现保持一致）。
  delete copy.kugouState;
  return copy;
}

function sortAccounts(accounts: RawMusicAccount[]): RawMusicAccount[] {
  return [...accounts].sort((left, right) => {
    const a = String(left?.api_access_key ?? '');
    const b = String(right?.api_access_key ?? '');
    return a < b ? -1 : a > b ? 1 : 0;
  });
}

function snapshot(accounts: RawMusicAccount[]): string {
  return JSON.stringify(accounts);
}

function parseAccountDocument(text: unknown): RawMusicAccount[] {
  if (typeof text !== 'string' || !text.trim()) return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return [];
  }
  const account =
    parsed && typeof parsed === 'object' && !Array.isArray(parsed) && 'account' in (parsed as Record<string, unknown>)
      ? (parsed as { account: unknown }).account
      : parsed;
  return account && typeof account === 'object' && !Array.isArray(account)
    ? [cloneAccount(account as RawMusicAccount)]
    : [];
}

function parseLegacyDocument(text: unknown): RawMusicAccount[] {
  if (typeof text !== 'string' || !text.trim()) return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return [];
  }
  const accounts = Array.isArray(parsed)
    ? parsed
    : parsed && typeof parsed === 'object' && Array.isArray((parsed as { accounts?: unknown }).accounts)
      ? (parsed as { accounts: unknown[] }).accounts
      : [];
  return accounts
    .filter((item): item is RawMusicAccount => Boolean(item) && typeof item === 'object' && !Array.isArray(item))
    .map(cloneAccount);
}

type RedisReply = { result?: unknown; error?: string };

export class UpstashAccountStore implements AccountStore {
  readonly location: string;
  private readonly url: string;
  private readonly token: string;
  private readonly prefix: string;
  private accounts: RawMusicAccount[] = [];
  /** 待写队列：null 表示删除该账号。 */
  private readonly pending = new Map<string, RawMusicAccount | null>();
  private flushChain: Promise<void> = Promise.resolve();
  private snapshotValue = '';

  constructor(options: UpstashAccountStoreOptions) {
    this.url = options.url.replace(/\/$/, '');
    this.token = options.token;
    this.prefix = options.prefix ?? DEFAULT_UPSTASH_PREFIX;
    this.location = options.location ?? `upstash://${hostOf(this.url)}/${this.prefix}`;
  }

  static async open(options: UpstashAccountStoreOptions): Promise<UpstashAccountStore> {
    const store = new UpstashAccountStore(options);
    await store.load();
    return store;
  }

  list(): RawMusicAccount[] {
    return this.accounts.map(cloneAccount);
  }

  insert(account: RawMusicAccount): void {
    const stored = cloneAccount(account);
    const apiAccessKey = String(stored.api_access_key ?? '');
    if (!apiAccessKey) throw new Error('新增账号缺少 api_access_key');
    this.accounts = sortAccounts([...this.accounts.filter(
      (item) => String(item.api_access_key ?? '') !== apiAccessKey
    ), stored]);
    this.pending.set(accountKey(this.prefix, apiAccessKey), stored);
  }

  update(apiAccessKey: string, changes: Partial<RawMusicAccount>): void {
    const index = this.accounts.findIndex((item) => String(item.api_access_key ?? '') === apiAccessKey);
    if (index < 0) throw new Error('Upstash 中未找到对应 api_access_key');
    const next = cloneAccount({ ...this.accounts[index], ...changes, api_access_key: apiAccessKey });
    this.accounts[index] = next;
    this.pending.set(accountKey(this.prefix, apiAccessKey), next);
  }

  delete(apiAccessKey: string): void {
    this.accounts = this.accounts.filter((item) => String(item.api_access_key ?? '') !== apiAccessKey);
    this.pending.set(accountKey(this.prefix, apiAccessKey), null);
  }

  /** 把待写队列刷到 Upstash；连续调用会串行执行。 */
  flush(): Promise<void> {
    if (this.pending.size === 0) return this.flushChain;
    const batch = [...this.pending.entries()];
    this.pending.clear();
    const commands: Array<Array<string>> = [];
    for (const [key, value] of batch) {
      if (value === null) {
        commands.push(['DEL', key]);
        commands.push(['SREM', indexKey(this.prefix), key]);
      } else {
        commands.push(['SET', key, JSON.stringify({ version: DOCUMENT_VERSION, account: value })]);
        commands.push(['SADD', indexKey(this.prefix), key]);
      }
    }
    this.flushChain = this.flushChain.then(async () => {
      await this.pipeline(commands);
    });
    return this.flushChain;
  }

  /** 从 Upstash 重新读取账号；内容无变化返回 false。 */
  async reload(): Promise<boolean> {
    const fresh = await this.fetchAll();
    const next = snapshot(fresh);
    if (next === this.snapshotValue) return false;
    this.accounts = fresh;
    this.snapshotValue = next;
    return true;
  }

  private async load(): Promise<void> {
    const fresh = await this.fetchAll();
    this.accounts = fresh;
    this.snapshotValue = snapshot(fresh);
  }

  private async fetchAll(): Promise<RawMusicAccount[]> {
    const members = await this.command(['SMEMBERS', indexKey(this.prefix)]);
    const keys = Array.isArray(members) ? members.map((item) => String(item)) : [];

    const accounts: RawMusicAccount[] = [];
    if (keys.length > 0) {
      const values = await this.command(['MGET', ...keys]);
      if (Array.isArray(values)) {
        for (const value of values) accounts.push(...parseAccountDocument(value));
      }
      // 索引里有 key 但值已失效时顺手清掉，避免索引无限膨胀。
      if (Array.isArray(values)) {
        const stale = keys.filter((_, index) => values[index] === null || values[index] === undefined);
        if (stale.length > 0) {
          await this.pipeline([
            ...stale.map((key) => ['DEL', key]),
            ['SREM', indexKey(this.prefix), ...stale]
          ]);
        }
      }
    }

    if (accounts.length === 0) {
      // 兼容读取单文档键（旧版本或手工导入）。
      const legacy = await this.command(['GET', legacyDocumentKey(this.prefix)]);
      accounts.push(...parseLegacyDocument(legacy));
      if (accounts.length > 0) {
        for (const account of accounts) {
          const apiAccessKey = String(account.api_access_key ?? '');
          if (apiAccessKey) this.pending.set(accountKey(this.prefix, apiAccessKey), account);
        }
        await this.flush();
      }
    }

    return sortAccounts(accounts);
  }

  private async command(command: string[]): Promise<unknown> {
    const response = await axios.post(this.url, command, {
      headers: {
        Authorization: `Bearer ${this.token}`,
        'Content-Type': 'application/json'
      },
      timeout: 15000,
      validateStatus: () => true
    });
    const body = response.data as RedisReply | string | undefined;
    if (response.status >= 400) {
      throw new Error(`Upstash 请求失败: HTTP ${response.status} ${describe(body)}`);
    }
    if (body && typeof body === 'object' && 'error' in body && body.error) {
      throw new Error(`Upstash 返回错误: ${body.error}`);
    }
    return body && typeof body === 'object' ? body.result : undefined;
  }

  private async pipeline(commands: Array<Array<string>>): Promise<void> {
    if (commands.length === 0) return;
    const response = await axios.post(`${this.url}/pipeline`, commands, {
      headers: {
        Authorization: `Bearer ${this.token}`,
        'Content-Type': 'application/json'
      },
      timeout: 15000,
      validateStatus: () => true
    });
    if (response.status >= 400) {
      throw new Error(`Upstash 批量写入失败: HTTP ${response.status} ${describe(response.data)}`);
    }
    const results = Array.isArray(response.data) ? response.data as RedisReply[] : [];
    const failure = results.find((item) => item && typeof item === 'object' && item.error);
    if (failure) throw new Error(`Upstash 批量写入返回错误: ${failure.error}`);
  }
}

function hostOf(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

function describe(body: unknown): string {
  if (typeof body === 'string') return body.slice(0, 200);
  try {
    return JSON.stringify(body).slice(0, 200);
  } catch {
    return String(body);
  }
}

export function upstashOptionsFromEnv(): UpstashAccountStoreOptions | null {
  const url = (process.env.UPSTASH_REDIS_REST_URL || '').trim();
  const token = (process.env.UPSTASH_REDIS_REST_TOKEN || '').trim();
  if (!url || !token) return null;
  const prefix = (process.env.UPSTASH_ACCOUNTS_PREFIX || '').trim() || DEFAULT_UPSTASH_PREFIX;
  return { url, token, prefix };
}

/**
 * Node / Docker 入口使用：有 Upstash 配置时返回 Redis 账号存储，
 * 否则返回 null（沿用本地 SQLite）。
 */
export async function openAccountStoreFromEnv(): Promise<AccountStore | null> {
  const options = upstashOptionsFromEnv();
  if (!options) return null;
  const store = await UpstashAccountStore.open(options);
  console.log(`[accounts] Upstash Redis 账号存储已启用：${store.location}（已加载 ${store.list().length} 个账号）`);
  return store;
}
