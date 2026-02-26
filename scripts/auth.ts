/**
 * auth.ts
 * Manage Notion authentication tokens (save/load from ~/.config/ncli/auth.json).
 */

import fs from 'node:fs'
import path from 'node:path'
import os from 'node:os'

// ---------------------------------------------------------------------------
// Config file location
// ---------------------------------------------------------------------------

function configPath(): string {
  return path.join(os.homedir(), '.config', 'ncli', 'auth.json')
}

interface AuthConfig {
  token: string
  type: 'internal' | 'oauth'
  savedAt: string
}

function loadConfig(): AuthConfig | null {
  try {
    const raw = fs.readFileSync(configPath(), 'utf-8')
    return JSON.parse(raw) as AuthConfig
  } catch {
    return null
  }
}

function saveConfig(config: AuthConfig): void {
  const dir = path.dirname(configPath())
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(configPath(), JSON.stringify(config, null, 2), { mode: 0o600 })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Resolve Notion token from (in priority order):
 *   1. NOTION_TOKEN environment variable
 *   2. Saved auth config (~/.config/ncli/auth.json)
 */
export function resolveToken(): string | null {
  if (process.env.NOTION_TOKEN) {
    return process.env.NOTION_TOKEN
  }
  const config = loadConfig()
  return config?.token ?? null
}

/**
 * Save an internal integration token to the config file.
 */
export async function loginWithToken(token: string): Promise<void> {
  if (!token || !token.trim()) {
    console.error('Error: Token must not be empty.')
    process.exit(1)
  }

  saveConfig({
    token: token.trim(),
    type: 'internal',
    savedAt: new Date().toISOString(),
  })

  console.log('Authentication saved successfully.')
  console.log(`Config: ${configPath()}`)
  console.log('Token type: Internal integration token')
}

/**
 * Placeholder for OAuth flow.
 */
export async function loginWithOAuth(
  clientId?: string,
  clientSecret?: string
): Promise<void> {
  if (!clientId || !clientSecret) {
    console.error('Error: --client-id and --client-secret are required for OAuth flow.')
    process.exit(1)
  }

  console.error('OAuth flow is not yet implemented.')
  console.error('Use --token <token> for internal integration tokens instead.')
  process.exit(1)
}

/**
 * Print current auth status.
 */
export function showAuthStatus(): void {
  const envToken = process.env.NOTION_TOKEN
  const config = loadConfig()

  if (envToken) {
    console.log('Authentication status: Active (from NOTION_TOKEN environment variable)')
    console.log(`Token prefix: ${envToken.substring(0, 8)}...`)
    if (config) {
      console.log(`Saved config also exists at: ${configPath()}`)
    }
    return
  }

  if (config) {
    console.log('Authentication status: Active (from saved config)')
    console.log(`Token prefix: ${config.token.substring(0, 8)}...`)
    console.log(`Token type: ${config.type}`)
    console.log(`Saved at: ${config.savedAt}`)
    console.log(`Config: ${configPath()}`)
    return
  }

  console.log('Authentication status: Not authenticated')
  console.log('Run "ncli auth login --token <token>" to authenticate.')
}

/**
 * Remove saved auth config.
 */
export function logout(): void {
  const p = configPath()
  try {
    fs.unlinkSync(p)
    console.log('Logged out. Saved authentication removed.')
  } catch (err: any) {
    if (err.code === 'ENOENT') {
      console.log('No saved authentication found.')
    } else {
      console.error(`Error removing config: ${err.message}`)
      process.exit(1)
    }
  }
}
