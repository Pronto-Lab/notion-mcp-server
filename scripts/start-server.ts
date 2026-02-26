import path from 'node:path'
import fs from 'node:fs'
import os from 'node:os'
import { fileURLToPath } from 'url'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js'
import { randomUUID, randomBytes } from 'node:crypto'
import express from 'express'

import { initProxy, ValidationError } from '../src/init-server'
import { resolveToken, loginWithToken, loginWithOAuth, showAuthStatus, logout } from './auth.js'

export async function startServer(args: string[] = process.argv) {
  const filename = fileURLToPath(import.meta.url)
  const directory = path.dirname(filename)
  const specPath = path.resolve(directory, '../scripts/notion-openapi.json')
  
  const baseUrl = process.env.BASE_URL ?? undefined

  // Parse command line arguments manually (similar to slack-mcp approach)
  function parseArgs() {
    const args = process.argv.slice(2);
    let transport = 'stdio'; // default
    let port = 3000;
    let authToken: string | undefined;
    let disableAuth = false;

    for (let i = 0; i < args.length; i++) {
      if (args[i] === '--transport' && i + 1 < args.length) {
        transport = args[i + 1];
        i++; // skip next argument
      } else if (args[i] === '--port' && i + 1 < args.length) {
        port = parseInt(args[i + 1], 10);
        i++; // skip next argument
      } else if (args[i] === '--auth-token' && i + 1 < args.length) {
        authToken = args[i + 1];
        i++; // skip next argument
      } else if (args[i] === '--disable-auth') {
        disableAuth = true;
      } else if (args[i] === '--help' || args[i] === '-h') {
        console.log(`
Usage: ncli serve [options]

Options:
  --transport <type>     Transport type: 'stdio' or 'http' (default: stdio)
  --port <number>        Port for HTTP server when using Streamable HTTP transport (default: 3000)
  --auth-token <token>   Bearer token for HTTP transport authentication (optional)
  --disable-auth         Disable bearer token authentication for HTTP transport
  --help, -h             Show this help message

Environment Variables:
  NOTION_TOKEN           Notion integration token (recommended)
  OPENAPI_MCP_HEADERS    JSON string with Notion API headers (alternative)
  AUTH_TOKEN             Bearer token for HTTP transport authentication (alternative to --auth-token)

Examples:
  ncli serve                                    # Use stdio transport (default)
  ncli serve --transport stdio                  # Use stdio transport explicitly
  ncli serve --transport http                   # Use Streamable HTTP transport on port 3000
  ncli serve --transport http --port 8080       # Use Streamable HTTP transport on port 8080
  ncli serve --transport http --auth-token mytoken # Use Streamable HTTP transport with custom auth token
  ncli serve --transport http --disable-auth    # Use Streamable HTTP transport without authentication
  AUTH_TOKEN=mytoken ncli serve --transport http # Use Streamable HTTP transport with auth token from env var
`);
        process.exit(0);
      }
      // Ignore unrecognized arguments (like command name passed by Docker)
    }

    return { transport: transport.toLowerCase(), port, authToken, disableAuth };
  }

  const options = parseArgs()
  const transport = options.transport

  if (transport === 'stdio') {
    // Use stdio transport (default)
    const proxy = await initProxy(specPath, baseUrl)
    await proxy.connect(new StdioServerTransport())
    return proxy.getServer()
  } else if (transport === 'http') {
    // Use Streamable HTTP transport
    const app = express()
    app.use(express.json())

    // Generate or use provided auth token (from CLI arg or env var) only if auth is enabled
    let authToken: string | undefined
    if (!options.disableAuth) {
      authToken = options.authToken || process.env.AUTH_TOKEN || randomBytes(32).toString('hex')
      if (!options.authToken && !process.env.AUTH_TOKEN) {
        console.log(`Generated auth token: ${authToken}`)
        console.log(`Use this token in the Authorization header: Bearer ${authToken}`)
      }
    }

    // Authorization middleware
    const authenticateToken = (req: express.Request, res: express.Response, next: express.NextFunction): void => {
      const authHeader = req.headers['authorization']
      const token = authHeader && authHeader.split(' ')[1] // Bearer TOKEN

      if (!token) {
        res.status(401).json({
          jsonrpc: '2.0',
          error: {
            code: -32001,
            message: 'Unauthorized: Missing bearer token',
          },
          id: null,
        })
        return
      }

      if (token !== authToken) {
        res.status(403).json({
          jsonrpc: '2.0',
          error: {
            code: -32002,
            message: 'Forbidden: Invalid bearer token',
          },
          id: null,
        })
        return
      }

      next()
    }

    // Health endpoint (no authentication required)
    app.get('/health', (req, res) => {
      res.status(200).json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        transport: 'http',
        port: options.port
      })
    })

    // Apply authentication to all /mcp routes only if auth is enabled
    if (!options.disableAuth) {
      app.use('/mcp', authenticateToken)
    }

    // Map to store transports by session ID
    const transports: { [sessionId: string]: StreamableHTTPServerTransport } = {}

    // Handle POST requests for client-to-server communication
    app.post('/mcp', async (req, res) => {
      try {
        // Check for existing session ID
        const sessionId = req.headers['mcp-session-id'] as string | undefined
        let transport: StreamableHTTPServerTransport

        if (sessionId && transports[sessionId]) {
          // Reuse existing transport
          transport = transports[sessionId]
        } else if (!sessionId && isInitializeRequest(req.body)) {
          // New initialization request
          transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: () => randomUUID(),
            onsessioninitialized: (sessionId) => {
              // Store the transport by session ID
              transports[sessionId] = transport
            }
          })

          // Clean up transport when closed
          transport.onclose = () => {
            if (transport.sessionId) {
              delete transports[transport.sessionId]
            }
          }

          const proxy = await initProxy(specPath, baseUrl)
          await proxy.connect(transport)
        } else {
          // Invalid request
          res.status(400).json({
            jsonrpc: '2.0',
            error: {
              code: -32000,
              message: 'Bad Request: No valid session ID provided',
            },
            id: null,
          })
          return
        }

        // Handle the request
        await transport.handleRequest(req, res, req.body)
      } catch (error) {
        console.error('Error handling MCP request:', error)
        if (!res.headersSent) {
          res.status(500).json({
            jsonrpc: '2.0',
            error: {
              code: -32603,
              message: 'Internal server error',
            },
            id: null,
          })
        }
      }
    })

    // Handle GET requests for server-to-client notifications via Streamable HTTP
    app.get('/mcp', async (req, res) => {
      const sessionId = req.headers['mcp-session-id'] as string | undefined
      if (!sessionId || !transports[sessionId]) {
        res.status(400).send('Invalid or missing session ID')
        return
      }
      
      const transport = transports[sessionId]
      await transport.handleRequest(req, res)
    })

    // Handle DELETE requests for session termination
    app.delete('/mcp', async (req, res) => {
      const sessionId = req.headers['mcp-session-id'] as string | undefined
      if (!sessionId || !transports[sessionId]) {
        res.status(400).send('Invalid or missing session ID')
        return
      }
      
      const transport = transports[sessionId]
      await transport.handleRequest(req, res)
    })

    const port = options.port
    app.listen(port, '0.0.0.0', () => {
      console.log(`MCP Server listening on port ${port}`)
      console.log(`Endpoint: http://0.0.0.0:${port}/mcp`)
      console.log(`Health check: http://0.0.0.0:${port}/health`)
      if (options.disableAuth) {
        console.log(`Authentication: Disabled`)
      } else {
        console.log(`Authentication: Bearer token required`)
        if (options.authToken) {
          console.log(`Using provided auth token`)
        }
      }
    })

    // Return a dummy server for compatibility
    return { close: () => {} }
  } else {
    throw new Error(`Unsupported transport: ${transport}. Use 'stdio' or 'http'.`)
  }
}

// ---------------------------------------------------------------------------
// Subcommand router
// ---------------------------------------------------------------------------

function printMainHelp(): void {
  console.log(`
ncli - Notion CLI Tool

Usage: ncli <command> [options]

Commands:
  list [options]           List pages and databases in your workspace
  export [options]         Export Notion workspace to local Markdown files
  serve [options]          Start MCP server (default if no command given)
  auth <subcommand>        Manage authentication (login, status, logout)

Run 'ncli <command> --help' for more information on a command.

Environment Variables:
  NOTION_TOKEN             Notion integration token (required for export, recommended for serve)
  OPENAPI_MCP_HEADERS      JSON string with Notion API headers (alternative to NOTION_TOKEN)
`)
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  const command = args[0]

  // No command or starts with -- → default to serve
  if (!command || command.startsWith('-')) {
    if (command === '--help' || command === '-h') {
      printMainHelp()
      process.exit(0)
    }
    // Default: start MCP server (backward compatible)
    await startServer(process.argv)
    return
  }

  switch (command) {
    case 'serve':
      // Remove 'serve' from args and pass to startServer
      process.argv.splice(2, 1)
      await startServer(process.argv)
      break

    case 'export': {
      // Dynamic import to avoid bundling export script when not needed
      const { runExport } = await import('./notion-to-local.js')

      // Parse export-specific args (everything after 'export')
      const exportArgs = args.slice(1)
      let output = './notion-export'
      let pageId: string | null = null
      let includeDatabases = true
      let downloadImages = false
      let resume = false
      let fresh = false
      let help = false

      for (let i = 0; i < exportArgs.length; i++) {
        const arg = exportArgs[i]
        if (arg === '--help' || arg === '-h') help = true
        else if (arg === '--output' || arg === '-o') output = exportArgs[++i] ?? output
        else if (arg === '--page-id') pageId = exportArgs[++i] ?? null
        else if (arg === '--download-images') downloadImages = true
        else if (arg === '--resume') resume = true
        else if (arg === '--fresh') fresh = true
        else if (arg === '--include-databases') {
          const next = exportArgs[i + 1]
          if (next === 'false') { includeDatabases = false; i++ }
        }
      }

      if (help) {
        console.log(`
ncli export - Export Notion workspace to local Markdown files

Usage: ncli export [options]

Options:
  --output, -o <dir>       Output directory (default: ./notion-export)
  --page-id <id>           Export specific page and its children only
  --include-databases      Include database rows (default: true)
  --download-images        Download images to local _assets directory
  --resume                 Resume from last export state
  --fresh                  Clear state and start fresh export
  --help, -h               Show this help

Environment:
  NOTION_TOKEN             Notion integration token (required)

Examples:
  ncli export -o ./export
  ncli export --page-id abc123 -o ./export
  ncli export --download-images --resume -o ./export
`)
        process.exit(0)
      }

      const token = resolveToken()
      if (!token) {
        console.error('Error: No Notion token found.')
        console.error('  Set NOTION_TOKEN environment variable, or run: ncli auth login --token <token>')
        process.exit(1)
      }

      await runExport({
        token,
        output,
        pageId,
        includeDatabases,
        downloadImages,
        resume,
        fresh,
      })
      break
    }

    case 'auth': {
      const subCmd = args[1]

      if (!subCmd || subCmd === '--help' || subCmd === '-h') {
        console.log(`
ncli auth - Manage Notion authentication

Usage: ncli auth <subcommand> [options]

Subcommands:
  login [options]          Authenticate with Notion
  status                   Show current authentication status
  logout                   Remove saved authentication

Login Options:
  --token <token>          Use internal integration token directly
  --oauth                  Use OAuth 2.0 flow (opens browser)
  --client-id <id>         OAuth client ID
  --client-secret <secret> OAuth client secret

Examples:
  ncli auth login --token ntn_****                    # Internal integration
  ncli auth login --oauth --client-id xxx --client-secret yyy  # OAuth
  ncli auth status                                     # Show auth info
  ncli auth logout                                     # Clear saved auth
`)
        process.exit(0)
      }

      switch (subCmd) {
        case 'login': {
          const loginArgs = args.slice(2)
          let token: string | undefined
          let useOAuth = false
          let clientId: string | undefined
          let clientSecret: string | undefined

          for (let i = 0; i < loginArgs.length; i++) {
            const a = loginArgs[i]
            if (a === '--token') token = loginArgs[++i]
            else if (a === '--oauth') useOAuth = true
            else if (a === '--client-id') clientId = loginArgs[++i]
            else if (a === '--client-secret') clientSecret = loginArgs[++i]
          }

          if (token) {
            await loginWithToken(token)
          } else if (useOAuth) {
            await loginWithOAuth(clientId, clientSecret)
          } else {
            console.error('Specify either --token <token> or --oauth')
            console.error('Run "ncli auth --help" for usage.')
            process.exit(1)
          }
          break
        }

        case 'status':
          showAuthStatus()
          break

        case 'logout':
          logout()
          break

        default:
          console.error(`Unknown auth subcommand: ${subCmd}`)
          console.error('Run "ncli auth --help" for usage.')
          process.exit(1)
      }
      break
    }

    case 'list': {
      const listArgs = args.slice(1)
      let listHelp = false
      let filter: 'all' | 'page' | 'database' = 'all'
      let depth = 1
      let listPageId: string | null = null
      let refresh = false
      let jsonOutput = false
      let addRoot: string | null = null
      let removeRoot: string | null = null

      for (let i = 0; i < listArgs.length; i++) {
        const a = listArgs[i]
        if (a === '--help' || a === '-h') listHelp = true
        else if (a === '--pages') filter = 'page'
        else if (a === '--databases') filter = 'database'
        else if (a === '--depth' || a === '-d') depth = parseInt(listArgs[++i] ?? '1', 10)
        else if (a === '--all' || a === '-a') depth = 999
        else if (a === '--page-id') listPageId = listArgs[++i] ?? null
        else if (a === '--refresh') refresh = true
        else if (a === '--json') jsonOutput = true
        else if (a === '--add-root') addRoot = listArgs[++i] ?? null
        else if (a === '--remove-root') removeRoot = listArgs[++i] ?? null
      }

      if (listHelp) {
        console.log(`
ncli list - Browse your Notion workspace like a file system

Usage: ncli list [options]

Options:
  --page-id <id>           List children of a specific page (default: workspace root)
  --depth, -d <n>          How many levels deep to show (default: 1)
  --all, -a                Show all levels (full tree)
  --pages                  Show only pages
  --databases              Show only databases
  --refresh                Force rescan (bypass cache for root listing)
  --json                   Output as JSON
  --add-root <url-or-id>   Manually register a root page (instant, no scan)
  --remove-root <id>       Remove a page from root cache
  --help, -h               Show this help

Root listing is cached (~/.config/ncli/workspace-cache.json) for instant access.
First time: use --add-root to register root pages manually (instant), or
let it auto-scan (may take 1-2 minutes for large workspaces).

Examples:
  ncli list                                      # Show root-level items
  ncli list --add-root https://notion.so/Page-abc # Register root page (instant!)
  ncli list --add-root abc123-def456              # Register by ID
  ncli list --remove-root abc123                  # Remove from cache
  ncli list --refresh                             # Force rescan
  ncli list --json                                # Output as JSON
  ncli list --page-id <id>                        # Browse into a specific page
  ncli list --page-id <id> -d 3                   # 3 levels deep from that page

Output shows: [type] title  (id)
Use the id with:
  ncli list --page-id <id>            # Browse deeper
  ncli export --page-id <id> -o ./out # Export that page
`)
        process.exit(0)
      }

      const listToken = resolveToken()
      if (!listToken) {
        console.error('Error: No Notion token found.')
        console.error('  Run: ncli auth login --token <token>')
        process.exit(1)
      }

      const NOTION_API = 'https://api.notion.com'
      const apiHeaders = {
        'Authorization': `Bearer ${listToken}`,
        'Notion-Version': '2025-09-03',
        'Content-Type': 'application/json',
      }

      // Helper: fetch from Notion API (apiVersion override for db query compatibility)
      async function notionFetch(method: string, endpoint: string, body?: any, apiVersion?: string): Promise<any> {
        const headers = apiVersion
          ? { ...apiHeaders, 'Notion-Version': apiVersion }
          : apiHeaders
        const resp = await fetch(`${NOTION_API}${endpoint}`, {
          method,
          headers,
          body: body ? JSON.stringify(body) : undefined,
        })
        if (!resp.ok) {
          const text = await resp.text()
          throw new Error(`API error ${resp.status}: ${text}`)
        }
        return resp.json()
      }

      // Helper: get title from a page/database object
      function getTitle(item: any): string {
        if (item.properties) {
          for (const prop of Object.values(item.properties) as any[]) {
            if (prop.type === 'title' && prop.title?.length > 0) {
              return prop.title.map((t: any) => t.plain_text || '').join('') || 'Untitled'
            }
          }
        }
        if (item.title?.length > 0) {
          return item.title.map((t: any) => t.plain_text || '').join('') || 'Untitled'
        }
        return 'Untitled'
      }

      // Helper: print tree with depth limit
      function printTree(children: any[], indent: string, currentDepth: number, maxDepth: number): void {
        for (let i = 0; i < children.length; i++) {
          const item = children[i]
          const isLast = i === children.length - 1
          const prefix = indent + (isLast ? '└── ' : '├── ')
          const type = item.object === 'database' ? '📊 DB' : '📄'
          const title = getTitle(item)
          const id = item.id
          const hasChildren = item._children && item._children.length > 0
          const truncated = item._truncated
          const suffix = truncated ? '  ...' : ''
          console.log(`${prefix}${type} ${title}  (${id})${suffix}`)

          if (hasChildren && currentDepth < maxDepth) {
            const childIndent = indent + (isLast ? '    ' : '│   ')
            printTree(item._children, childIndent, currentDepth + 1, maxDepth)
          }
        }
      }

      // Shared: recursive function to fetch children up to depth
      async function fetchChildren(blockId: string, currentDepth: number, maxDepth: number, dbQueryId?: string): Promise<any[]> {
        if (currentDepth >= maxDepth) return []

        const results: any[] = []
        let cur: string | undefined
        let more = true

        while (more) {
          let endpoint = `/v1/blocks/${blockId}/children?page_size=100`
          if (cur) endpoint += `&start_cursor=${cur}`
          const data = await notionFetch('GET', endpoint)
          results.push(...(data.results ?? []))
          more = data.has_more === true
          cur = data.next_cursor ?? undefined
        }

        const items: any[] = []
        for (const block of results) {
          if (block.type === 'child_page') {
            const item: any = {
              id: block.id,
              object: 'page',
              properties: { title: { type: 'title', title: [{ plain_text: block.child_page?.title || 'Untitled' }] } },
              _children: [] as any[],
              _truncated: false,
            }
            if (currentDepth + 1 < maxDepth) {
              try {
                item._children = await fetchChildren(block.id, currentDepth + 1, maxDepth)
              } catch { /* skip */ }
            } else if (block.has_children) {
              item._truncated = true
            }
            items.push(item)
          } else if (block.type === 'child_database') {
            const item: any = {
              id: block.id,
              object: 'database',
              title: [{ plain_text: block.child_database?.title || 'Untitled' }],
              _children: [] as any[],
              _truncated: false,
            }
            items.push(item)
          }
        }

        // If this is a database root, also list its rows
        if (dbQueryId && blockId === dbQueryId) {
          try {
            const queryData = await notionFetch('POST', `/v1/databases/${blockId}/query`, { page_size: 100 }, '2022-06-28')
            for (const row of (queryData.results ?? [])) {
              items.push({
                ...row,
                _children: [] as any[],
                _truncated: false,
              })
            }
          } catch { /* not a database or no access */ }
        }

        return items
      }

      // --- Mode A: Browse specific page's children ---
      if (listPageId) {
        console.log(`Fetching children of ${listPageId}...\n`)

        // Get page info
        let pageInfo: any
        try {
          pageInfo = await notionFetch('GET', `/v1/pages/${listPageId}`)
        } catch {
          try {
            pageInfo = await notionFetch('GET', `/v1/databases/${listPageId}`)
          } catch (e: any) {
            console.error(`Could not find page or database: ${listPageId}`)
            process.exit(1)
          }
        }

        const pageTitle = getTitle(pageInfo)
        const pageType = pageInfo.object === 'database' ? '📊 DB' : '📄'
        const dbId = pageInfo.object === 'database' ? listPageId : undefined
        const children = await fetchChildren(listPageId, 0, depth, dbId)

        console.log(`${pageType} ${pageTitle}  (${listPageId})`)
        if (children.length === 0) {
          console.log('  (no child pages or databases)')
        } else {
          printTree(children, '', 1, depth)
        }

        console.log(`\nShowing depth: ${depth}. Use -d <n> for deeper, --all for full tree.`)
        console.log(`Tip: ncli list --page-id <id>  or  ncli export --page-id <id> -o ./out`)
        break
      }

      // --- Mode B: Workspace root (with caching for instant access) ---
      // The Notion API has no "list root pages" endpoint. The only way is to
      // scan all pages via search and filter parent.type === 'workspace'.
      // We cache root page IDs so subsequent calls are instant.

      const CACHE_DIR = path.join(os.homedir(), '.config', 'ncli')
      const CACHE_FILE = path.join(CACHE_DIR, 'workspace-cache.json')

      interface CachedRootItem {
        id: string
        object: string
        title: string
      }
      interface WorkspaceCache {
        rootItems: CachedRootItem[]
        timestamp: number
        scanned: number
      }

      function loadListCache(): WorkspaceCache | null {
        try {
          const data = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'))
          // Cache is permanent — only refreshed with --refresh
          if (!refresh) return data
        } catch {}
        return null
      }

      function saveListCache(cache: WorkspaceCache): void {
        fs.mkdirSync(CACHE_DIR, { recursive: true })
        fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2))
      }

      // Helper: extract page ID from Notion URL or raw ID
      function parseNotionId(input: string): string {
        // Handle Notion URLs like https://www.notion.so/Page-Title-abc123def456
        const urlMatch = input.match(/([a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})/)
        if (urlMatch) return urlMatch[1]
        // Might be a raw ID without dashes
        return input.replace(/[^a-f0-9-]/gi, '')
      }

      // --- Add/Remove root pages manually ---
      if (addRoot) {
        const pageId = parseNotionId(addRoot)
        if (!pageId) {
          console.error('Could not parse page ID from:', addRoot)
          process.exit(1)
        }

        // Fetch page info (single API call — instant)
        let pageInfo: any
        let objectType = 'page'
        try {
          pageInfo = await notionFetch('GET', `/v1/pages/${pageId}`)
        } catch {
          try {
            pageInfo = await notionFetch('GET', `/v1/databases/${pageId}`)
            objectType = 'database'
          } catch (e: any) {
            console.error(`Could not find page or database: ${pageId}`)
            console.error('Make sure your integration has access to this page.')
            process.exit(1)
          }
        }

        const title = getTitle(pageInfo)

        // Load existing cache or create new
        let existingCache: WorkspaceCache
        try {
          existingCache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'))
        } catch {
          existingCache = { rootItems: [], timestamp: Date.now(), scanned: 0 }
        }

        // Check for duplicates
        if (existingCache.rootItems.some(r => r.id === pageInfo.id)) {
          console.log(`Already registered: ${title} (${pageInfo.id})`)
        } else {
          existingCache.rootItems.push({
            id: pageInfo.id,
            object: objectType,
            title,
          })
          console.log(`Added root page: ${title} (${pageInfo.id})`)
        }

        // Always save (update timestamp)
        existingCache.timestamp = Date.now()
        saveListCache(existingCache)

        console.log(`\nCurrent root pages (${existingCache.rootItems.length}):`)
        for (const item of existingCache.rootItems) {
          const icon = item.object === 'database' ? '📊 DB' : '📄'
          console.log(`  ${icon} ${item.title}  (${item.id})`)
        }
        break
      }

      if (removeRoot) {
        const pageId = parseNotionId(removeRoot)
        let existingCache: WorkspaceCache
        try {
          existingCache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'))
        } catch {
          console.log('No cache file found. Nothing to remove.')
          break
        }

        const before = existingCache.rootItems.length
        existingCache.rootItems = existingCache.rootItems.filter(r => !r.id.includes(pageId))
        const after = existingCache.rootItems.length

        if (before === after) {
          console.log(`No root page found matching: ${pageId}`)
        } else {
          existingCache.timestamp = Date.now()
          saveListCache(existingCache)
          console.log(`Removed ${before - after} root page(s).`)
        }

        console.log(`\nCurrent root pages (${existingCache.rootItems.length}):`)
        for (const item of existingCache.rootItems) {
          const icon = item.object === 'database' ? '📊 DB' : '📄'
          console.log(`  ${icon} ${item.title}  (${item.id})`)
        }
        break
      }

      // Try loading from cache first
      let rootItems: any[] = []
      let fromCache = false
      let cacheAge = 0
      const cached = loadListCache()

      if (cached) {
        // Instant: use cached root items
        fromCache = true
        cacheAge = Math.round((Date.now() - cached.timestamp) / 60000)
        rootItems = cached.rootItems.map(ci => ({
          id: ci.id,
          object: ci.object,
          properties: ci.object === 'page'
            ? { title: { type: 'title', title: [{ plain_text: ci.title }] } }
            : undefined,
          title: ci.object === 'database'
            ? [{ plain_text: ci.title }]
            : undefined,
          _children: [] as any[],
          _truncated: true,
        }))
      } else {
        // No cache or stale/refresh — scan workspace
        // Default: scan up to 5000 items (covers most root pages).
        // After finding root items, stop after 10 consecutive empty batches.
        // Use --refresh for a more thorough scan (up to 20000 items).
        console.log('Scanning workspace for root items (bidirectional)...')
        const seenIds = new Set<string>()
        let scanned = 0
        let done = false
        const MAX_RETRIES = 3

        // Bidirectional scan: two streams from opposite ends, meeting in the middle
        async function scanDirection(direction: 'ascending' | 'descending', label: string): Promise<void> {
          let cursor: string | undefined
          let hasMore = true
          let retries = 0

          try {
            while (hasMore && !done) {
              const body: any = {
                page_size: 100,
                sort: { timestamp: 'last_edited_time', direction },
              }
              if (cursor) body.start_cursor = cursor
              if (filter !== 'all') body.filter = { value: filter === 'database' ? 'data_source' : 'page', property: 'object' }

              let data: any
              try {
                data = await notionFetch('POST', '/v1/search', body)
                retries = 0
              } catch (err: any) {
                const isRetryable = err.message.includes('start_cursor')
                  || err.message.includes('429')
                  || err.message.includes('504')
                  || err.message.includes('502')
                if (retries < MAX_RETRIES && isRetryable) {
                  retries++
                  const wait = retries * 3
                  process.stdout.write(`\n  [${label} retry ${retries}/${MAX_RETRIES}] Waiting ${wait}s...`)
                  await new Promise(r => setTimeout(r, wait * 1000))
                  if (err.message.includes('start_cursor')) cursor = undefined
                  continue
                }
                throw err
              }

              const results = data.results ?? []
              let overlapInBatch = 0

              for (const item of results) {
                if (seenIds.has(item.id)) {
                  overlapInBatch++
                  continue
                }
                seenIds.add(item.id)
                scanned++

                const p = item.parent
                if (p?.type === 'workspace' && !rootItems.some(r => r.id === item.id)) {
                  (item as any)._children = [];
                  (item as any)._truncated = true
                  rootItems.push(item)
                }
              }

              hasMore = data.has_more === true
              cursor = data.next_cursor ?? undefined

              // If >50% of batch was already seen by other stream, we've overlapped
              if (overlapInBatch > 50) {
                done = true
                break
              }

              process.stdout.write(`\rScanning... found ${rootItems.length} root items (${scanned} unique scanned)`)
            }
          } catch (err: any) {
            // Signal the other stream to stop and save partial results
            done = true
            process.stderr.write(`\n  [${label}] Stream error: ${err.message}\n`)
          }
        }

        // Run both streams in parallel, wait for both to finish
        await Promise.allSettled([
          scanDirection('ascending', '→'),
          scanDirection('descending', '←'),
        ])

        if (scanned > 100) {
          process.stdout.write('\r' + ' '.repeat(70) + '\r')
        }

        // Save to cache for instant access next time
        if (rootItems.length > 0) {
          const cacheData: WorkspaceCache = {
            rootItems: rootItems.map(item => ({
              id: item.id,
              object: item.object,
              title: getTitle(item),
            })),
            timestamp: Date.now(),
            scanned,
          }
          saveListCache(cacheData)
          console.log(`Found ${rootItems.length} root items (scanned ${scanned}). Cache saved for instant access.\n`)
        } else {
          console.log(`Scanned ${scanned} items, no root items found.\n`)
        }
      }

      // Apply filter on cached results
      if (filter !== 'all' && fromCache) {
        const filterObj = filter === 'page' ? 'page' : 'database'
        rootItems = rootItems.filter(item => item.object === filterObj)
      }

      if (rootItems.length === 0) {
        console.log('No root-level items found.')
        console.log('Make sure your integration has access to pages.')
        console.log('(Go to a Notion page → ··· → Connections → add your integration)')
      } else {
        // If depth > 1, fetch children for each root item
        if (depth > 1) {
          for (const item of rootItems) {
            try {
              item._children = await fetchChildren(item.id, 0, depth - 1)
              item._truncated = false
            } catch {
              // keep _truncated = true
            }
          }
        }

        if (jsonOutput) {
          const output = rootItems.map(item => ({
            id: item.id,
            type: item.object,
            title: getTitle(item),
          }))
          console.log(JSON.stringify(output, null, 2))
        } else {
          if (fromCache) {
            console.log(`${rootItems.length} root items (cached ${cacheAge}m ago, --refresh to update):\n`)
          } else {
            console.log(`${rootItems.length} root items:\n`)
          }
          printTree(rootItems, '', 1, depth)
        }
      }

      if (!jsonOutput) {
        console.log(`\nTip: ncli list --page-id <id>  or  ncli export --page-id <id> -o ./out`)
      }
      break
    }

    default:
      console.error(`Unknown command: ${command}`)
      printMainHelp()
      process.exit(1)
  }
}

main().catch(error => {
  if (error instanceof ValidationError) {
    console.error('Invalid OpenAPI 3.1 specification:')
    error.errors.forEach(err => console.error(err))
  } else {
    console.error('Error:', error)
  }
  process.exit(1)
})
