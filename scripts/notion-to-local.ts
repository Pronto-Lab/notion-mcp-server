#!/usr/bin/env tsx
/**
 * notion-to-local.ts
 * Export a Notion workspace to local directory + Markdown files.
 *
 * Usage:
 *   NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts -o ./export
 *   NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts --page-id abc123 -o ./export
 *   NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts --resume -o ./export
 *   NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts --fresh -o ./export
 *   NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts --download-images -o ./export
 */

import fs from 'fs'
import path from 'path'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface NotionPage {
  id: string
  object: 'page' | 'database'
  parent: any
  properties: any
  url: string
  created_time: string
  last_edited_time: string
}

interface NotionBlock {
  id: string
  type: string
  has_children: boolean
  [key: string]: any
}

interface ExportState {
  exportedPageIds: string[]
  lastRun: string
}

export interface ExportOptions {
  token: string
  downloadImages: boolean
  outputDir: string
  state: ExportState
  resume: boolean
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const NOTION_API_BASE = 'https://api.notion.com'
const NOTION_VERSION = '2025-09-03'
const MAX_RETRIES = 3

// ---------------------------------------------------------------------------
// Progress tracking
// ---------------------------------------------------------------------------

const progress = {
  totalPages: 0,
  exportedPages: 0,
  totalBlocks: 0,
  errors: 0,
  startTime: Date.now(),
}

function printProgress(): void {
  const elapsed = ((Date.now() - progress.startTime) / 1000).toFixed(1)
  process.stdout.write(
    `\r[${elapsed}s] Pages: ${progress.exportedPages}/${progress.totalPages} | Blocks: ${progress.totalBlocks} | Errors: ${progress.errors}   `
  )
}

// ---------------------------------------------------------------------------
// State persistence (resume mechanism)
// ---------------------------------------------------------------------------

function loadState(outputDir: string): ExportState {
  const statePath = path.join(outputDir, '.export-state.json')
  try {
    return JSON.parse(fs.readFileSync(statePath, 'utf-8'))
  } catch {
    return { exportedPageIds: [], lastRun: '' }
  }
}

function saveState(outputDir: string, state: ExportState): void {
  const statePath = path.join(outputDir, '.export-state.json')
  state.lastRun = new Date().toISOString()
  fs.writeFileSync(statePath, JSON.stringify(state, null, 2), 'utf-8')
}

function clearState(outputDir: string): void {
  const statePath = path.join(outputDir, '.export-state.json')
  try {
    fs.unlinkSync(statePath)
  } catch {
    // ignore if doesn't exist
  }
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

function parseArgs(argv: string[]): {
  output: string
  pageId: string | null
  includeDatabases: boolean
  resume: boolean
  fresh: boolean
  downloadImages: boolean
  help: boolean
} {
  const args = argv.slice(2)
  let output = './notion-export'
  let pageId: string | null = null
  let includeDatabases = true
  let resume = false
  let fresh = false
  let downloadImages = false
  let help = false

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg === '--help' || arg === '-h') {
      help = true
    } else if (arg === '--output' || arg === '-o') {
      output = args[++i] ?? output
    } else if (arg === '--page-id') {
      pageId = args[++i] ?? null
    } else if (arg === '--include-databases') {
      const next = args[i + 1]
      if (next === 'false') {
        includeDatabases = false
        i++
      }
    } else if (arg === '--resume') {
      resume = true
    } else if (arg === '--fresh') {
      fresh = true
    } else if (arg === '--download-images') {
      downloadImages = true
    }
  }

  return { output, pageId, includeDatabases, resume, fresh, downloadImages, help }
}

function printHelp(): void {
  console.log(`
notion-to-local.ts - Export Notion workspace to local Markdown files

Usage:
  tsx scripts/notion-to-local.ts [options]

Options:
  --output, -o <dir>       Output directory (default: ./notion-export)
  --page-id <id>           Export specific page and its children only
  --include-databases      Include database rows (default: true)
  --resume                 Resume from last export state
  --fresh                  Clear state and export from scratch
  --download-images        Download images to local _assets directory
  --help, -h               Show this help

Environment:
  NOTION_TOKEN             Notion integration token (required)

Examples:
  NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts -o ./export
  NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts --page-id abc123 -o ./export
  NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts --resume -o ./export
  NOTION_TOKEN=ntn_**** npx tsx scripts/notion-to-local.ts --download-images -o ./export
`)
}

// ---------------------------------------------------------------------------
// Notion API client
// ---------------------------------------------------------------------------

async function notionRequest(
  token: string,
  method: string,
  endpoint: string,
  body?: any,
  apiVersion?: string
): Promise<any> {
  const url = `${NOTION_API_BASE}${endpoint}`
  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
    'Notion-Version': apiVersion ?? NOTION_VERSION,
    'Content-Type': 'application/json',
  }

  let attempt = 0
  while (attempt < MAX_RETRIES) {
    const response = await fetch(url, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    })

    if (response.status === 429) {
      const retryAfter = parseInt(response.headers.get('Retry-After') ?? '5', 10)
      console.warn(`\n  [rate-limit] Retrying after ${retryAfter}s (attempt ${attempt + 1}/${MAX_RETRIES})`)
      await new Promise(r => setTimeout(r, retryAfter * 1000))
      attempt++
      continue
    }

    if (!response.ok) {
      const text = await response.text()
      throw new Error(`Notion API error ${response.status} on ${method} ${endpoint}: ${text}`)
    }

    return response.json()
  }

  throw new Error(`Notion API: exceeded max retries for ${method} ${endpoint}`)
}

// ---------------------------------------------------------------------------
// Workspace search / pagination
// ---------------------------------------------------------------------------

async function searchAll(token: string): Promise<NotionPage[]> {
  const results: NotionPage[] = []
  let startCursor: string | undefined = undefined
  let hasMore = true

  while (hasMore) {
    const body: any = { page_size: 100 }
    if (startCursor) body.start_cursor = startCursor

    const data = await notionRequest(token, 'POST', '/v1/search', body)
    results.push(...(data.results ?? []))
    hasMore = data.has_more === true
    startCursor = data.next_cursor ?? undefined
  }

  return results
}

async function getPageContent(token: string, pageId: string): Promise<NotionPage> {
  return notionRequest(token, 'GET', `/v1/pages/${pageId}`)
}

async function getBlockChildren(token: string, blockId: string): Promise<NotionBlock[]> {
  const results: NotionBlock[] = []
  let startCursor: string | undefined = undefined
  let hasMore = true

  while (hasMore) {
    let endpoint = `/v1/blocks/${blockId}/children?page_size=100`
    if (startCursor) endpoint += `&start_cursor=${startCursor}`

    const data = await notionRequest(token, 'GET', endpoint)
    results.push(...(data.results ?? []))
    hasMore = data.has_more === true
    startCursor = data.next_cursor ?? undefined
  }

  progress.totalBlocks += results.length
  return results
}

async function getDatabaseRows(token: string, databaseId: string): Promise<NotionPage[]> {
  const results: NotionPage[] = []
  let startCursor: string | undefined = undefined
  let hasMore = true

  while (hasMore) {
    const body: any = { page_size: 100 }
    if (startCursor) body.start_cursor = startCursor

    // Try data_sources endpoint first (new API), fallback to databases (legacy)
    // Note: databases/{id}/query requires 2022-06-28 version (deprecated in 2025-09-03)
    let data: any
    try {
      data = await notionRequest(token, 'POST', `/v1/data_sources/${databaseId}/query`, body)
    } catch {
      data = await notionRequest(token, 'POST', `/v1/databases/${databaseId}/query`, body, '2022-06-28')
    }
    results.push(...(data.results ?? []))
    hasMore = data.has_more === true
    startCursor = data.next_cursor ?? undefined
  }

  return results
}

// ---------------------------------------------------------------------------
// Image download
// ---------------------------------------------------------------------------

async function downloadFile(url: string, destPath: string): Promise<void> {
  const response = await fetch(url)
  if (!response.ok) throw new Error(`Failed to download: ${response.status}`)
  const buffer = Buffer.from(await response.arrayBuffer())
  ensureDir(path.dirname(destPath))
  fs.writeFileSync(destPath, buffer)
}

function extractExtFromUrl(url: string): string {
  try {
    const pathname = new URL(url).pathname
    const ext = path.extname(pathname)
    return ext || '.png'
  } catch {
    return '.png'
  }
}

// ---------------------------------------------------------------------------
// Rich text → Markdown
// ---------------------------------------------------------------------------

function richTextToMarkdown(richTexts: any[]): string {
  if (!richTexts || richTexts.length === 0) return ''
  return richTexts
    .map(rt => {
      let text = rt.plain_text || rt.text?.content || ''
      if (!text) return ''
      const ann = rt.annotations || {}
      if (ann.code) text = '`' + text + '`'
      if (ann.bold) text = '**' + text + '**'
      if (ann.italic) text = '*' + text + '*'
      if (ann.strikethrough) text = '~~' + text + '~~'
      if (rt.href || rt.text?.link?.url) {
        text = `[${text}](${rt.href || rt.text.link.url})`
      }
      return text
    })
    .join('')
}

// ---------------------------------------------------------------------------
// Block → Markdown
// ---------------------------------------------------------------------------

function blockToMarkdown(
  block: NotionBlock,
  indent: number = 0,
  opts?: { downloadImages: boolean; outputDir: string }
): string {
  const prefix = '  '.repeat(indent)
  switch (block.type) {
    case 'paragraph':
      return prefix + richTextToMarkdown(block.paragraph?.rich_text ?? []) + '\n'
    case 'heading_1':
      return '# ' + richTextToMarkdown(block.heading_1?.rich_text ?? []) + '\n'
    case 'heading_2':
      return '## ' + richTextToMarkdown(block.heading_2?.rich_text ?? []) + '\n'
    case 'heading_3':
      return '### ' + richTextToMarkdown(block.heading_3?.rich_text ?? []) + '\n'
    case 'bulleted_list_item':
      return prefix + '- ' + richTextToMarkdown(block.bulleted_list_item?.rich_text ?? []) + '\n'
    case 'numbered_list_item':
      return prefix + '1. ' + richTextToMarkdown(block.numbered_list_item?.rich_text ?? []) + '\n'
    case 'to_do': {
      const checked = block.to_do?.checked ? 'x' : ' '
      return prefix + `- [${checked}] ` + richTextToMarkdown(block.to_do?.rich_text ?? []) + '\n'
    }
    case 'toggle':
      return (
        prefix +
        '<details><summary>' +
        richTextToMarkdown(block.toggle?.rich_text ?? []) +
        '</summary>\n\n'
      )
    case 'code': {
      const lang = block.code?.language || ''
      return (
        prefix +
        '```' +
        lang +
        '\n' +
        richTextToMarkdown(block.code?.rich_text ?? []) +
        '\n' +
        prefix +
        '```\n'
      )
    }
    case 'quote':
      return prefix + '> ' + richTextToMarkdown(block.quote?.rich_text ?? []) + '\n'
    case 'callout': {
      const icon = block.callout?.icon?.emoji || '💡'
      return prefix + `> ${icon} ` + richTextToMarkdown(block.callout?.rich_text ?? []) + '\n'
    }
    case 'divider':
      return '---\n'
    case 'image': {
      const imgUrl = block.image?.file?.url || block.image?.external?.url || ''
      const caption = block.image?.caption ? richTextToMarkdown(block.image.caption) : ''
      if (opts?.downloadImages && imgUrl) {
        const ext = extractExtFromUrl(imgUrl)
        const assetFilename = `image-${block.id}${ext}`
        block._pendingDownload = {
          url: imgUrl,
          dest: path.join(opts.outputDir, '_assets', assetFilename),
        }
        return `![${caption}](./_assets/${assetFilename})\n`
      }
      return `![${caption}](${imgUrl})\n`
    }
    case 'bookmark': {
      const bmUrl = block.bookmark?.url || ''
      const bmCaption = block.bookmark?.caption
        ? richTextToMarkdown(block.bookmark.caption)
        : bmUrl
      return `[${bmCaption}](${bmUrl})\n`
    }
    case 'equation':
      return `$$\n${block.equation?.expression ?? ''}\n$$\n`
    case 'table':
      return '' // rows handled via children
    case 'table_row': {
      const cells = (block.table_row?.cells ?? []).map((cell: any[]) =>
        richTextToMarkdown(cell)
      )
      return '| ' + cells.join(' | ') + ' |\n'
    }
    case 'child_page':
      return `📄 [${block.child_page?.title}](./${sanitizeFilename(block.child_page?.title ?? 'page')}/index.md)\n`
    case 'child_database':
      return `📊 [${block.child_database?.title}](./${sanitizeFilename(block.child_database?.title ?? 'database')}/)\n`
    case 'embed':
      return `[Embed](${block.embed?.url ?? ''})\n`
    case 'video': {
      const vidUrl = block.video?.file?.url || block.video?.external?.url || ''
      return `[Video](${vidUrl})\n`
    }
    case 'pdf': {
      const pdfUrl = block.pdf?.file?.url || block.pdf?.external?.url || ''
      return `[PDF](${pdfUrl})\n`
    }
    case 'audio': {
      const audioUrl = block.audio?.file?.url || block.audio?.external?.url || ''
      return `[Audio](${audioUrl})\n`
    }
    case 'link_preview':
      return `[${block.link_preview?.url ?? ''}](${block.link_preview?.url ?? ''})\n`
    case 'link_to_page':
      return `[Linked Page](notion://${block.link_to_page?.page_id || block.link_to_page?.database_id})\n`
    default:
      return `<!-- Unsupported block type: ${block.type} -->\n`
  }
}

// ---------------------------------------------------------------------------
// Blocks → Markdown (recursive, with table separator support)
// ---------------------------------------------------------------------------

async function blocksToMarkdown(
  token: string,
  blocks: NotionBlock[],
  indent: number = 0,
  opts?: { downloadImages: boolean; outputDir: string }
): Promise<string> {
  let md = ''
  let i = 0

  while (i < blocks.length) {
    const block = blocks[i]
    const line = blockToMarkdown(block, indent, opts)
    md += line

    // Handle image downloads queued by blockToMarkdown
    if (opts?.downloadImages && block._pendingDownload) {
      const { url, dest } = block._pendingDownload as { url: string; dest: string }
      try {
        await downloadFile(url, dest)
      } catch (err: any) {
        console.warn(`\n  [warn] Failed to download image for block ${block.id}: ${err.message}`)
        progress.errors++
      }
      delete block._pendingDownload
    }

    // Table: first row = header, add separator after it
    if (block.type === 'table' && block.has_children) {
      try {
        const rowBlocks = await getBlockChildren(token, block.id)
        if (rowBlocks.length > 0) {
          // header row
          md += blockToMarkdown(rowBlocks[0], indent, opts)
          // separator
          const colCount = (rowBlocks[0].table_row?.cells ?? []).length
          md += '| ' + Array(colCount).fill('---').join(' | ') + ' |\n'
          // data rows
          for (let r = 1; r < rowBlocks.length; r++) {
            md += blockToMarkdown(rowBlocks[r], indent, opts)
          }
        }
      } catch (err: any) {
        console.warn(`\n  [warn] Failed to read table rows for block ${block.id}: ${err.message}`)
        progress.errors++
      }
      i++
      continue
    }

    // Toggle: add children inside details tag
    if (block.type === 'toggle' && block.has_children) {
      try {
        const childBlocks = await getBlockChildren(token, block.id)
        md += await blocksToMarkdown(token, childBlocks, indent + 1, opts)
      } catch (err: any) {
        console.warn(`\n  [warn] Failed to read toggle children for block ${block.id}: ${err.message}`)
        progress.errors++
      }
      md += prefix(indent) + '</details>\n\n'
      i++
      continue
    }

    // Generic has_children (indented recursion)
    if (block.has_children && !['table', 'toggle'].includes(block.type)) {
      try {
        const childBlocks = await getBlockChildren(token, block.id)
        md += await blocksToMarkdown(token, childBlocks, indent + 1, opts)
      } catch (err: any) {
        console.warn(`\n  [warn] Failed to read children for block ${block.id}: ${err.message}`)
        progress.errors++
      }
    }

    i++
  }

  return md
}

function prefix(indent: number): string {
  return '  '.repeat(indent)
}

// ---------------------------------------------------------------------------
// Property → frontmatter value
// ---------------------------------------------------------------------------

function propertyToFrontmatter(prop: any): string | number | boolean | string[] | null {
  if (!prop) return null
  switch (prop.type) {
    case 'title':
      return richTextToMarkdown(prop.title ?? [])
    case 'rich_text':
      return richTextToMarkdown(prop.rich_text ?? [])
    case 'number':
      return prop.number ?? null
    case 'select':
      return prop.select?.name ?? null
    case 'multi_select':
      return (prop.multi_select ?? []).map((s: any) => s.name)
    case 'date':
      return prop.date?.start ?? null
    case 'checkbox':
      return prop.checkbox ?? false
    case 'url':
      return prop.url ?? null
    case 'status':
      return prop.status?.name ?? null
    case 'email':
      return prop.email ?? null
    case 'phone_number':
      return prop.phone_number ?? null
    case 'created_time':
      return prop.created_time ?? null
    case 'last_edited_time':
      return prop.last_edited_time ?? null
    default:
      return null
  }
}

function buildFrontmatter(properties: any): string {
  const lines: string[] = ['---']
  for (const [key, value] of Object.entries(properties)) {
    const v = propertyToFrontmatter(value as any)
    if (v === null || v === undefined) continue
    if (Array.isArray(v)) {
      if (v.length === 0) continue
      lines.push(`${key}:`)
      for (const item of v) {
        lines.push(`  - ${JSON.stringify(item)}`)
      }
    } else if (typeof v === 'string') {
      // multi-line strings
      if (v.includes('\n')) {
        lines.push(`${key}: |`)
        for (const l of v.split('\n')) {
          lines.push(`  ${l}`)
        }
      } else {
        lines.push(`${key}: ${JSON.stringify(v)}`)
      }
    } else {
      lines.push(`${key}: ${v}`)
    }
  }
  lines.push('---')
  return lines.join('\n') + '\n\n'
}

// ---------------------------------------------------------------------------
// File system helpers
// ---------------------------------------------------------------------------

function sanitizeFilename(name: string): string {
  return name
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
    .substring(0, 200) || 'untitled'
}

function ensureDir(dirPath: string): void {
  fs.mkdirSync(dirPath, { recursive: true })
}

function writeFile(filePath: string, content: string): void {
  ensureDir(path.dirname(filePath))
  fs.writeFileSync(filePath, content, 'utf-8')
}

// ---------------------------------------------------------------------------
// Page title extraction
// ---------------------------------------------------------------------------

function getPageTitle(page: NotionPage): string {
  if (!page.properties) return 'Untitled'
  for (const prop of Object.values(page.properties) as any[]) {
    if (prop.type === 'title' && prop.title?.length > 0) {
      return richTextToMarkdown(prop.title) || 'Untitled'
    }
  }
  return 'Untitled'
}

// ---------------------------------------------------------------------------
// Export a single page (page content → Markdown file)
// ---------------------------------------------------------------------------

async function exportPage(
  page: NotionPage,
  outputPath: string,
  exportOpts: ExportOptions,
  isDbRow: boolean = false
): Promise<void> {
  const title = getPageTitle(page)

  // Resume: skip already-exported pages
  if (exportOpts.resume && exportOpts.state.exportedPageIds.includes(page.id)) {
    progress.exportedPages++
    printProgress()
    return
  }

  let content = ''

  // Frontmatter for DB rows
  if (isDbRow && page.properties) {
    content += buildFrontmatter(page.properties)
  } else {
    content += `# ${title}\n\n`
  }

  const pageOutputDir = path.dirname(outputPath)
  const blockOpts = { downloadImages: exportOpts.downloadImages, outputDir: pageOutputDir }

  // Fetch blocks
  try {
    const blocks = await getBlockChildren(exportOpts.token, page.id)
    content += await blocksToMarkdown(exportOpts.token, blocks, 0, blockOpts)
  } catch (err: any) {
    console.warn(`\n  [warn] Could not fetch blocks for page ${page.id}: ${err.message}`)
    content += `<!-- Error fetching content: ${err.message} -->\n`
    progress.errors++
  }

  writeFile(outputPath, content)

  progress.exportedPages++
  printProgress()

  exportOpts.state.exportedPageIds.push(page.id)
  if (exportOpts.state.exportedPageIds.length % 20 === 0) {
    saveState(exportOpts.outputDir, exportOpts.state)
  }
}

// ---------------------------------------------------------------------------
// Export a database (metadata + rows)
// ---------------------------------------------------------------------------

async function exportDatabase(
  db: NotionPage,
  outputDir: string,
  exportOpts: ExportOptions,
  includeDatabases: boolean
): Promise<void> {
  const title = getPageTitle(db)

  ensureDir(outputDir)

  // Write index file
  let indexContent = `# ${title}\n\n_Database_\n\n`
  const blockOpts = { downloadImages: exportOpts.downloadImages, outputDir }
  try {
    const blocks = await getBlockChildren(exportOpts.token, db.id)
    indexContent += await blocksToMarkdown(exportOpts.token, blocks, 0, blockOpts)
  } catch (_) {
    // databases may have no block content
  }
  writeFile(path.join(outputDir, 'index.md'), indexContent)

  if (!includeDatabases) return

  // Export rows
  try {
    const rows = await getDatabaseRows(exportOpts.token, db.id)
    for (const row of rows) {
      const rowTitle = getPageTitle(row)
      const rowFilename = sanitizeFilename(rowTitle) + '.md'
      await exportPage(row, path.join(outputDir, rowFilename), exportOpts, true)
    }
  } catch (err: any) {
    console.warn(`\n  [warn] Could not fetch database rows for ${db.id}: ${err.message}`)
    progress.errors++
  }
}

// ---------------------------------------------------------------------------
// Build parent → children map
// ---------------------------------------------------------------------------

function buildTree(pages: NotionPage[]): Map<string, NotionPage[]> {
  const tree = new Map<string, NotionPage[]>()

  for (const page of pages) {
    const parent = page.parent
    let parentId: string

    if (parent?.type === 'page_id') {
      parentId = parent.page_id
    } else if (parent?.type === 'database_id') {
      parentId = parent.database_id
    } else if (parent?.type === 'workspace') {
      parentId = 'workspace'
    } else if (parent?.type === 'block_id') {
      parentId = parent.block_id
    } else {
      parentId = 'workspace'
    }

    if (!tree.has(parentId)) tree.set(parentId, [])
    tree.get(parentId)!.push(page)
  }

  return tree
}

// ---------------------------------------------------------------------------
// Build tree from a single page (recursive block children discovery)
// ---------------------------------------------------------------------------

/**
 * Build a parent→children tree by recursively discovering child_page and
 * child_database blocks via the block children API.
 * Only fetches pages reachable from the given root — no full workspace scan.
 */
async function buildTreeFromPage(
  token: string,
  rootPageId: string
): Promise<Map<string, NotionPage[]>> {
  const tree = new Map<string, NotionPage[]>()

  async function discover(parentId: string): Promise<void> {
    const children = await getBlockChildren(token, parentId)
    for (const block of children) {
      if (block.type === 'child_page') {
        try {
          const page = await getPageContent(token, block.id)
          if (!tree.has(parentId)) tree.set(parentId, [])
          tree.get(parentId)!.push(page)
          progress.totalPages++
          printProgress()
          await discover(block.id)
        } catch (err: any) {
          console.warn(`\n  [warn] Could not fetch child page ${block.id}: ${err.message}`)
          progress.errors++
        }
      } else if (block.type === 'child_database') {
        try {
          const db = await notionRequest(token, 'GET', `/v1/databases/${block.id}`)
          if (!tree.has(parentId)) tree.set(parentId, [])
          tree.get(parentId)!.push(db)
          progress.totalPages++
        } catch (err: any) {
          console.warn(`\n  [warn] Could not fetch child database ${block.id}: ${err.message}`)
          progress.errors++
        }
      }
    }
  }

  await discover(rootPageId)
  return tree
}

// ---------------------------------------------------------------------------
// Recursive export
// ---------------------------------------------------------------------------

async function exportNode(
  page: NotionPage,
  outputDir: string,
  tree: Map<string, NotionPage[]>,
  exportOpts: ExportOptions,
  includeDatabases: boolean,
  depth: number = 0
): Promise<void> {
  const title = getPageTitle(page)
  const safeName = sanitizeFilename(title)

  if (page.object === 'database') {
    const dbDir = path.join(outputDir, safeName)
    await exportDatabase(page, dbDir, exportOpts, includeDatabases)

    // Child pages of this DB are already exported as rows; skip sub-tree to avoid duplicates
    // (DB rows are fetched via /databases/{id}/query, not via tree)
  } else {
    // Regular page
    const children = tree.get(page.id) ?? []

    if (children.length > 0) {
      // Page has sub-pages → directory with index.md
      const pageDir = path.join(outputDir, safeName)
      ensureDir(pageDir)
      await exportPage(page, path.join(pageDir, 'index.md'), exportOpts)

      for (const child of children) {
        try {
          await exportNode(child, pageDir, tree, exportOpts, includeDatabases, depth + 1)
        } catch (err: any) {
          console.warn(`\n  [warn] Skipping child ${child.id} of ${page.id}: ${err.message}`)
          progress.errors++
        }
      }
    } else {
      // Leaf page → single .md file
      const filePath = path.join(outputDir, safeName + '.md')
      await exportPage(page, filePath, exportOpts)
    }
  }
}

// ---------------------------------------------------------------------------
// Core export logic (callable from external code)
// ---------------------------------------------------------------------------

export interface RunExportOptions {
  token: string
  output: string
  pageId: string | null
  includeDatabases: boolean
  downloadImages: boolean
  resume: boolean
  fresh: boolean
}

export async function runExport(opts: RunExportOptions): Promise<void> {
  const { token, pageId, includeDatabases, resume, fresh, downloadImages } = opts
  const outputDir = path.resolve(opts.output)

  ensureDir(outputDir)

  if (fresh) {
    clearState(outputDir)
  }

  const state = resume ? loadState(outputDir) : { exportedPageIds: [], lastRun: '' }

  const exportOpts: ExportOptions = {
    token,
    downloadImages,
    outputDir,
    state,
    resume,
  }

  // Reset progress counters for this run
  progress.totalPages = 0
  progress.exportedPages = 0
  progress.totalBlocks = 0
  progress.errors = 0
  progress.startTime = Date.now()

  // Single-page mode
  if (pageId) {
    console.log(`Fetching specific page: ${pageId}`)
    const page = await getPageContent(token, pageId)
    progress.totalPages = 1
    console.log('Discovering child pages...')
    const tree = await buildTreeFromPage(token, pageId)
    await exportNode(page as any, outputDir, tree, exportOpts, includeDatabases)

    saveState(outputDir, state)
    console.log('\n')
    console.log(
      `Export complete. Pages: ${progress.exportedPages}/${progress.totalPages} | Blocks: ${progress.totalBlocks} | Errors: ${progress.errors}`
    )
    return
  }

  // Full workspace mode
  console.log('Searching all pages and databases...')
  const allPages = await searchAll(token)

  progress.totalPages = allPages.length
  console.log(`Found ${allPages.length} items`)
  if (resume && state.exportedPageIds.length > 0) {
    console.log(`Resuming: ${state.exportedPageIds.length} pages already exported`)
  }

  const tree = buildTree(allPages)
  const rootItems = tree.get('workspace') ?? []
  console.log(`Root-level items: ${rootItems.length}`)

  for (const item of rootItems) {
    try {
      await exportNode(item, outputDir, tree, exportOpts, includeDatabases)
    } catch (err: any) {
      console.warn(`\n[warn] Skipping root item ${item.id}: ${err.message}`)
      progress.errors++
    }
  }

  saveState(outputDir, state)
  console.log('\n')
  console.log(
    `Export complete. Pages: ${progress.exportedPages}/${progress.totalPages} | Blocks: ${progress.totalBlocks} | Errors: ${progress.errors}`
  )
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const opts = parseArgs(process.argv)

  if (opts.help) {
    printHelp()
    process.exit(0)
  }

  // Try to resolve token from config or env
  let token: string | undefined
  try {
    const { resolveToken } = await import('./auth.js')
    token = resolveToken() ?? undefined
  } catch {
    token = process.env.NOTION_TOKEN
  }
  if (!token) {
    console.error('Error: No Notion token found.')
    console.error('  Set NOTION_TOKEN environment variable, or run: ncli auth login --token <token>')
    process.exit(1)
  }

  console.log(`Starting Notion export to: ${path.resolve(opts.output)}`)

  await runExport({
    token,
    output: opts.output,
    pageId: opts.pageId,
    includeDatabases: opts.includeDatabases,
    resume: opts.resume,
    fresh: opts.fresh,
    downloadImages: opts.downloadImages,
  })
}

// Only auto-run when executed directly (not imported)
const isDirectRun =
  process.argv[1]?.endsWith('notion-to-local.ts') ||
  process.argv[1]?.endsWith('notion-to-local.js') ||
  process.argv[1]?.endsWith('notion-to-local.mjs')

if (isDirectRun) {
  main().catch(err => {
    console.error('Fatal error:', err)
    process.exit(1)
  })
}
